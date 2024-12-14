import Firebase from 'firebase-admin';
import type { WriteResult/*, DocumentReference, DocumentData */ } from 'firebase-admin/firestore';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import deepMerge from 'deepmerge';
import { nanoid } from 'nanoid';
import LevelUp from 'levelup';
import MemDown from 'memdown';

import LevelSQLite from './sqlite.js';
import AbstractFireStash, { IFireStash, IFireStashPage, FireStashOptions, ServiceAccount } from './types.js';

export type { IFireStash, IFireStashPage, FireStashOptions, ServiceAccount };

const GET_PAGINATION_SIZE = 100;

declare global {
  // https://github.com/DefinitelyTyped/DefinitelyTyped/issues/40366
  interface PromiseConstructor {
    /* eslint-disable-next-line */
    allSettled<T>(promises: Array<Promise<T>>): Promise<Array<{status: 'fulfilled'; value: T;} | {status: 'rejected'; reason: Error}>>;
  }

  /* eslint-disable-next-line @typescript-eslint/no-namespace */
  namespace NodeJS {
    interface ProcessEnv {
      FIRESTASH_PAGINATION: number | undefined;
    }
  }
}

// Maximum payload size for Firestore is 11,534,336 bytes (11.5MB).
// Maximum document size is 1MB. Worst case: 10 documents are safe to send at a time.
const MAX_BATCH_SIZE = 10;
const MAX_UPDATE_SIZE = 490;
// const MAX_READ_SIZE = 500;
const DELETE_RECORD = Symbol('firestash-delete');

interface ThrottledSnapshot {
  updatedAt: number;
  timeout: number;
  callbacks: Set<(snapshot: any) => void>;
  releaseSnapshot: (() => void) | null;
  intervalId: NodeJS.Timeout | null;
  data: FirebaseFirestore.DocumentSnapshot | null;
  count: number;
}

function stringify(json: any): Buffer { return Buffer.from('0' + JSON.stringify(json)); }
function reify<T = object>(data: Buffer | null) { return data ? JSON.parse(data.toString('utf8', data[0] === 123 ? 0 : 1)) as T : null; } // Character code for "{";
function isDirty(data: Buffer): boolean { return data[0] !== 49; } // Character code for "0". Is dirty unless first character is "0".
// function isClean(data: Buffer): boolean { return data[0] === 49; } // Character code for "0";
function setDirty(data: Buffer): void { data[0] = 48; } // Character code for "1";
function setClean(data: Buffer): void { data[0] = 49; } // Character code for "0";

/* eslint-disable-next-line */
const overwriteMerge = (_destinationArray: any[], sourceArray: any[]) => sourceArray;
const numId = (id: string) => [...crypto.createHash('md5').update(id).digest().values()].reduce((a, b) => a + b);

// We base64 encode page keys to safely represent deep collections, who's paths contain '/', in a flat list.
function encode(key: string) {
  return Buffer.from(key).toString('base64').replace(/=/g, '').replace(/\//g, '.');
}

export function cacheKey(collection: string, page: number) { return encode(`${collection}-${page}`); }

const PAGINATION = 15000;
// const BATCH_SIZE = 10;
function pageSize(): number {
  return process.env.FIRESTASH_PAGINATION || PAGINATION;
}

/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
function ensureFirebaseSafe(obj: any, FieldValue: typeof Firebase.firestore.FieldValue, depth = 0): void {
  if (depth > 10) { return; }
  if (Array.isArray(obj)) { return; }
  if (typeof obj === 'object') {
    for (const key of Object.keys(obj)) {
      if (obj[key] === undefined) {
        obj[key] = FieldValue.delete();
      }
      else if (Object.prototype.toString.call(obj[key]) === '[object Object]') {
        ensureFirebaseSafe(obj[key], FieldValue, depth++);
      }
    }
  }
}

export default class FireStash extends AbstractFireStash {
  toUpdate: Map<string, Map<string, (object | typeof DELETE_RECORD | null)[]>> = new Map();
  firebase: typeof Firebase;
  app: Firebase.app.App;
  db: Firebase.firestore.Firestore;
  dir: string | null;
  log: typeof console;
  #watchers: Map<string, () => void> = new Map();
  #pendingUpdates: Set<Promise<void>> = new Set();
  timeout: NodeJS.Timeout | number | null = null;
  timeoutPromise: Promise<void> | null = null;
  level: LevelUp.LevelUp | LevelSQLite;
  documentListeners: Map<string, ThrottledSnapshot> = new Map();

  /**
   * Create a new FireStash. Binds to the app database provided. Writes stash backups to disk if directory is provided.
   * @param firebase The Firebase Library.
   * @param app The Firebase App Instance.
   * @param directory The cache backup directory (optional)
   */
  constructor(project: ServiceAccount | string | null, options?: Partial<FireStashOptions> | undefined) {
    super(project, options);
    const creds = typeof project === 'string' ? { projectId: project } : { projectId: project?.projectId, credential: project ? Firebase.credential.cert(project) : undefined };
    this.app = Firebase.initializeApp(creds, `${creds.projectId}-firestash-${nanoid()}`);
    this.firebase = Firebase;
    this.db = this.app.firestore();
    this.dir = this.options?.directory || null;
    this.log = console;

    // Clean up after ourselves if the process exits.
    this.stop = this.stop.bind(this);
    process.on('exit', () => this.stop);
    process.on('SIGHUP', () => this.stop);
    process.on('SIGINT', () => this.stop);
    process.on('SIGTERM', () => this.stop);

    // Save ourselves from annoying throws. This cases should be handled in-library anyway.
    this.db.settings({ ignoreUndefinedProperties: true });
    if (!this.options.lowMem && this.options.datastore === 'sqlite' && this.dir) {
      fs.mkdirSync(this.dir, { recursive: true });
      this.level = new LevelSQLite(path.join(this.dir, '.firestash.db'));
    }
    else {
      this.level = LevelUp((MemDown as any)());
      (this.level as unknown as LevelSQLite).markDirty = async(key: string) => {
        let obj = Buffer.from('1{}');
        try {
          obj = await this.level.get(key, { asBuffer: true })
        } catch { 1; }
        setDirty(obj);
        await this.level.put(key, obj);
      }
    }

    // Set Immediate is better to use than the default nextTick for newer versions of Node.js.
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    this.level._nextTick = setImmediate;
  }

  cacheKey(collection: string, page: number) { return cacheKey(collection, page); }

  async watchers() { return [...new Set(this.#watchers.keys())]; }

  /**
   * Resolves when all previously called updates are written to remote. Like requestAnimationFrame for the collection cache.
   */
  async allSettled() {
    await Promise.allSettled([...this.#pendingUpdates, this.timeoutPromise]);
  }

  private stashPagesMemo: Record<string, Record<string, IFireStashPage | undefined>> = {};
  private async stashPages(collection: string): Promise<Record<string, IFireStashPage | undefined>> {
    if (this.stashPagesMemo[collection]) { return this.stashPagesMemo[collection]; }
    try {
      return this.stashPagesMemo[collection] = (reify(await this.level.get(collection, { asBuffer: true }) || null) as Record<string, IFireStashPage>) || {};
    }
    catch (_err) {
      return this.stashPagesMemo[collection] = {};
    }
  }

  private async hasKey(collection: string, key: string): Promise<boolean> {
    await this.watch(collection);
    for (const stash of Object.values(await this.stashPages(collection))) {
      if (Object.hasOwnProperty.call(stash?.cache || {}, key)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the entire stash cache for a collection.
   * @param collection Collection Path
   */
  private stashMemo: Record<string, IFireStashPage> = {};
  async stash(collection: string): Promise<IFireStashPage> {
    if (this.stashMemo[collection]) { return this.stashMemo[collection]; }

    const pages = await this.stashPages(collection);
    const out: IFireStashPage = { collection, cache: {} };
    for (const dat of Object.values(pages)) {
      if (!dat) { continue; }
      for (const [key, value] of Object.entries(dat.cache)) {
        // If the incrementing count is fragmented across multiple pages, we
        // need to account for all values added. Sum them to get the real count.
        out.cache[key] = (out.cache[key] || 0) + value;
      }
    }

    return (this.stashMemo[collection] = out);
  }

  /**
   * Called once per second when there are updated queued. Writes all changes to remote stash cache.
   */
  private showWarning = true;
  private async saveCacheUpdate() {
    if (!this.level.open) { return; }
    // Wait for our local events queue to finish before syncing remote.
    const pending = this.timeoutPromise;
    pending && this.#pendingUpdates.add(pending);
    if (this.#pendingUpdates.size > 10 && this.showWarning) {
      this.showWarning = false
      this.log.warn('[FireStash] Warning: cache save has exceeded 10 writes per second.');
      this.log.warn('            This is a sign of a hot code path that can be optimized.');
      this.log.warn('            Key Sample:\n', [...this.toUpdate.keys()].slice(0, 10).map(k => `             - ${k}`).join('\n'));
    }
    if (this.#pendingUpdates.size < 5 && !this.showWarning) {
      this.showWarning = true;
    }
    try {
      await this.drainEventsPromise;

      this.timeout = null;
      this.timeoutPromise = null;

      /* eslint-disable-next-line */
      const promises: Promise<any>[] = [];
      const FieldValue = this.firebase.firestore.FieldValue;
      const updates: Record<string, IFireStashPage<FirebaseFirestore.FieldValue>> = {};
      const entries = [...this.toUpdate.entries()];
      const collectionStashes: Map<string, Record<string, IFireStashPage | undefined>> = new Map();

      let docCount = 0;
      let updateCount = 0;
      let docUpdates: ['set' | 'delete', string, any][] = [];

      // Clear queue for further updates while we work.
      this.toUpdate.clear();

      // Ensure we are watching for remote updates. Intentional fire and forget here.
      // We do this at the end of an update queue to not throttle our watchers on many new collection additions.
      const collectionStashPromises = entries.map(async([collection]) => {
        // If we encounter a collection, it's always better on memory to set up a watcher instead of reading each time.
        if (!this.#watchers.has(collection)) { await this.watch(collection); }

        // Because we have a watcher, we know we're up to date with latest as all times! Use existing stash pages.
        collectionStashes.set(collection, await this.stashPages(collection));
      });

      // Wait to ensure we have all the latest stash caches.
      // We need this to ensure all we know what page to increment the generation number on.
      await Promise.allSettled(collectionStashPromises);
      const localBatch = this.level.batch();
      const events: Map<string, Set<string>> = new Map();
      const commits: Promise<void>[] = [];
      try {
        for (const [ collection, keys ] of entries) {
          // Get / ensure we have the remote cache object present.
          const localStash = collectionStashes.get(collection) || {};
          collectionStashes.set(collection, localStash);

          // Get the stash's page and document count.
          let pageCount = Object.keys(localStash).length;

          // For each key queued, set the cache object.
          for (const [ key, objects ] of keys.entries()) {

            // Calculate the page we're adding to. In re-balance this is a md5 hash as decimal, mod page size, but on insert we just append to the last page present.
            let pageIdx = Math.max(pageCount - 1, 0);

            // Check to make sure this key doesn't already exist on remote. If it does, use its existing page.
            // TODO: Perf? This is potentially a lot of looping.
            const hasPageNum = numId(key) % pageCount;
            if (localStash[cacheKey(collection, hasPageNum)]?.cache?.[key]) {
              pageIdx = hasPageNum;
            }
            else {
              let i = 0;
              for (const dat of Object.values(localStash)) {
                if (!dat) { continue; }
                if (dat.cache?.[key]) { pageIdx = i; break; }
                i++;
              }
            }

            // Get our final cache page destination.
            let pageName = cacheKey(collection, pageIdx);

            // If adding this key would put our key count over 15,000, create a new page.
            if (!localStash[pageName]?.cache?.[key] && Object.keys(localStash[pageName]?.cache || {}).length >= pageSize()) {
              pageIdx = pageIdx + 1;
              pageName = cacheKey(collection, pageIdx);
            }

            // If this page does not exist in our update yet, add one extra write to our count for ensuring the collection name.
            if (!updates[pageName]) { docCount++; }

            // Update remote object.
            const update: IFireStashPage<FirebaseFirestore.FieldValue> = updates[pageName] = updates[pageName] || { collection, cache: {} };
            update.cache[key] = FieldValue.increment(1);
            updateCount++;

            // Keep our local stash in sync to prevent unnessicary object syncs down the line.
            const page = localStash[pageName] = localStash[pageName] || { collection, cache: {} };

            // For each object we've been asked to update (run at least once even if no object was presented)...
            for (const obj of objects.length ? objects : [null]) {
              if (obj === DELETE_RECORD) {
                page.cache[key] = (page.cache[key] || 0) + 1;
                docUpdates.push([ 'delete', `${collection}/${key}`, null ]);
                docCount += 1; // +1 for object delete
              }
              else if (obj) {
                page.cache[key] = (page.cache[key] || 0) + 1;
                ensureFirebaseSafe(obj, FieldValue);
                docUpdates.push([ 'set', `${collection}/${key}`, obj ]);
                docCount += 1; // +1 for object merge
              }
              else {
                if (!this.options.lowMem) {
                  await (this.level as LevelSQLite).markDirty(`${collection}/${key}`)
                }
                page.cache[key] = (page.cache[key] || 0);
                const keys = events.get(collection) || new Set();
                keys.add(key);
                events.set(collection, keys);
              }

              // If we've hit the batch write limit, batch write these objects.
              if (docCount >= MAX_BATCH_SIZE || updateCount >= MAX_UPDATE_SIZE) {
                let batch = this.db.batch();
                for (const [ type, key, value ] of docUpdates) {
                  type === 'delete' && batch.delete(this.db.doc(key));
                  type === 'set' && batch.set(this.db.doc(key), value, { merge: true });
                }
                try {
                  await batch.commit();
                }
                catch (err) {
                  if (err.details?.startsWith('maximum entity size')) {
                    this.log.error('Content Document Too Large');
                    const batch = this.db.batch();
                    for (const [ type, key, value ] of docUpdates) {
                      type === 'delete' && batch.delete(this.db.doc(key));
                      type === 'set' && batch.set(this.db.doc(key), value, { merge: true });
                    }
                    await batch.commit();
                  }
                  else if (err.details === 'cannot have more than 500 field transforms on a single document') {
                    this.log.error('Excess Content Transforms Detected');
                  }
                  else {
                    this.log.error(err);
                  }
                }

                this.emit('save');
                docUpdates = [];
                docCount = 0;
                updateCount = 0;
                batch = this.db.batch();
              }
            }

            // Optimistically remove this key
            // TODO: Only remove after confirmed batch?
            keys.delete(key);
          }

          // Queue a commit of our collection's local state.
          this.stashPagesMemo[collection] = localStash;
          if (!this.options.lowMem) {
            localStash ? localBatch.put(collection, stringify(localStash)) : localBatch.del(collection);
          }

          // Re-calculate stashMemo on next stash() request.
          delete this.stashMemo[collection];
        }

        // Batch write the changes.
        let batch = this.db.batch();
        for (const [ type, key, value ] of docUpdates) {
          type === 'delete' && batch.delete(this.db.doc(key));
          type === 'set' && batch.set(this.db.doc(key), value, { merge: true });
        }

        try {
          await batch.commit();
        }
        catch (err) {
          if (err.details?.startsWith('maximum entity size')) {
            this.log.error('Content Document Too Large');
            const batch = this.db.batch();
            for (const [ type, key, value ] of docUpdates) {
              type === 'delete' && batch.delete(this.db.doc(key));
              type === 'set' && batch.set(this.db.doc(key), value, { merge: true });
            }
            await batch.commit();
          }
          else if (err.details === 'cannot have more than 500 field transforms on a single document') {
            this.log.error('Excess Content Transforms Detected');
          }
          else {
            this.log.error(err);
          }
        }

        type UpdatePage = Record<string, IFireStashPage<FirebaseFirestore.FieldValue | number>>;
        const commitPage = async(db: FirebaseFirestore.Firestore, batch: FirebaseFirestore.WriteBatch, updates: UpdatePage) => {
          try {
            await batch.commit();
          }
          catch (err) {
            if (err.details?.startsWith('maximum entity size')) {
              this.log.error('Cache Object Data Overflow Detected');
              for (const [ pageId, update ] of Object.entries(updates)) {
                try {
                  this.log.info(`Updating Cache Object ${pageId} With ${Object.keys(update.cache).length} Cache Keys`);
                  if (Object.keys(update.cache || {}).length) {
                    await db.collection('firestash').doc(pageId).set(update, { merge: true });
                  }
                }
                catch {
                  this.log.error(`Correcting Cache Object Data Overflow on ${pageId}`);
                  const localStash = collectionStashes.get(update.collection) || {};
                  const newPage = cacheKey(update.collection, Object.keys(localStash).length);
                  if (Object.keys(update.cache || {}).length) {
                    await db.collection('firestash').doc(newPage).set(update, { merge: true });
                  }
                }
              }
            }
            if (err.details === 'cannot have more than 500 field transforms on a single document') {
              this.log.error('Excess Cache Key Transforms Detected');
              this.log.log(updates);
            }
          }
        };

        let i = 0;
        let batchUpdates: UpdatePage = {};
        batch = this.db.batch();
        for (const [ pageId, page ] of Object.entries(updates)) {
          batchUpdates[pageId] = batchUpdates[pageId] || { collection: page.collection, cache: {} };
          for (const [ key, value ] of Object.entries(page.cache)) {
            i++;
            batchUpdates[pageId].cache[key] = value;
            if (i >= MAX_UPDATE_SIZE) {
              if (Object.keys(batchUpdates[pageId].cache || {}).length) {
                batch.set(this.db.collection('firestash').doc(pageId), batchUpdates[pageId], { merge: true });
              }
              await commitPage(this.db, batch, batchUpdates);
              batchUpdates = {};
              batchUpdates[pageId] = { collection: page.collection, cache: {} };
              batch = this.db.batch();
              i = 0;
            }
          }

          if (Object.keys(batchUpdates[pageId].cache || {}).length) {
            batch.set(this.db.collection('firestash').doc(pageId), batchUpdates[pageId], { merge: true });
          }
        }
        await commitPage(this.db, batch, batchUpdates);

        this.emit('save');

        // Save our local stash
        !this.options.lowMem && promises.push(localBatch.write());

        // Once all of our batch writes are done, re-balance our caches if needed and resolve.
        promises.length && await Promise.allSettled(promises);
        // Emit events for locally saved objects that we don't have to wait for a round trip on to access!
        for (const [ collection, keys ] of events) {
          for (const key of keys) {
            this.emit(`${collection}/${key}`, collection, [key]);
          }
          this.emit(collection, collection, [...keys]);
        }

        // Let everyone know we're done.
        this.emit('settled');
      }
      catch (err) {
        this.log.error(`[FireStash] Error persisting ${promises.length} FireStash data updates.`, err);
      }

      await Promise.all(commits);
    }
    catch (err) {
      this.log.error('Error running saveCacheUpdate', err);
      pending && this.#pendingUpdates.delete(pending);
      throw err;
    }
    pending && this.#pendingUpdates.delete(pending);
  }

  /**
   * Merge a remote updated collection stash with the local stash.
   * @param collection Collection Path
   * @param data IFireStashPage map of updates
   */
  private modified: Map<string, Set<string>> = new Map();
  private eventsTimer: NodeJS.Timeout | null = null;
  private async mergeRemote(collection: string, update: FirebaseFirestore.DocumentSnapshot[]): Promise<boolean> {
    // Fetch the local stash object for this collection that has just updated.
    const local: Record<string, IFireStashPage | undefined> = await this.stashPages(collection) || {};

    // Track modified document references to modified docs in this collection.
    const modified: [string, string][] = [];

    // Here we build our new local cache for the collection.
    const data: Record<string, IFireStashPage> = {};

    let i = 0;
    for (const doc of update) {
      const page = doc.id;
      const stash = doc.data() as IFireStashPage;
      data[page] = stash;
      const localPage = local[page] = local[page] || { collection, cache: {} };
      for (const [ id, value ] of Object.entries(stash.cache)) {
        !(++i % GET_PAGINATION_SIZE) && await new Promise(r => setTimeout(r, 25));
        // If we have the same cache generation id, we already have the latest (likely from a local update). Skip.
        if (localPage.cache[id] === value) { continue; }
        localPage.cache[id] = value;
        modified.push([ collection, id ]);
        const keys = this.modified.get(collection) || new Set();
        this.modified.set(collection, keys);
        keys.add(id);
      }
    }

    // Update the Cache Stash. For every updated object, delete it from our stash to force a re-fetch on next get().
    this.stashPagesMemo[collection] = local;

    // Re-compute on next stash() request.
    delete this.stashMemo[collection];

    if (!this.options.lowMem) {
      const batch = this.level.batch();
      batch.put(collection, stringify(local));
      let i = 0;
      for (const [ collection, id ] of modified) {
        (++i% 5) && new Promise(r => setTimeout(r, 10));
        await (this.level as LevelSQLite).markDirty(`${collection}/${id}`);
      }
      await batch.write();
    }

    // Destroy our memoized get() request if it exists for everything that has changed to ensure we fetch latest on next call.
    // Queue our events for a trigger.
    for (const [doc] of modified) {
      this.getRequestsMemo.delete(doc);
    }
    if (modified.length) {
      this.getRequestsMemo.delete(collection);
    }

    // Ensure we're scheduled for an event trigger.
    this.eventsTimer = this.eventsTimer || setTimeout(this.triggerChangeEvents.bind(this), 180);

    return !!modified.length;
  }

  /**
   * Triggered every 180ms when there are events in the queue to notify listeners in batches.
   * Companion function to `mergeRemote`.
   */
  private triggerChangeEvents() {
    // Emit events for locally saved objects that we don't have to wait for a round trip on to access!
    for (const [ collection, keys ] of this.modified) {
      for (const key of keys) {
        this.emit(`${collection}/${key}`, collection, [key]);
      }
      this.emit(collection, collection, [...keys]);
    }
    this.modified.clear();
    this.eventsTimer = null;
  }

  private async handleSnapshot(documentPath: string, snapshot: FirebaseFirestore.DocumentSnapshot) {
    const listener = this.documentListeners.get(documentPath);
    if (!listener) return;

    const data = snapshot.data();
    const now = snapshot.updateTime?.toMillis() || 0;
    const changed = !listener.data || now !== (listener.data?.updateTime?.toMillis() || 0);
    if (listener.releaseSnapshot) {
      // If our update is outside of our timeout windows, keep the subscription going.
      if (listener.timeout <= now - listener.updatedAt) {
        listener.count = 0;
        listener.updatedAt = snapshot.updateTime?.toMillis() || 0;
      }

      // If we've exceeded the number of updates within the timeout, switch to polling.
      if (changed && ++listener.count >= 3) {
        listener.releaseSnapshot();
        listener.releaseSnapshot = null;
        listener.count = 0;
        listener.intervalId = setInterval(() => this.db.doc(documentPath).get().then(this.handleSnapshot.bind(this, documentPath)), listener.timeout);
      }

      // Call all callbacks regardless as long as something has changed.
      changed && listener.callbacks.forEach(cb => cb(data));
    }
    else {
      // If we have detected a change within the polling interval, reset count and call the callbacks.
      if (changed) {
        listener.count = 0;
        listener.callbacks.forEach(cb => cb(data));
      }

      // If we have had no changed updates for at least 3x the polling window, fall back to the passive watcher.
      else if (++listener.count >= 3) {
        listener.count = 0;
        listener.intervalId && clearTimeout(listener.intervalId);
        listener.intervalId = null;
        listener.releaseSnapshot = this.db.doc(documentPath).onSnapshot(this.handleSnapshot.bind(this, documentPath));
      }
    }

    // Initial document reads don't count against quota.
    !listener.data && (listener.count = 0);

    // Update our listener with latest info.
    listener.data = snapshot;
  }

  /**
   * Watch for updates from a collection stash.
   * @param collection Collection Path
   */
  private watcherStarters: Record<string, undefined | true | Promise<() => void> | undefined> = {};
  async watch(collection: string): Promise<() => void> {
    if (collection === 'people') { return () => { 1; }; }
    // If we call this function on repeat, make sure we wait for the first call to be resolve.
    if (this.watcherStarters[collection]) { await this.watcherStarters[collection]; }

    // If we've already started the watcher, return.
    const pending = this.#watchers.get(collection);
    if (pending) { return pending; }

    let [ resolve, reject ]: [null | ((stop: () => void) => void), null | ((err: Error) => void)] = [ null, null ];
    const toDeferrable = (res: (stop: () => void) => void, rej: (err: Error) => void) => { resolve = res; reject = rej; };
    const localPromise = new Promise<() => void>(toDeferrable);
    this.watcherStarters[collection] = localPromise;

    let callsThisSecond = 0;
    let lastUpdate = 0;
    let liveWatcher: (() => void) | null = null;
    let timeoutId: NodeJS.Timeout | null = null;
    const query = this.db.collection('firestash').where('collection', '==', collection);
    const handleSnapshot = async(update: FirebaseFirestore.QuerySnapshot<FirebaseFirestore.DocumentData>) => {
      let isFirst = false;
      if (this.watcherStarters[collection]) {
        // If successfully completed first run.
        if (this.watcherStarters[collection] === localPromise) {
          delete this.watcherStarters[collection];
          isFirst = true;
        }

        // If timed out and we've started again, clear our own watchers.
        else {
          if (this.#watchers.get(collection) === liveWatcher) {
            this.#watchers.delete(collection);
          }
          liveWatcher?.();
          timeoutId && clearTimeout(timeoutId);
        }
      }

      const now = update.readTime?.toMillis() || 0;
      const docs: FirebaseFirestore.DocumentSnapshot[] = update.docChanges().map(change => change.doc);
      const changed = await this.mergeRemote(collection, docs);

      if (liveWatcher) {
        // Increment our callsThisSecond if called within a second of the last call, otherwise reset to zero.
        (now - lastUpdate < 1000) ? (callsThisSecond += 1) : (callsThisSecond = 0);
        if (callsThisSecond >= 3) {
          liveWatcher();
          liveWatcher = null;
          callsThisSecond = 0;
        }
        else {
          this.#watchers.set(collection, liveWatcher);
        }
      }

      if (!liveWatcher) { // Intentionally not an else. Needs to run if set to null in above if statement.
        (!changed) ? (callsThisSecond += 1) : (callsThisSecond = 0);
        if (callsThisSecond >= 3) {
          callsThisSecond = 0;
          liveWatcher = query.onSnapshot(handleSnapshot, reject || this.log.error);
          liveWatcher && this.#watchers.set(collection, liveWatcher);
        }
        else {
          timeoutId = setTimeout(() => query.get().then(handleSnapshot, reject), 1000);
          this.#watchers.set(collection, () => timeoutId && clearTimeout(timeoutId));
        }
      }

      lastUpdate = now;
      isFirst && resolve?.(() => this.unwatch(collection));
    };

    liveWatcher = query.onSnapshot(handleSnapshot, (err: Error) => {
      if (this.watcherStarters[collection] && this.watcherStarters[collection] === localPromise) {
        delete this.watcherStarters[collection];
      }
      if (this.#watchers.get(collection) === liveWatcher) {
        this.#watchers.delete(collection);
      }
      liveWatcher?.();
      timeoutId && clearTimeout(timeoutId);
      reject?.(err);
    });
    liveWatcher && this.#watchers.set(collection, liveWatcher);
    setTimeout(() => {
      if (this.watcherStarters[collection] && this.watcherStarters[collection] === localPromise) {
        this.log.error(`[FireStash] Timeout: "${collection}" Collection Watch Snapshot Timed Out`);
        if (this.#watchers.get(collection) === liveWatcher) {
          this.#watchers.delete(collection);
        }
        liveWatcher?.();
        timeoutId && clearTimeout(timeoutId);
        delete this.watcherStarters[collection];
        reject?.(new Error(`Timeout: "${collection}" Collection Watch Snapshot Timed Out`));
      }
    }, 180000); // 3 minute max timeout on listener start.

    // Return new promise that resolves after initial snapshot is done.
    return localPromise;
  }

  /**
   * Finish the last update and disconnect all watchers and timeouts.
   */
  async unwatch(collection: string) {
    this.#watchers.get(collection)?.();
    this.#watchers.delete(collection);
    await this.allSettled();
  }

  /**
   * Finish the last update and disconnect all watchers and timeouts.
   */
  private deleted = false;
  async stop() {
    if (this.deleted === true) { return; }
    this.deleted = true;

    // Wait for any pending operations to finish. We don't really care what the result is.
    try {
      await this.drainEventsPromise;
      await this.allSettled();
    } catch {1;}

    // Unbind watchers.
    for (const [ key, unsubscribe ] of this.#watchers.entries()) {
      unsubscribe();
      this.#watchers.delete(key);
    }

    // Close databases
    await this.level.close();
    await this.app.delete();

    // Clear timesouts
    this.timeout && clearTimeout(this.timeout as NodeJS.Timeout);
    this.timeout = null;
    this.timeoutPromise = null;

    // Remove process watchers.
    process.off('exit', () => this.stop);
    process.off('SIGHUP', () => this.stop);
    process.off('SIGINT', () => this.stop);
    process.off('SIGTERM', () => this.stop);
  }

  private async safeGet(collection: string, key: string): Promise<Buffer | null> {
    const id = `${collection}/${key}`;
    if (this.options.lowMem) { return null; }
    try {
      const o = await this.level.get(id, { asBuffer: true }) || null;
      return o;
    }
    catch (_err) { return null; }
  }

  private PENDING_FETCHES: Map<string, Promise<FirebaseFirestore.DocumentSnapshot<object>>> = new Map()
  private async fetchAllFromFirebase<T=object>(collection: string, idSet: Set<string>): Promise<Record<string, T>> {
    const start = Date.now();
    this.log.info(`[FireStash] Fetching ${idSet.size} "${collection}" records from remote starting at ${start}.`);
    const promises: Promise<FirebaseFirestore.DocumentSnapshot<T>>[] = Array.from(idSet).map(async id => {
      if (!this.PENDING_FETCHES.has(`${collection}/${id}`)) {
        this.PENDING_FETCHES.set(`${collection}/${id}`, this.db.collection(collection).doc(id).get())
      }
      return this.PENDING_FETCHES.get(`${collection}/${id}`) as Promise<FirebaseFirestore.DocumentSnapshot<T>>;
    })

    // Insert all stashes and docs into the local store.
    const out: Record<string, T> = {};
    const batch = this.level.batch();
    const resolutions = await Promise.allSettled(promises);

    for (const id of idSet) {
      this.PENDING_FETCHES.delete(`${collection}/${id}`);
      this.emit('fetch', collection, id);
    }
    
    for (let i = 0; i < resolutions.length; i++) {
      const res = resolutions[i];
      if (res.status === 'rejected') {
        this.log.error(`[FireStash] Error fetching a document for "${collection}" from remote.`, res.reason);
      }
      else {
        const id = res.value.id;
        const obj = out[id] = res.value.data() as T;
        if (obj) {
          const key = `${collection}/${id}`;
          const data = stringify(obj);
          setClean(data);
          batch.put(key, data);
        }
        else {
          batch.del(id);
        }
      }
    }
    batch.write(); // Fire and forget

    this.log.info(`[FireStash] Finished fetching ${promises.length} "${collection}" records from remote in ${((Date.now() - start) / 1000)}s.`);
    return out;

    // // Fetch all our updated documents.
    // const documents: FirebaseFirestore.DocumentSnapshot<T>[] = [];
    // let requestPageSize = MAX_READ_SIZE;
    // let missCount = 0;

    // // Retry getAll fetches at least three times. getAll in firebase is unreliable.
    // // You'll get most of the object very quickly, but some may take a second request.
    // while (idSet.size && requestPageSize > 1) {
    //   const ids = [...idSet];
    //   this.log.info(`[FireStash] Fetching ${ids.length} "${collection}" records from remote. Attempt ${missCount + 1}.`);
    //   const promises: Promise<FirebaseFirestore.DocumentSnapshot<T>[]>[] = [];
    //   for (let i = 0; i < Math.ceil(ids.length / requestPageSize); i++) {
    //     let fetching = new Set();
    //     // this.log.info(`[FireStash] Fetching page ${i + 1}/${Math.ceil(ids.length / requestPageSize)} for "${collection}".`);
    //     const toFetch = ids.slice(i * requestPageSize, (i + 1) * requestPageSize).map((id) => {
    //       // if (this.PENDING_FETCHES.has(id)) {
    //       //   idSet.delete(id);
    //       //   return null;
    //       // }
    //       // this.PENDING_FETCHES.add(id);
    //       fetching.add(id);
    //       this.emit('fetch', collection, id);
    //       return this.db.collection(collection).doc(id);
    //     }).filter(Boolean) as DocumentReference<DocumentData>[];
    //     const p = toFetch.length ? this.db.getAll(...toFetch) as Promise<FirebaseFirestore.DocumentSnapshot<T>[]> : Promise.resolve([]);
    //     promises.push(p);
    //     // Force get serialization since grpc can panic with too many open connections.
    //     try { await p; }
    //     catch { /* Rejection appropriately handled later. */ }
    //     for (const id of fetching) {
    //       this.PENDING_FETCHES.delete(id);
    //     }
    //     // this.log.info(`[FireStash] Done fetching page ${i + 1}/${Math.ceil(ids.length / requestPageSize)} for "${collection}".`);
    //   }

    //   const resolutions = await Promise.allSettled(promises);

    //   for (let i = 0; i < resolutions.length; i++) {
    //     const res = resolutions[i];
    //     if (res.status === 'rejected') {
    //       this.log.error(`[FireStash] Error fetching results page ${i} ${idSet.size} "${collection}" records from remote on attempt ${missCount + 1}.`, res.reason);
    //     }
    //     else {
    //       const docs = res.value;
    //       for (const data of docs) {
    //         documents.push(data);
    //         idSet.delete(data.id);
    //       }
    //     }
    //   }

    //   requestPageSize = Math.round(requestPageSize / 3); // With this magic math, we get up to 6 tries to get this right!
    //   missCount += 1;
    // }
  }

  /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
  private getRequestsMemo: Map<string, Promise<any>> = new Map();

  /**
   * Get the entire stash cache for a collection.
   * @param collection Collection Path
   */
  public async * stream<T=object>(collection: string, id?: string | string[], filter: string | null = null): AsyncGenerator<[string, T | null], void, void> {
    const idArr = id ? (Array.isArray(id) ? id : [id]) : [] as string[];
    const ids = new Set(idArr.map(id => `${collection}/${id}`));
    const toGet: string[] = [...ids];
    const modified: Set<string> = new Set();

    // Ensure we're getting the latest stash data for this collection.
    await this.watch(collection);

    if (this.options.lowMem) {
      for (const stash of Object.values(await this.stashPages(collection))) {
        const ids = id ? idArr : Object.keys(stash?.cache || {});
        for (const id of ids) {
          modified.add(id);
        }
      }
    }
    else if (ids.size > 0 && ids.size <= 30) {
      for (const id of idArr) {
        const record = await this.safeGet(collection, id);
        if (record) {
          isDirty(record) ? modified.add(id) : yield [ id, reify<T>(record) ];
        }
        else {
          await this.hasKey(collection, id) && modified.add(id);
        }
      }
    }
    else {
      const collectionPrefix = `${collection}/`;
      const sliceIdx = collectionPrefix.length;
      if (!toGet.length) {
        // Slightly faster to request the keys as strings instead of buffers.
        const iterator = this.level.iterator({ gt: collection, values: false, keys: true, keyAsBuffer: false, valueAsBuffer: false, filter });
        for await (const [id] of iterator as unknown as AsyncIterableIterator<string>) {
          if (id.slice(sliceIdx).includes('/')) { continue; }
          if (!id || (id.slice(0, sliceIdx) !== collectionPrefix)) { break; }
          // If is in the provided ID set, is not the collection itself, queue a get for this key
          if ((!ids.size || ids.has(id)) && id !== collection) { toGet.push(id); }
        }
      }

      let done = 0;
      let records: (Buffer | undefined)[] = [];
      for (const id of toGet) {
        if (done % GET_PAGINATION_SIZE === 0) {
          await new Promise(r => setTimeout(r, 25));
          records = await this.level.getMany(toGet.slice(done, done + GET_PAGINATION_SIZE), { asBuffer: true });
        }

        // Get the document key relative to this collection.
        const key = id.slice(sliceIdx);
        const record = records[done++ % GET_PAGINATION_SIZE];

        // If this is a sub-collection item, continue.
        if (key.indexOf('/') !== -1) { continue; }

        try {
          // If something went wrong reading the doc, mark it for fetching if we are watching for the latest updates in the ledger,
          // or if the property is tracked in the ledger, implying we have it on the remote server but not tracked locally.
          if (!record) {
            await this.hasKey(collection, key) && modified.add(key);
          }

          // If dirty, queue for fetch, otherwise add to our JSON return.
          else {
            try {
              if (filter && !record.includes(filter)) { continue; }
              // Faster than one big parse at end.
              isDirty(record) ? modified.add(key) : yield [ key, reify<T>(record) ];
            }
            catch (err) {
              this.log.error(`[FireStash] Error parsing batch get for '${collection}/${key}'.`, err);
              modified.add(key);
            }
          }
        }
        catch (err) {
          // If something went wrong reading the doc, mark it for fetching if we are watching for the latest updates in the ledger,
          // or if the property is tracked in the ledger, implying we have it on the remote server but not tracked locally.
          if (!err) {
            await this.hasKey(collection, key) && modified.add(key);
          }
        }
      }
    }

    if (modified.size) {
      const out = await this.fetchAllFromFirebase<T>(collection, modified);
      for (const message of Object.entries(out)) {
        yield message;
      }
    }
  }

  /**
   * Get the entire stash cache for a collection.
   * @param collection Collection Path
   */
  /* eslint-disable no-dupe-class-members */
  async get<T=object>(collection: string): Promise<Record<string, T | null>>;
  async get<T=object>(collection: string, id: string): Promise<T | null>;
  async get<T=object>(collection: string, id: string[]): Promise<Record<string, T | null>>;
  async get<T=object>(collection: string, id?: string | string[]): Promise<Record<string, T | null> | T | null> {
  /* eslint-enable no-dupe-class-members */
    const idArr = id ? (Array.isArray(id) ? id : [id]) : [] as string[];
    const ids = new Set(idArr.map(id => `${collection}/${id}`));
    const toGet: string[] = [...ids];
    const memoKey = `${collection}${id ? `/${[...ids].join(',')}` : ''}`;

    // If request is memoized, return the cached request. Prevents multiple quick requests from blowing out the cache.
    if (this.getRequestsMemo.has(memoKey)) {
      return this.getRequestsMemo.get(memoKey) as Promise<Record<string, T | null> | T | null>;
    }

    this.getRequestsMemo.set(memoKey, (async() => {
      const out: Record<string, T | null> = {};
      const modified: Set<string> = new Set();

      // Ensure we're getting the latest stash data for this collection.
      !this.options.lowMem && await this.watch(collection);
      if (this.options.lowMem) {
        for (const stash of Object.values(await this.stashPages(collection))) {
          const ids = id ? idArr : Object.keys(stash?.cache || {});
          for (const id of ids) {
            modified.add(id);
          }
        }
      }
      else if (ids.size > 0 && ids.size <= 30) {
        for (const id of idArr) {
          const record = await this.safeGet(collection, id);
          if (record) {
            isDirty(record) ? modified.add(id) : (out[id] = reify<T>(record));
          }
          else {
            await this.hasKey(collection, id) && modified.add(id);
          }
        }
      }
      else {
        const collectionPrefix = `${collection}/`;
        const sliceIdx = collectionPrefix.length;
        if (!toGet.length) {
          // Slightly faster to request the keys as strings instead of buffers.
          const iterator = this.level.iterator({ gt: collection, values: false, keys: true, keyAsBuffer: false, valueAsBuffer: false });
          for await (const [id] of iterator as unknown as AsyncIterableIterator<string>) {
            if (id.slice(sliceIdx).includes('/')) { continue; }
            if (!id || (id.slice(0, sliceIdx) !== collectionPrefix)) { break; }
            // If is in the provided ID set, is not the collection itself, queue a get for this key
            if ((!ids.size || ids.has(id)) && id !== collection) { toGet.push(id); }
          }
        }

        let done = 0;
        let records: (Buffer | undefined)[] = [];
        for (const id of toGet) {
          if (done % GET_PAGINATION_SIZE === 0) {
            await new Promise(r => setTimeout(r, 10));
            records = await this.level.getMany(toGet.slice(done, done + GET_PAGINATION_SIZE), { asBuffer: true });
          }

          // Get the document key relative to this collection.
          const key = id.slice(sliceIdx);
          const record = records[done++ % GET_PAGINATION_SIZE];

          // If this is a sub-collection item, continue.
          if (key.indexOf('/') !== -1) { continue; }

          // If something went wrong reading the doc, mark it for fetching if we are watching for the latest updates in the ledger,
          // or if the property is tracked in the ledger, implying we have it on the remote server but not tracked locally.
          if (!record) {
            await this.hasKey(collection, key) && modified.add(key);
          }

          // If dirty, queue for fetch, otherwise add to our JSON return.
          else {
            try {
              // Faster than one big parse at end.
              isDirty(record) ? modified.add(key) : out[key] = reify<T>(record);
            }
            catch (err) {
              this.log.error(`[FireStash] Error parsing batch get for '${collection}/${key}'.`, err);
              modified.add(key);
            }
          }
        }
      }
      if (modified.size) {
        const docs = await this.fetchAllFromFirebase<T>(collection, modified);
        for (const [id, obj] of Object.entries(docs)) {
          out[id] = obj;
        }
      }

      this.getRequestsMemo.delete(memoKey);
      return typeof id === 'string' ? (out[id] || null) : out;
    })().catch((err) => {
      this.getRequestsMemo.delete(memoKey);
      throw err;
    }));
    return this.getRequestsMemo.get(memoKey) as Promise<Record<string, T | null> | T | null>;
  }

  private localEvents: Map<string, Map<string, Set<object | typeof DELETE_RECORD> | null>> = new Map();
  private drainEventsTimer: NodeJS.Immediate | null = null;
  public drainEventsPromise: Promise<void> = Promise.resolve();
  private async drainEvents() {
    const batch = this.level.batch();
    for (const [ collection, records ] of this.localEvents) {
      const keyIds: string[] = [];
      for (const [ key, updates ] of records) {
        const id = `${collection}/${key}`;
        if (collection && key && updates?.size) {
          try {
            let localObj: Buffer | null = await this.safeGet(collection, key) || null;
            let val: object | null = localObj ? reify(localObj) : {};
            for (const obj of updates) {
              val = (obj === DELETE_RECORD) ? null : deepMerge(val, obj, { arrayMerge: overwriteMerge });
            }
            localObj = stringify(val);
            localObj && setClean(localObj);
            (localObj === null) ? batch.del(id) : batch.put(id, localObj);
          }
          catch (err) {
            this.log.error('[FireStash] Error draining event:', err);
          }
        }
        this.emit(id, collection, [key]);
        keyIds.push(key);
      }
      this.emit(collection, collection, keyIds);
    }
    await batch.write();
    this.localEvents.clear();
    this.drainEventsTimer = null;
  }

  /**
   * Increment a single document's generation cache key. Syncs to remote once per second.
   * @param collection Collection Path
   */
  async update(collection: string, key: string, obj: object | null = null) {
    // Ensure we're ready to trigger a remote update on next cycle.
    const localPromise = this.timeoutPromise = this.timeoutPromise || new Promise((resolve, reject) => {
      this.timeout = setTimeout(() => this.saveCacheUpdate().then(resolve, reject), 100);
    });

    // Queue update for next remote data sync (10 per second)
    const keys = this.toUpdate.get(collection) || new Map();
    this.toUpdate.set(collection, keys);
    const updates = keys.get(key) || [];
    updates.push(obj);
    keys.set(key, updates);

    // If we were provided an object, queue it for local update on next tick.
    if (obj) {
      const collectionMap = this.localEvents.get(collection) || new Map();
      this.localEvents.set(collection, collectionMap);
      const objSet = collectionMap.get(key) || new Set();
      collectionMap.set(key, objSet);
      objSet.add(obj);
      // Ensure we're ready to trigger events on next tick.
      if (!this.drainEventsTimer && !this.options.lowMem) {
        this.drainEventsTimer = setImmediate(() => this.drainEventsPromise = this.drainEvents());
      }
    }

    return localPromise;
  }

  /**
   * Increment a single document's generation cache key. Syncs to remote once per second.
   * @param collection Collection Path
   */
  async delete(collection: string, key: string) {
    // Ensure we're ready to trigger a remote update on next cycle.
    const localPromise = this.timeoutPromise = this.timeoutPromise || new Promise((resolve, reject) => {
      this.timeout = setTimeout(() => this.saveCacheUpdate().then(resolve, reject), 100);
    });

    // Queue update for next remote data sync (1 per second)
    const keys = this.toUpdate.get(collection) || new Map();
    this.toUpdate.set(collection, keys);
    const updates = keys.get(key) || [];
    keys.set(key, updates);
    updates.push(DELETE_RECORD);

    // If we were provided an object, queue it for local update on next tick.
    const collectionMap = this.localEvents.get(collection) || new Map();
    this.localEvents.set(collection, collectionMap);
    const objSet = collectionMap.get(key) || new Set();
    collectionMap.set(key, objSet);
    objSet.add(DELETE_RECORD);
    // Ensure we're ready to trigger events on next tick.
    if (!this.drainEventsTimer && !this.options.lowMem) {
      this.drainEventsTimer = setImmediate(() => this.drainEventsPromise = this.drainEvents());
    }

    // Ensure we're ready to trigger a remote update on next cycle.
    return localPromise;
  }

  /**
   * Bust all existing cache keys by incrementing by one.
   * @param collection Collection Path
   */
  async bust(collection: string, key?: string) {
    // Ensure we are watching for remote updates.
    await this.watch(collection);
    if (key) {
      this.update(collection, key);
    }
    else {
      const stash = await this.stashPages(collection);
      for (const page of Object.values(stash)) {
        if (!page) { continue; }
        for (const id of Object.keys(page.cache)) {
          this.update(collection, id);
        }
      }
    }
    await this.allSettled();
  }

  /**
   * Destroys any record of the collection in the stash.
   * @param collection Collection Path
   */
  async purge(collection: string) {
    // Ensure we are watching for remote updates.
    const pages = await this.db.collection('firestash').where('collection', '==', collection).get();
    const localPages = await this.stashPages(collection);
    await this.unwatch(collection);
    delete this.stashPagesMemo[collection];
    delete this.stashMemo[collection];

    for (const page of pages.docs) {
      await this.db.collection('firestash').doc(page.id).delete();
    }

    const batch = this.level.batch();
    for (const page of Object.values(localPages)) {
      if (!page || !page.cache) { continue; }
      for (const id of Object.keys(page.cache)) {
        batch.del(`${collection}/${id}`);
      }
    }

    batch.del(collection);
    await batch.write();
    await this.allSettled();
  }

  /**
   * Ensure all documents in the collection are present in the cache. Will not update existing cache keys.
   * @param collection Collection Path
   */
  async ensure(collection: string, key?: string) {
    // Ensure we are watching for remote updates.
    await this.watch(collection);
    if (key) {
      const obj = (await this.db.collection(collection).doc(key).get()).data() || {};
      await this.update(collection, key, obj);
      await this.allSettled();
      return;
    }
    const docs = (await this.db.collection(collection).get()).docs;
    for (const doc of docs) {
      if ((key && key !== doc.id) || await this.hasKey(collection, doc.id)) { continue; }
      this.update(collection, doc.id);
    }
    await this.allSettled();
  }

  /**
   * Balance the distribution of cache keys between all available pages.
   * @param collection Collection Path
   */
  async balance(collection: string) {
    const FieldValue = this.firebase.firestore.FieldValue;
    const remote = await this.stashPages(collection);

    let recordCount = 0;
    for (const dat of Object.values(remote)) {
      if (!dat) { continue; }
      recordCount += Object.keys(dat.cache || {}).length;
    }
    const pageCount = Math.ceil(recordCount / pageSize());
    const updates: Record<string, IFireStashPage<FirebaseFirestore.FieldValue | number>> = {};

    for (let i = 0; i < pageCount; i++) {
      const pageName = cacheKey(collection, i);
      updates[pageName] = { collection, cache: {} };
      remote[pageName] = remote[pageName] || { collection, cache: {} };
    }

    let docCount = 0;
    let updateCount = 0;
    let pending: Promise<WriteResult[]>[] = [];
    for (const [ id, dat ] of Object.entries(remote)) {
      if (!dat) { continue; }
      for (const [ key, value ] of Object.entries(dat.cache)) {
        const pageId = cacheKey(collection, numId(key) % pageCount);
        if (pageId === id) { continue; }
        const page = remote[pageId];
        if (!page) { continue; }

        updates[pageId].cache[key] = value;
        page.cache[key] = value;
        updateCount += 1;

        updates[id].cache[key] = FieldValue.delete();
        delete page.cache[key];

        docCount += 2;
        if (docCount >= (MAX_BATCH_SIZE - (pageCount * 2)) || updateCount >= MAX_UPDATE_SIZE) {
          const batch = this.db.batch();
          for (const [ id, page ] of Object.entries(updates)) {
            batch.set(this.db.collection('firestash').doc(id), page, { merge: true });
            updates[id] = { collection, cache: {} };
          }
          pending.push(batch.commit());
          if (pending.length >= 10) {
            await Promise.all(pending);
            pending = [];
          }
          docCount = 0;
          updateCount = 0;
        }
      }
    }

    await Promise.all(pending);

    const batch = this.db.batch();
    for (const [ id, page ] of Object.entries(updates)) {
      batch.set(this.db.collection('firestash').doc(id), page, { merge: true });
      updates[id] = { collection, cache: {} };
    }
    await batch.commit();
    this.emit('balance', collection);
  }
}
