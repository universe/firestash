import Firebase from 'firebase-admin';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import * as LevelUp from 'levelup';
import * as LevelDown from 'leveldown';
import * as RocksDb from 'rocksdb';
import * as MemDown from 'memdown';
import * as deepMerge from 'deepmerge';
import { nanoid } from 'nanoid';

import AbstractFireStash, { IFireStash, IFireStashPage, FireStashOptions, ServiceAccount } from './types';

export { IFireStash, IFireStashPage, FireStashOptions, ServiceAccount };

const levelup = LevelUp as unknown as typeof LevelUp.default;
const leveldown = LevelDown as unknown as typeof LevelDown.default;
const memdown = MemDown as unknown as typeof MemDown.default;
const rocksdb = RocksDb as unknown as typeof RocksDb.default;

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
const MAX_READ_SIZE = 500;
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
function isClean(data: Buffer): boolean { return data[0] === 49; } // Character code for "0";
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
const BATCH_SIZE = 10;
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

const DEFAULT_OPTIONS: FireStashOptions = {
  datastore: 'rocksdb',
  readOnly: false,
  lowMem: false,
  directory: null,
};

export default class FireStash extends AbstractFireStash {
  toUpdate: Map<string, Map<string, (object | typeof DELETE_RECORD | null)[]>> = new Map();
  firebase: typeof Firebase;
  app: Firebase.app.App;
  db: Firebase.firestore.Firestore;
  dir: string | null;
  log: typeof console;
  #watchers: Map<string, () => void> = new Map();
  timeout: NodeJS.Timeout | number | null = null;
  timeoutPromise: Promise<void> = Promise.resolve();
  level: LevelUp.LevelUp;
  options: FireStashOptions = { ...DEFAULT_OPTIONS };
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
    this.dir = options?.directory || null;
    this.log = console;

    // Save ourselves from annoying throws. This cases should be handled in-library anyway.
    this.db.settings({ ignoreUndefinedProperties: true });

    if (this.options.datastore === 'rocksdb' && this.dir) {
      fs.mkdirSync(this.dir, { recursive: true });
      this.level = levelup(rocksdb(path.join(this.dir, '.firestash.rocks')), { readOnly: this.options.readOnly });
    }
    else if (this.options.datastore === 'leveldown' && this.dir) {
      fs.mkdirSync(this.dir, { recursive: true });
      this.level = levelup(leveldown(path.join(this.dir, '.firestash')));
    }
    else {
      this.level = levelup(memdown());
    }

    // Set Immediate is better to use than the default nextTick for newer versions of Node.js.
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    this.level._nextTick = setImmediate;
  }

  cacheKey(collection: string, page: number) { return cacheKey(collection, page); }

  async watchers() { return new Set(this.#watchers.keys()); }

  /**
   * Resolves when all previously called updates are written to remote. Like requestAnimationFrame for the collection cache.
   */
  allSettled() { return this.timeoutPromise; }

  private stashMemo: Record<string, IFireStashPage> = {};
  private stashPagesMemo: Record<string, Record<string, IFireStashPage | undefined>> = {};
  private async stashPages(collection: string): Promise<Record<string, IFireStashPage | undefined>> {
    if (this.stashPagesMemo[collection]) { return this.stashPagesMemo[collection]; }
    if (this.options.lowMem) {
      const out: Record<string, IFireStashPage> = {};
      const res = await this.db.collection('firestash').where('collection', '==', collection).get();
      for (const doc of res.docs) {
        const dat = doc.data() as IFireStashPage | undefined;
        dat && (out[doc.id] = dat);
      }
      return out;
    }
    try {
      return this.stashPagesMemo[collection] = (reify(await this.level.get(collection, { asBuffer: true }) || null) as Record<string, IFireStashPage>) || {};
    }
    catch (_err) {
      return this.stashPagesMemo[collection] = {};
    }
  }

  /**
   * Get the entire stash cache for a collection.
   * @param collection Collection Path
   */
  async stash(collection: string): Promise<IFireStashPage> {
    if (this.stashMemo[collection]) { return this.stashMemo[collection]; }

    const pages = await this.stashPages(collection);
    const out: IFireStashPage = { collection, cache: {} };
    for (const dat of Object.values(pages)) {
      if (!dat) { continue; }
      Object.assign(out.cache, dat.cache);
    }

    return this.stashMemo[collection] = out;
  }

  /**
   * Called once per second when there are updated queued. Writes all changes to remote stash cache.
   */
  private async saveCacheUpdate() {
    // Wait for our local events queue to finish before syncing remote.
    await this.drainEventsPromise;

    this.timeout = null;
    const FieldValue = this.firebase.firestore.FieldValue;
    /* eslint-disable-next-line */
    const promises: Promise<any>[] = [];
    let docCount = 0;
    let updateCount = 0;
    const updates: Record<string, IFireStashPage<FirebaseFirestore.FieldValue>> = {};
    let docUpdates: ['set' | 'delete', string, any][] = [];
    const localBatch = this.level.batch();
    const entries = [...this.toUpdate.entries()];
    const collectionStashes: Map<string, Record<string, IFireStashPage | undefined>> = new Map();

    // Clear queue for further updates while we work.
    this.toUpdate.clear();

    // Ensure we are watching for remote updates. Intentional fire and forget here.
    // We do this at the end of an update queue to not throttle our watchers on many new collection additions.
    let working: string[] = [];
    const collectionStashPromises = entries.map(async([collection], i) => {
      // If this collection has a watcher, we know we're up to date with latest as all times. Use existing.
      if (this.#watchers.has(collection)) {
        collectionStashes.set(collection, await this.stashPages(collection));
        return;
      }

      // Push this collection name to our "to fetch" list
      working.push(collection);

      // Once we hit the limits of Firebase batch get queries, or the end of the list, fetch the latest stash cash.
      if (working.length === BATCH_SIZE || i === (entries.length - 1)) {
        const keywords = [...working];
        working = [];
        const stashes = await this.db.collection('firestash').where('collection', 'in', keywords).get();
        for (const stash of stashes.docs) {
          const data = stash.data() as IFireStashPage | undefined;
          if (!data) { continue; }
          const pageStashes = collectionStashes.get(data.collection) || {};
          collectionStashes.set(data.collection, pageStashes);
          pageStashes[stash.id] = data;
        }
        for (const collection of keywords) {
          if (collectionStashes.has(collection)) { continue; }
          collectionStashes.set(collection, { [cacheKey(collection, 0)]: { collection, cache: {} } });
        }
      }
    });

    // Wait to ensure we have all the latest stash caches.
    // We need this to ensure all we know what page to increment the generation number on.
    await Promise.allSettled(collectionStashPromises);

    const events: Map<string, Set<string>> = new Map();
    const commits: Promise<void>[] = [];
    try {
      for (const [ collection, keys ] of entries) {
        // Get / ensure we have the remote cache object present.
        const localStash = collectionStashes.get(collection) || {};
        collectionStashes.set(collection, localStash);

        // Get the stash's page and document count.
        let pageCount = Object.keys(localStash).length;
        let documentCount = 0;
        for (const dat of Object.values(localStash)) {
          if (!dat) { continue; }
          documentCount += Object.keys(dat.cache || {}).length;
        }

        // For each key queued, set the cache object.
        for (const [ key, objects ] of keys.entries()) {
          // Increment our document count to trigger page overflows in batched writes.
          documentCount += 1;

          // Expand the cache object pages to fit the page count balance.
          while (pageCount < Math.ceil(documentCount / pageSize())) {
            const pageName = cacheKey(collection, pageCount);
            await this.db.collection('firestash').doc(pageName).set({ collection, cache: {} }, { merge: true });
            localStash[pageName] = { collection, cache: {} };
            pageCount += 1;
          }

          // Calculate the page we're adding to. In re-balance this is a md5 hash as decimal, mod page size, but on insert we just append to the last page present.
          let pageIdx = pageCount - 1;

          // Check to make sure this key doesn't already exist on remote. If it does, use its existing page.
          // TODO: Perf? This is potentially a lot of looping.
          const hasPageNum = numId(key) % pageCount;
          if (localStash[cacheKey(collection, hasPageNum)]?.cache[key]) {
            pageIdx = hasPageNum;
          }
          else {
            let i = 0;
            for (const dat of Object.values(localStash)) {
              if (!dat) { continue; }
              if (dat.cache[key]) { pageIdx = i; break; }
              i++;
            }
          }

          // Get our final cache page destination.
          const pageName = cacheKey(collection, pageIdx);

          // If this page does not exist in our update yet, add one extra write to our count for ensuring the collection name.
          if (!updates[pageName]) { docCount++; }

          // Update remote object.
          const update: IFireStashPage<FirebaseFirestore.FieldValue> = updates[pageName] = updates[pageName] || { collection, cache: {} };
          update.cache[key] = FieldValue.increment(1);
          updateCount += 1;

          // Keep our local stash in sync to prevent unnessicary object syncs down the line.
          const page = localStash[pageName] = localStash[pageName] || { collection, cache: {} };
          page.cache[key] = (page.cache[key] || 0) + 1;

          // For each object we've been asked to update (run at least once even if no object was presented)...
          for (const obj of objects.length ? objects : [null]) {
            if (obj === DELETE_RECORD) {
              docUpdates.push([ 'delete', `${collection}/${key}`, null ]);
              docCount += 1; // +1 for object delete
            }
            else if (obj) {
              ensureFirebaseSafe(obj, FieldValue);
              docUpdates.push([ 'set', `${collection}/${key}`, obj ]);
              docCount += 1; // +1 for object merge
            }
            else {
              if (!this.options.lowMem) {
                this.safeGet(collection, key).then((localObj) => {
                  localObj = localObj || Buffer.from('1{}');
                  const id = `${collection}/${key}`;
                  setDirty(localObj);
                  localBatch.put(id, localObj);
                  this.documentMemo[id] = localObj;
                });
              }
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
        if (!this.options.lowMem) {
          localStash ? localBatch.put(collection, stringify(localStash)) : localBatch.del(collection);
          this.stashPagesMemo[collection] = localStash;
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
                await db.collection('firestash').doc(pageId).set(update, { merge: true });
              }
              catch {
                this.log.error(`Correcting Cache Object Data Overflow on ${pageId}`);
                const localStash = collectionStashes.get(update.collection) || {};
                const newPage = cacheKey(update.collection, Object.keys(localStash).length);
                await db.collection('firestash').doc(newPage).set(update, { merge: true });
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
            batch.set(this.db.collection('firestash').doc(pageId), batchUpdates[pageId], { merge: true });
            await commitPage(this.db, batch, batchUpdates);
            batchUpdates = {};
            batchUpdates[pageId] = { collection: page.collection, cache: {} };
            batch = this.db.batch();
            i = 0;
          }
        }
        batch.set(this.db.collection('firestash').doc(pageId), batchUpdates[pageId], { merge: true });
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

    this.options.lowMem && collectionStashes.clear();
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

    for (const doc of update) {
      const page = doc.id;
      const stash = doc.data() as IFireStashPage;
      data[page] = stash;
      const localPage = local[page] = local[page] || { collection, cache: {} };
      for (const [ id, value ] of Object.entries(stash.cache)) {
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
    const batch = this.level.batch();
    batch.put(collection, stringify(local));
    this.stashPagesMemo[collection] = local;

    // Re-compute on next stash() request.
    delete this.stashMemo[collection];

    for (const [ collection, id ] of modified) {
      const obj = await this.safeGet(collection, id) || Buffer.from('1{}');
      setDirty(obj);
      batch.put(`${collection}/${id}`, obj);
    }
    await batch.write();

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
    const data = snapshot.data();
    if (!listener) return;

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

  // Removes the watcher callback, and if there are no callbacks left, unsubscribes the watchers.
  private async unsubscribe(documentPath: string, callback: (snapshot: any) => void): Promise<void> {
    const listener = this.documentListeners.get(documentPath);
    listener?.callbacks.delete(callback);
    if (listener?.callbacks.size === 0) {
      listener.releaseSnapshot && listener.releaseSnapshot();
      listener.intervalId && clearTimeout(listener.intervalId);
      this.documentListeners.delete(documentPath);
    }
  }

  async onSnapshot<D = any>(
    documentPath: string,
    callback: (snapshot?: D) => any,
    timeout = 1000,
  ): Promise<() => void> {
    return new Promise((resolve, reject) => {
      const unsubscribe = this.unsubscribe.bind(this, documentPath, callback);

      // Get existing listener object if exists, otherwise create a new listener object and start watching the document.
      const listener = this.documentListeners.get(documentPath) || {
        updatedAt: Date.now(),
        timeout,
        callbacks: new Set([callback]),
        releaseSnapshot: this.db.doc(documentPath).onSnapshot(d => this.handleSnapshot(documentPath, d).then(() => resolve(unsubscribe)), reject),
        intervalId: null,
        data: null,
        count: 0,
      };

      // Ensure our listener is in the hash.
      this.documentListeners.set(documentPath, listener);

      // Add our callback to the list.
      listener.callbacks.add(callback);

      // If we're still waiting for initialization, we're done.
      if (!listener.data) { return; }

      // If already initialized, call this specific callback with our latest data.
      // eslint-disable-next-line node/no-callback-literal
      callback((listener.data.data() as D));

      // Resolve with the unsubscribe function.
      resolve(unsubscribe);
    });
  }

  /**
   * Watch for updates from a collection stash.
   * @param collection Collection Path
   */
  private watcherStarters: Record<string, undefined | true | Promise<() => void> | undefined> = {};
  async watch(collection: string): Promise<() => void> {
    // If we call this function on repeat, make sure we wait for the first call to be resolve.
    if (this.watcherStarters[collection]) { await this.watcherStarters[collection]; }

    // If we've already started the watcher, return.
    const pending = this.#watchers.get(collection);
    if (pending) { return pending; }

    const watch = (resolve: (stop: () => void) => void, reject: (err: Error) => void) => {
      let callsThisSecond = 0;
      let lastUpdate = 0;
      let liveWatcher: (() => void) | null = null;
      const query = this.db.collection('firestash').where('collection', '==', collection);
      const handleSnapshot = async(update: FirebaseFirestore.QuerySnapshot<FirebaseFirestore.DocumentData>) => {
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
        }

        if (!liveWatcher) { // Intentionally not an else. Needs to run if set to null in above if statement.
          (!changed) ? (callsThisSecond += 1) : (callsThisSecond = 0);
          if (callsThisSecond >= 3) {
            callsThisSecond = 0;
            liveWatcher = query.onSnapshot(handleSnapshot, reject);
            this.#watchers.set(collection, liveWatcher);
          }
          else {
            const timeoutId = setTimeout(() => query.get().then(handleSnapshot, reject), 1000);
            this.#watchers.set(collection, () => clearTimeout(timeoutId));
          }
        }

        lastUpdate = now;
        if (this.watcherStarters[collection]) {
          delete this.watcherStarters[collection];
          resolve(() => this.unwatch(collection));
        }
      };

      liveWatcher = query.onSnapshot(handleSnapshot, reject);
      this.#watchers.set(collection, liveWatcher);
    };

    // Return new promise that resolves after initial snapshot is done.
    return this.watcherStarters[collection] = new Promise<() => void>(watch);
  }

  /**
   * Finish the last update and disconnect all watchers and timeouts.
   */
  async unwatch(collection: string) {
    const unsubscribe = this.#watchers.get(collection);
    unsubscribe && unsubscribe();
    this.#watchers.delete(collection);
    await this.allSettled();
  }

  /**
   * Finish the last update and disconnect all watchers and timeouts.
   */
  async stop() {
    for (const [ key, unsubscribe ] of this.#watchers.entries()) {
      unsubscribe();
      this.#watchers.delete(key);
    }
    await this.allSettled();
    this.level.close();
    await this.app.delete();
    this.timeout && clearTimeout(this.timeout as NodeJS.Timeout);
    this.timeout = null;
  }

  private documentMemo: Record<string, Buffer> = {};
  private async safeGet(collection: string, key: string): Promise<Buffer | null> {
    const id = `${collection}/${key}`;
    if (this.documentMemo[id]) { return this.documentMemo[id]; }
    if (this.options.lowMem) { return null; }
    try {
      const o = await this.level.get(id, { asBuffer: true }) || Buffer.from('1{}');
      !this.options.lowMem && (this.documentMemo[id] = o);
      return o;
    }
    catch (_err) { return null; }
  }

  private async fetchAllFromFirebase<T=object>(collection: string, idSet: Set<string>): Promise<FirebaseFirestore.DocumentSnapshot<T>[]> {
    // Fetch all our updated documents.
    const start = Date.now();
    const documents: FirebaseFirestore.DocumentSnapshot<T>[] = [];
    let requestPageSize = MAX_READ_SIZE;
    let missCount = 0;
    // Retry getAll fetches at least three times. getAll in firebase is unreliable.
    // You'll get most of the object very quickly, but some may take a second request.
    while (idSet.size && requestPageSize > 1) {
      const ids = [...idSet];
      this.log.info(`[FireCache] Fetching ${ids.length} "${collection}" records from remote. Attempt ${missCount + 1}.`);
      const promises: Promise<FirebaseFirestore.DocumentSnapshot<T>[]>[] = [];
      for (let i = 0; i < Math.ceil(ids.length / requestPageSize); i++) {
        // this.log.info(`[FireCache] Fetching page ${i + 1}/${Math.ceil(ids.length / requestPageSize)} for "${collection}".`);
        const p = this.db.getAll(
          ...ids.slice(i * requestPageSize, (i + 1) * requestPageSize).map((id) => {
            this.emit('fetch', collection, id);
            return this.db.collection(collection).doc(id);
          }),
        ) as Promise<FirebaseFirestore.DocumentSnapshot<T>[]>;
        promises.push(p);
        // Force get serialization since grpc can panic with too many open connections.
        try { await p; }
        catch { /* Rejection appropriately handled later. */ }
        // this.log.info(`[FireCache] Done fetching page ${i + 1}/${Math.ceil(ids.length / requestPageSize)} for "${collection}".`);
      }

      const resolutions = await Promise.allSettled(promises);

      for (let i = 0; i < resolutions.length; i++) {
        const res = resolutions[i];
        if (res.status === 'rejected') {
          this.log.error(`[FireCache] Error fetching results page ${i} ${idSet.size} "${collection}" records from remote on attempt ${missCount + 1}.`, res.reason);
        }
        else {
          const docs = res.value;
          for (const data of docs) {
            documents.push(data);
            idSet.delete(data.id);
          }
        }
      }

      requestPageSize = Math.round(requestPageSize / 3); // With this magic math, we get up to 6 tries to get this right!
      missCount += 1;
    }
    this.log.info(`[FireCache] Finished fetching ${documents.length} "${collection}" records from remote in ${((Date.now() - start) / 1000)}s.`);

    return documents as FirebaseFirestore.DocumentSnapshot<T>[];
  }

  /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
  private getRequestsMemo: Map<string, Promise<any>> = new Map();

  /**
   * Get the entire stash cache for a collection.
   * @param collection Collection Path
   */
  async * stream<T=object>(collection: string, id?: string | string[]): AsyncGenerator<[string, T | null], void, void> {
    const idArr = id ? (Array.isArray(id) ? id : [id]) : [] as string[];
    const ids = new Set(idArr.map(id => `${collection}/${id}`));
    const toGet: string[] = [...ids];
    const modified: Set<string> = new Set();

    // Ensure we're getting the latest stash data for this collection.
    !this.options.lowMem && await this.watch(collection);
    const stash = await this.stash(collection);

    if (this.options.lowMem) {
      const ids = id ? idArr : Object.keys(stash.cache);
      for (const id of ids) {
        modified.add(id);
      }
    }
    else if (ids.size > 0 && ids.size <= 30) {
      for (const id of idArr) {
        const record = await this.safeGet(collection, id);
        if (record && isClean(record) && !this.options.lowMem) {
          yield [ id, reify<T>(record) ];
        }
        else if (record && isDirty(record)) {
          modified.add(id);
        }
        else {
          const docMightExist = !this.#watchers.get(collection) || Object.hasOwnProperty.call(stash.cache, id);
          docMightExist && modified.add(id);
        }
      }
    }
    else {
      // Slightly faster to request the keys as strings instead of buffers.
      const iterator = this.level.iterator({ gt: collection, values: false, keys: true, keyAsBuffer: false, valueAsBuffer: false });
      const collectionPrefix = `${collection}/`;
      const sliceIdx = collectionPrefix.length;
      if (!toGet.length) {
        await new Promise<void>((resolve, reject) => {
          const process = (err?: Error | null, id?: string) => {
            // If the key doesn't start with the collection name, we're done!
            if (err || id === undefined || id.slice(0, sliceIdx) !== collectionPrefix) {
              return iterator.end(() => err ? reject(err) : resolve());
            }

            // Queue up the next request!
            iterator.next(process);

            // If is in the provided ID set, is not the collection itself, queue a get for this key
            if ((!ids.size || ids.has(id)) && id !== collection) {
              toGet.push(id);
            }
          };
          iterator.next(process);
        });
      }

      let done = 0;
      for (const id of toGet) {
        // Get the document key relative to this collection.
        const key = id.slice(sliceIdx);

        // If this is a sub-collection item, continue.
        if (key.indexOf('/') !== -1) {
          if (++done === toGet.length) { break; }
          continue;
        }

        try {
          const record = await this.level.get(id, { asBuffer: true });
          // If something went wrong reading the doc, mark it for fetching if we are watching for the latest updates in the ledger,
          // or if the property is tracked in the ledger, implying we have it on the remote server but not tracked locally.
          if (!record) {
            const docMightExist = !this.#watchers.get(collection) || Object.hasOwnProperty.call(stash.cache, id);
            docMightExist && modified.add(key);
          }

          // If dirty, queue for fetch, otherwise add to our JSON return.
          else {
            try {
              // Faster than one big parse at end.
              if (isDirty(record)) {
                modified.add(key);
              }
              else {
                yield [ key, reify<T>(record) ];
              }
            }
            catch (err) {
              this.log.error(`[FireStash] Error parsing batch get for '${collection}/${key}'.`, err);
              modified.add(key);
            }
          }
          if (++done === toGet.length) { break; }
        }
        catch (err) {
          // If something went wrong reading the doc, mark it for fetching if we are watching for the latest updates in the ledger,
          // or if the property is tracked in the ledger, implying we have it on the remote server but not tracked locally.
          if (!err) {
            const docMightExist = !this.#watchers.get(collection) || Object.hasOwnProperty.call(stash.cache, id);
            docMightExist && modified.add(key);
          }
        }
      }
    }

    if (modified.size) {
      const documents = await this.fetchAllFromFirebase(collection, modified);
      // Insert all stashes and docs into the local store.
      const batch = this.level.batch();
      for (const doc of documents) {
        const id = `${collection}/${doc.id}`;
        const obj = (doc.data() as T | undefined) || null;
        if (obj) {
          const data = stringify(obj);
          setClean(data);
          batch.put(id, data);
          !this.options.lowMem && (this.documentMemo[id] = data);
          yield [ doc.id, obj ];
        }
        else {
          batch.del(id);
          delete this.documentMemo[id];
        }
      }
      await batch.write();
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
      const stash = await this.stash(collection);

      if (this.options.lowMem) {
        const ids = id ? idArr : Object.keys(stash.cache);
        for (const id of ids) {
          modified.add(id);
        }
      }
      else if (ids.size > 0 && ids.size <= 30) {
        for (const id of idArr) {
          const record = await this.safeGet(collection, id);
          if (record && isClean(record) && !this.options.lowMem) { out[id] = reify<T>(record); }
          else if (record && isDirty(record)) {
            modified.add(id);
          }
          else {
            const docMightExist = !this.#watchers.get(collection) || Object.hasOwnProperty.call(stash.cache, id);
            docMightExist && modified.add(id);
          }
        }
      }
      else {
        // Slightly faster to request the keys as strings instead of buffers.
        const iterator = this.level.iterator({ gt: collection, values: false, keys: true, keyAsBuffer: false, valueAsBuffer: false });
        const collectionPrefix = `${collection}/`;
        const sliceIdx = collectionPrefix.length;
        if (!toGet.length) {
          await new Promise<void>((resolve, reject) => {
            const process = (err?: Error | null, id?: string) => {
              // If the key doesn't start with the collection name, we're done!
              if (err || id === undefined || id.slice(0, sliceIdx) !== collectionPrefix) {
                return iterator.end(() => err ? reject(err) : resolve());
              }

              // Queue up the next request!
              iterator.next(process);

              // If is in the provided ID set, is not the collection itself, queue a get for this key
              if ((!ids.size || ids.has(id)) && id !== collection) {
                toGet.push(id);
              }
            };
            iterator.next(process);
          });
        }

        await new Promise<void>((resolve) => {
          let done = 0;
          if (toGet.length === 0) { resolve(); }
          for (const id of toGet) {
            // Get the document key relative to this collection.
            const key = id.slice(sliceIdx);

            // If this is a sub-collection item, continue.
            if (key.indexOf('/') !== -1) {
              if (++done === toGet.length) { resolve(); }
              continue;
            }

            this.level.get(id, { asBuffer: true }, (err, record) => {
              // If something went wrong reading the doc, mark it for fetching if we are watching for the latest updates in the ledger,
              // or if the property is tracked in the ledger, implying we have it on the remote server but not tracked locally.
              if (err || !record) {
                const docMightExist = !this.#watchers.get(collection) || Object.hasOwnProperty.call(stash.cache, id);
                docMightExist && modified.add(key);
              }

              // If dirty, queue for fetch, otherwise add to our JSON return.
              else {
                try {
                  // Faster than one big parse at end.
                  out[key] = reify<T>(record);
                  if (isDirty(record)) {
                    modified.add(key);
                  }
                }
                catch (err) {
                  this.log.error(`[FireStash] Error parsing batch get for '${collection}/${key}'.`, err);
                  modified.add(key);
                }
              }
              if (++done === toGet.length) { resolve(); }
            });
          }
        });
      }

      if (modified.size) {
        const documents = await this.fetchAllFromFirebase(collection, modified);
        // Insert all stashes and docs into the local store.
        const batch = this.level.batch();
        for (const doc of documents) {
          const id = `${collection}/${doc.id}`;
          const obj = out[doc.id] = (doc.data() as T | undefined) || null;
          if (obj) {
            const data = stringify(obj);
            setClean(data);
            batch.put(id, data);
            !this.options.lowMem && (this.documentMemo[id] = data);
          }
          else {
            batch.del(id);
            delete this.documentMemo[id];
          }
        }
        await batch.write();
      }

      this.getRequestsMemo.delete(memoKey);
      return typeof id === 'string' ? (out[id] || null) : out;
    })());
    return this.getRequestsMemo.get(memoKey) as Promise<Record<string, T | null> | T | null>;
  }

  private localEvents: Map<string, Map<string, Set<object | typeof DELETE_RECORD> | null>> = new Map();
  private drainEventsTimer: NodeJS.Immediate | null = null;
  public drainEventsPromise: Promise<void> = Promise.resolve();
  private async drainEvents() {
    for (const [ collection, records ] of this.localEvents) {
      const keyIds: string[] = [];
      for (const [ key, updates ] of records) {
        const id = `${collection}/${key}`;
        if (collection && key && updates?.size) {
          let localObj: Buffer | null = await this.safeGet(collection, key) || null;
          let val: object | null = localObj ? reify(localObj) : {};
          for (const obj of updates) {
            val = (obj === DELETE_RECORD) ? null : deepMerge(val, obj, { arrayMerge: overwriteMerge });
          }
          localObj = stringify(val);
          localObj && setClean(localObj);
          this.documentMemo[id] = localObj;
          (localObj === null) ? await this.level.del(id) : await this.level.put(id, localObj);
        }
        this.emit(id, collection, [key]);
        keyIds.push(key);
      }
      this.emit(collection, collection, keyIds);
    }
    this.localEvents.clear();
    this.drainEventsTimer = null;
  }

  /**
   * Increment a single document's generation cache key. Syncs to remote once per second.
   * @param collection Collection Path
   */
  async update(collection: string, key: string, obj: object | null = null) {
    // Queue update for next remote data sync (1 per second)
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

    // Ensure we're ready to trigger a remote update on next cycle.
    if (this.timeout) { return this.timeoutPromise; }
    this.timeout = 1;
    return this.timeoutPromise = this.timeoutPromise.then(() => new Promise((resolve, reject) => {
      this.timeout = setTimeout(() => this.saveCacheUpdate().then(resolve, reject), 1000);
    }));
  }

  /**
   * Increment a single document's generation cache key. Syncs to remote once per second.
   * @param collection Collection Path
   */
  async delete(collection: string, key: string) {
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
    if (this.timeout) { return this.timeoutPromise; }
    this.timeout = 1;
    return this.timeoutPromise = this.timeoutPromise.then(() => new Promise((resolve, reject) => {
      this.timeout = setTimeout(() => this.saveCacheUpdate().then(resolve, reject), 1000);
    }));
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
    const stash = (await this.stash(collection)) || { collection, cache: {} };
    const docs = (await this.db.collection(collection).get()).docs;
    for (const doc of docs) {
      if ((key && key !== doc.id) || stash.cache[doc.id]) { continue; }
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
          await batch.commit();
          docCount = 0;
          updateCount = 0;
        }
      }
    }

    const batch = this.db.batch();
    for (const [ id, page ] of Object.entries(updates)) {
      batch.set(this.db.collection('firestash').doc(id), page, { merge: true });
      updates[id] = { collection, cache: {} };
    }
    await batch.commit();
    this.emit('balance', collection);
  }
}
