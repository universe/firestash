import 'firebase/compat/auth';
import 'firebase/compat/firestore';
import { 
  Firestore, 
  DocumentSnapshot, 
  initializeFirestore, 
  connectFirestoreEmulator, 
  collection, 
  FieldValue,
  WriteBatch,
  getDocs,
  query,
  where,
  doc,
  setDoc,
  deleteField,
  getDoc,
  deleteDoc,
  writeBatch,
  increment,
  onSnapshot,
  QuerySnapshot,
  DocumentData,
} from 'firebase/firestore';
import { FirebaseApp, initializeApp } from 'firebase/app';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import deepMerge from 'deepmerge';
import { nanoid } from 'nanoid';
// import * as LRU from 'lru-cache';
import LevelUp from 'levelup';
import MemDown from 'memdown';

import LevelSQLite from './sqlite.js';
import AbstractFireStash, { cacheKey, IFireStash, IFireStashPage, FireStashOptions, ServiceAccount } from './types.js';
import { deleteApp } from 'firebase-admin/app';

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
const MAX_READ_SIZE = 500;
const DELETE_RECORD = Symbol('firestash-delete');

interface ThrottledSnapshot {
  updatedAt: number;
  timeout: number;
  callbacks: Set<(snapshot: any) => void>;
  releaseSnapshot: (() => void) | null;
  intervalId: NodeJS.Timeout | null;
  data: DocumentSnapshot | null;
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

const PAGINATION = 15000;
const BATCH_SIZE = 10;
function pageSize(): number {
  return process.env.FIRESTASH_PAGINATION || PAGINATION;
}

/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
function ensureFirebaseSafe(obj: any, depth = 0): void {
  if (depth > 10) { return; }
  if (Array.isArray(obj)) { return; }
  if (typeof obj === 'object') {
    for (const key of Object.keys(obj)) {
      if (obj[key] === undefined) {
        obj[key] = deleteField();
      }
      else if (Object.prototype.toString.call(obj[key]) === '[object Object]') {
        ensureFirebaseSafe(obj[key], depth++);
      }
    }
  }
}

export default class FireStash extends AbstractFireStash {
  toUpdate: Map<string, Map<string, (object | typeof DELETE_RECORD | null)[]>> = new Map();
  app: FirebaseApp;
  db: Firestore;
  dir: string | null;
  log: typeof console;
  #watchers: Map<string, () => void> = new Map();
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
  constructor(config: ServiceAccount, options?: Partial<FireStashOptions> | undefined) {
    super(config, options);
    // const config = await (await window.fetch(`https://${pid}.firebaseapp.com/__/firebase/init.json`)).json();
    this.app = initializeApp(config, `${config.projectId}-firestash-${nanoid()}`);

    // Save ourselves from annoying throws. This cases should be handled in-library anyway.
    this.db = initializeFirestore(this.app, { ignoreUndefinedProperties: true });
    connectFirestoreEmulator(this.db, 'localhost', 5050);
    this.dir = this.options?.directory || null;
    this.log = console;

    // Clean up after ourselves if the process exits.
    this.stop = this.stop.bind(this);
    process.on('exit', () => this.stop);
    process.on('SIGHUP', () => this.stop);
    process.on('SIGINT', () => this.stop);
    process.on('SIGTERM', () => this.stop);

    if (this.options.datastore === 'sqlite' && this.dir) {
      fs.mkdirSync(this.dir, { recursive: true });
      this.level = new LevelSQLite(path.join(this.dir, '.firestash.db'));
    }
    else {
      this.level = LevelUp((MemDown as any)());
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
  allSettled() { return this.timeoutPromise || Promise.resolve(); }

  private stashPagesMemo: Record<string, Record<string, IFireStashPage | undefined>> = {};
  private async stashPages(collectionName: string): Promise<Record<string, IFireStashPage | undefined>> {
    if (this.options.lowMem) {
      const out: Record<string, IFireStashPage> = {};
      const res = await getDocs(query(collection(this.db, 'firestash'), where('collection', '==', collectionName)));
      for (const doc of res.docs) {
        const dat = doc.data() as IFireStashPage | undefined;
        dat && (out[doc.id] = dat);
      }
      return out;
    }
    if (this.stashPagesMemo[collectionName]) { return this.stashPagesMemo[collectionName]; }
    try {
      return this.stashPagesMemo[collectionName] = (reify(await this.level.get(collectionName, { asBuffer: true }) || null) as Record<string, IFireStashPage>) || {};
    }
    catch (_err) {
      return this.stashPagesMemo[collectionName] = {};
    }
  }

  /**
   * Get the entire stash cache for a collection.
   * @param collection Collection Path
   */
  private stashMemo: Record<string, IFireStashPage> = {};
  async stash(collection: string): Promise<IFireStashPage> {
    if (!this.options.lowMem && this.stashMemo[collection]) { return this.stashMemo[collection]; }

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

    return this.options.lowMem ? out : (this.stashMemo[collection] = out);
  }

  /**
   * Called once per second when there are updated queued. Writes all changes to remote stash cache.
   */
  private async saveCacheUpdate() {
    // Wait for our local events queue to finish before syncing remote.
    await this.drainEventsPromise;
    this.timeout = null;
    this.timeoutPromise = null;

    /* eslint-disable-next-line */
    const promises: Promise<any>[] = [];
    let docCount = 0;
    let updateCount = 0;
    const updates: Record<string, IFireStashPage<FieldValue>> = {};
    let docUpdates: ['set' | 'delete', string, any][] = [];
    const localBatch = this.level.batch();
    const entries = [...this.toUpdate.entries()];
    const collectionStashes: Map<string, Record<string, IFireStashPage | undefined>> = new Map();

    // Clear queue for further updates while we work.
    this.toUpdate.clear();

    // Ensure we are watching for remote updates. Intentional fire and forget here.
    // We do this at the end of an update queue to not throttle our watchers on many new collection additions.
    let working: string[] = [];
    const collectionStashPromises = entries.map(async([collectionName], i) => {
      // If this collection has a watcher, we know we're up to date with latest as all times. Use existing.
      if (this.#watchers.has(collectionName)) {
        collectionStashes.set(collectionName, await this.stashPages(collectionName));
        return;
      }

      // Push this collection name to our "to fetch" list
      working.push(collectionName);

      // Once we hit the limits of Firebase batch get queries, or the end of the list, fetch the latest stash cash.
      if (working.length === BATCH_SIZE || i === (entries.length - 1)) {
        const keywords = [...working];
        working = [];
        const stashes = await getDocs(query(collection(this.db, 'firestash'), where('collection', 'in', keywords)));
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
      for (const [ collectionName, keys ] of entries) {
        // Get / ensure we have the remote cache object present.
        const localStash = collectionStashes.get(collectionName) || {};
        collectionStashes.set(collectionName, localStash);

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
            const pageName = cacheKey(collectionName, pageCount);
            await setDoc(doc(this.db, 'firestash', pageName), { collection: collectionName, cache: {} }, { merge: true })
            localStash[pageName] = { collection: collectionName, cache: {} };
            pageCount += 1;
          }

          // Calculate the page we're adding to. In re-balance this is a md5 hash as decimal, mod page size, but on insert we just append to the last page present.
          let pageIdx = pageCount - 1;

          // Check to make sure this key doesn't already exist on remote. If it does, use its existing page.
          // TODO: Perf? This is potentially a lot of looping.
          const hasPageNum = numId(key) % pageCount;
          if (localStash[cacheKey(collectionName, hasPageNum)]?.cache?.[key]) {
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
          const pageName = cacheKey(collectionName, pageIdx);

          // If this page does not exist in our update yet, add one extra write to our count for ensuring the collection name.
          if (!updates[pageName]) { docCount++; }

          // Update remote object.
          const update: IFireStashPage<FieldValue> = updates[pageName] = updates[pageName] || { collection: collectionName, cache: {} };
          update.cache[key] = increment(1);
          updateCount += 1;

          // Keep our local stash in sync to prevent unnessicary object syncs down the line.
          const page = localStash[pageName] = localStash[pageName] || { collection: collectionName, cache: {} };
          page.cache[key] = (page.cache[key] || 0) + 1;

          // For each object we've been asked to update (run at least once even if no object was presented)...
          for (const obj of objects.length ? objects : [null]) {
            if (obj === DELETE_RECORD) {
              docUpdates.push([ 'delete', `${collectionName}/${key}`, null ]);
              docCount += 1; // +1 for object delete
            }
            else if (obj) {
              ensureFirebaseSafe(obj);
              docUpdates.push([ 'set', `${collectionName}/${key}`, obj ]);
              docCount += 1; // +1 for object merge
            }
            else {
              if (!this.options.lowMem) {
                this.safeGet(collectionName, key).then((localObj) => {
                  localObj = localObj || Buffer.from('1{}');
                  const id = `${collectionName}/${key}`;
                  setDirty(localObj);
                  localBatch.put(id, localObj);
                  // this.documentMemo.set(id, localObj);
                });
              }
              const keys = events.get(collectionName) || new Set();
              keys.add(key);
              events.set(collectionName, keys);
            }

            // If we've hit the batch write limit, batch write these objects.
            if (docCount >= MAX_BATCH_SIZE || updateCount >= MAX_UPDATE_SIZE) {
              let batch = writeBatch(this.db)
              for (const [ type, key, value ] of docUpdates) {
                type === 'delete' && batch.delete(doc(this.db, key));
                type === 'set' && batch.set(doc(this.db, key), value, { merge: true });
              }
              try {
                await batch.commit();
              }
              catch (err) {
                if (err.message?.includes('maximum entity size')) {
                  this.log.error('Content Document Too Large');
                  const batch = writeBatch(this.db);
                  for (const [ type, key, value ] of docUpdates) {
                    type === 'delete' && batch.delete(doc(this.db, key));
                    type === 'set' && batch.set(doc(this.db, key), value, { merge: true });
                  }
                  await batch.commit();
                }
                else if (err.message === 'cannot have more than 500 field transforms on a single document') {
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
              batch = writeBatch(this.db);
            }
          }

          // Optimistically remove this key
          // TODO: Only remove after confirmed batch?
          keys.delete(key);
        }

        // Queue a commit of our collection's local state.
        if (!this.options.lowMem) {
          localStash ? localBatch.put(collectionName, stringify(localStash)) : localBatch.del(collectionName);
          this.stashPagesMemo[collectionName] = localStash;
        }

        // Re-calculate stashMemo on next stash() request.
        delete this.stashMemo[collectionName];
      }

      // Batch write the changes.
      let batch = writeBatch(this.db);
      for (const [ type, key, value ] of docUpdates) {
        type === 'delete' && batch.delete(doc(this.db, key));
        type === 'set' && batch.set(doc(this.db, key), value, { merge: true });
      }

      try {
        await batch.commit();
      }
      catch (err) {
        if (err.message?.includes('maximum entity size')) {
          this.log.error('Content Document Too Large');
          const batch = writeBatch(this.db);
          for (const [ type, key, value ] of docUpdates) {
            type === 'delete' && batch.delete(doc(this.db, key));
            type === 'set' && batch.set(doc(this.db, key), value, { merge: true });
          }
          await batch.commit();
        }
        else if (err.message === 'cannot have more than 500 field transforms on a single document') {
          this.log.error('Excess Content Transforms Detected');
        }
        else {
          this.log.error(err);
        }
      }

      type UpdatePage = Record<string, IFireStashPage<FieldValue | number>>;
      const commitPage = async(batch: WriteBatch, updates: UpdatePage) => {
        try {
          await batch.commit();
        }
        catch (err) {
          if (err.message?.includes('maximum entity size')) {
            this.log.error('Cache Object Data Overflow Detected');
            for (const [ pageId, update ] of Object.entries(updates)) {
              try {
                this.log.info(`Updating Cache Object ${pageId} With ${Object.keys(update.cache).length} Cache Keys`);
                await setDoc(doc(this.db, 'firestash', pageId), update, { merge: true })
              }
              catch {
                this.log.error(`Correcting Cache Object Data Overflow on ${pageId}`);
                const localStash = collectionStashes.get(update.collection) || {};
                const newPage = cacheKey(update.collection, Object.keys(localStash).length);
                await setDoc(doc(this.db, 'firestash', newPage), update, { merge: true })
              }
            }
          }
          if (err.message === 'cannot have more than 500 field transforms on a single document') {
            this.log.error('Excess Cache Key Transforms Detected');
            this.log.log(updates);
          }
        }
      };

      let i = 0;
      let batchUpdates: UpdatePage = {};
      batch = writeBatch(this.db);
      for (const [ pageId, page ] of Object.entries(updates)) {
        batchUpdates[pageId] = batchUpdates[pageId] || { collection: page.collection, cache: {} };
        for (const [ key, value ] of Object.entries(page.cache)) {
          i++;
          batchUpdates[pageId].cache[key] = value;
          if (i >= MAX_UPDATE_SIZE) {
            batch.set(doc(this.db, 'firestash', pageId), batchUpdates[pageId], { merge: true });
            await commitPage(batch, batchUpdates);
            batchUpdates = {};
            batchUpdates[pageId] = { collection: page.collection, cache: {} };
            batch = writeBatch(this.db);
            i = 0;
          }
        }
        batch.set(doc(this.db, 'firestash', pageId), batchUpdates[pageId], { merge: true });
      }
      await commitPage(batch, batchUpdates);

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
  private async mergeRemote(collection: string, update: DocumentSnapshot[]): Promise<boolean> {
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
    !this.options.lowMem && (this.stashPagesMemo[collection] = local);

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
    if (this.deleted) { return; }
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

  private async handleSnapshot(documentPath: string, snapshot: DocumentSnapshot) {
    const listener = this.documentListeners.get(documentPath);
    const data = snapshot.data();
    if (!listener || this.deleted) return;

    const now = Date.now() || 0;
    const changed = !listener.data || now !== (listener.updatedAt || 0);
    if (listener.releaseSnapshot) {
      // If our update is outside of our timeout windows, keep the subscription going.
      if (listener.timeout <= now - listener.updatedAt) {
        listener.count = 0;
        listener.updatedAt = now || 0;
      }

      // If we've exceeded the number of updates within the timeout, switch to polling.
      if (changed && ++listener.count >= 3) {
        listener.releaseSnapshot();
        listener.releaseSnapshot = null;
        listener.count = 0;
        listener.intervalId = setInterval(() => getDoc(doc(this.db, documentPath)).then(this.handleSnapshot.bind(this, documentPath)), listener.timeout);
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
        listener.releaseSnapshot = onSnapshot(doc(this.db, documentPath), this.handleSnapshot.bind(this, documentPath));
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
        releaseSnapshot: onSnapshot(doc(this.db, documentPath), d => this.handleSnapshot(documentPath, d).then(() => resolve(unsubscribe)), reject),
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
   * @param collectionName Collection Path
   */
  private watcherStarters: Record<string, undefined | true | Promise<() => void> | undefined> = {};
  async watch(collectionName: string): Promise<() => void> {
    // If we call this function on repeat, make sure we wait for the first call to be resolve.
    if (this.watcherStarters[collectionName]) { await this.watcherStarters[collectionName]; }

    // If we've already started the watcher, return.
    const pending = this.#watchers.get(collectionName);
    if (pending) { return pending; }

    let [ resolve, reject ]: [null | ((stop: () => void) => void), null | ((err: Error) => void)] = [ null, null ];
    const toDeferrable = (res: (stop: () => void) => void, rej: (err: Error) => void) => { resolve = res; reject = rej; };
    const localPromise = new Promise<() => void>(toDeferrable);
    this.watcherStarters[collectionName] = localPromise;

    let callsThisSecond = 0;
    let lastUpdate = 0;
    let liveWatcher: (() => void) | null = null;
    let timeoutId: NodeJS.Timeout | null = null;
    const collectionQuery = query(collection(this.db, 'firestash'), where('collection', '==', collectionName));
    const handleSnapshot = async(update: QuerySnapshot<DocumentData>) => {
      let isFirst = false;

      if (this.watcherStarters[collectionName]) {
        // If successfully completed first run.
        if (this.watcherStarters[collectionName] === localPromise) {
          delete this.watcherStarters[collectionName];
          isFirst = true;
        }

        // If timed out and we've started again, clear our own watchers.
        else {
          if (this.#watchers.get(collectionName) === liveWatcher) {
            this.#watchers.delete(collectionName);
          }
          liveWatcher?.();
          timeoutId && clearTimeout(timeoutId);
        }
      }

      const now = Date.now() || 0;
      const docs: DocumentSnapshot[] = update.docChanges().map(change => change.doc);
      const changed = await this.mergeRemote(collectionName, docs);

      if (liveWatcher) {
        // Increment our callsThisSecond if called within a second of the last call, otherwise reset to zero.
        (now - lastUpdate < 1000) ? (callsThisSecond += 1) : (callsThisSecond = 0);
        if (callsThisSecond >= 3) {
          liveWatcher();
          liveWatcher = null;
          callsThisSecond = 0;
        }
        else {
          this.#watchers.set(collectionName, liveWatcher);
        }
      }

      if (!liveWatcher) { // Intentionally not an else. Needs to run if set to null in above if statement.
        (!changed) ? (callsThisSecond += 1) : (callsThisSecond = 0);
        if (callsThisSecond >= 3) {
          callsThisSecond = 0;
          liveWatcher = onSnapshot(collectionQuery, handleSnapshot, reject || console.error);
          this.#watchers.set(collectionName, liveWatcher);
        }
        else {
          timeoutId = setTimeout(() => getDocs(collectionQuery).then(handleSnapshot, reject), 1000);
          this.#watchers.set(collectionName, () => timeoutId && clearTimeout(timeoutId));
        }
      }

      lastUpdate = now;
      isFirst && resolve?.(() => this.unwatch(collectionName));
    };

    liveWatcher = onSnapshot(collectionQuery, handleSnapshot, (err) => {
      if (this.watcherStarters[collectionName] && this.watcherStarters[collectionName] === localPromise) {
        delete this.watcherStarters[collectionName];
      }
      if (this.#watchers.get(collectionName) === liveWatcher) {
        this.#watchers.delete(collectionName);
      }
      liveWatcher?.();
      timeoutId && clearTimeout(timeoutId);
      reject?.(err);
    });
    this.#watchers.set(collectionName, liveWatcher);
    setTimeout(() => {
      if (this.watcherStarters[collectionName] && this.watcherStarters[collectionName] === localPromise) {
        console.error(`[FireStash] Timeout: "${collectionName}" Collection Watch Snapshot Timed Out`);
        if (this.#watchers.get(collectionName) === liveWatcher) {
          this.#watchers.delete(collectionName);
        }
        liveWatcher?.();
        timeoutId && clearTimeout(timeoutId);
        delete this.watcherStarters[collectionName];
        reject?.(new Error(`Timeout: "${collectionName}" Collection Watch Snapshot Timed Out`));
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
    process.off('exit', () => this.stop);
    process.off('SIGHUP', () => this.stop);
    process.off('SIGINT', () => this.stop);
    process.off('SIGTERM', () => this.stop);
    for (const [ key, unsubscribe ] of this.#watchers.entries()) {
      unsubscribe?.();
      this.#watchers.delete(key);
    }
    await this.allSettled();
    try { await this.level.close(); } catch (err) { 1; }
    try { await deleteApp(this.app); } catch (err) { 1; }
    this.timeout && clearTimeout(this.timeout as NodeJS.Timeout);
    this.timeout = null;
    this.timeoutPromise = null;

    // Exit must be called at the end of the call stack to allow `runInWorker` to resolve for the parent process.
    setTimeout(() => process.exit(0), 1000)
  }

  // private documentMemo = new LRU<string, Buffer>({ max: 1000, maxSize: 400_000_000, sizeCalculation: b => b.length });
  private async safeGet(collection: string, key: string): Promise<Buffer | null> {
    const id = `${collection}/${key}`;
    // if (this.documentMemo.has(id)) { return this.documentMemo.get(id) || null; }
    if (this.options.lowMem) { return null; }
    try {
      const o = await this.level.get(id, { asBuffer: true }) || null;
      // !this.options.lowMem && (this.documentMemo.set(id, o));
      return o;
    }
    catch (_err) { return null; }
  }

  private async fetchAllFromFirebase<T=object>(collection: string, idSet: Set<string>): Promise<DocumentSnapshot<T>[]> {
    // Fetch all our updated documents.
    const start = Date.now();
    const documents: DocumentSnapshot<T>[] = [];
    let requestPageSize = MAX_READ_SIZE;
    let missCount = 0;

    // Retry getAll fetches at least three times. getAll in firebase is unreliable.
    // You'll get most of the object very quickly, but some may take a second request.
    while (idSet.size && requestPageSize > 1) {
      const ids = [...idSet];
      this.log.info(`[FireCache] Fetching ${ids.length} "${collection}" records from remote. Attempt ${missCount + 1}.`);
      const promises: Promise<DocumentSnapshot<T>>[] = [];

      for (const id of idSet) {
        promises.push(getDoc(doc(this.db, collection, id)) as unknown as Promise<DocumentSnapshot<T>>);
      }

      for (let i = 0; i < Math.ceil(ids.length / requestPageSize); i++) {
        // this.log.info(`[FireCache] Fetching page ${i + 1}/${Math.ceil(ids.length / requestPageSize)} for "${collection}".`);
        [...ids.slice(i * requestPageSize, (i + 1) * requestPageSize)].map((id) => {
          this.emit('fetch', collection, id);
          promises.push(getDoc(doc(this.db, collection, id)) as unknown as Promise<DocumentSnapshot<T>>);
        })
        // Force get serialization since grpc can panic with too many open connections.
        // try { await p; }
        // catch { /* Rejection appropriately handled later. */ }
        // this.log.info(`[FireCache] Done fetching page ${i + 1}/${Math.ceil(ids.length / requestPageSize)} for "${collection}".`);
      }

      const resolutions = await Promise.allSettled(promises);

      for (let i = 0; i < resolutions.length; i++) {
        const res = resolutions[i];
        if (res.status === 'fulfilled') {
          documents.push(res.value);
          idSet.delete(res.value.id);
        }
      }

      requestPageSize = Math.round(requestPageSize / 3); // With this magic math, we get up to 6 tries to get this right!
      missCount += 1;
    }
    this.log.info(`[FireCache] Finished fetching ${documents.length} "${collection}" records from remote in ${((Date.now() - start) / 1000)}s.`);

    return documents as DocumentSnapshot<T>[];
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
      const collectionPrefix = `${collection}/`;
      const sliceIdx = collectionPrefix.length;
      if (!toGet.length) {
        // Slightly faster to request the keys as strings instead of buffers.
        const iterator = this.level.iterator({ gt: collection, values: false, keys: true, keyAsBuffer: false, valueAsBuffer: false });
        for await (const [id] of iterator as unknown as AsyncIterableIterator<string>) {
          if (!id || (id.slice(0, sliceIdx) !== collectionPrefix)) { break; }
          // If is in the provided ID set, is not the collection itself, queue a get for this key
          if ((!ids.size || ids.has(id)) && id !== collection) { toGet.push(id); }
        }
      }

      let done = 0;
      let records: (Buffer | undefined)[] = [];
      for (const id of toGet) {
        if (done % GET_PAGINATION_SIZE === 0) {
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
            const docMightExist = !this.#watchers.get(collection) || Object.hasOwnProperty.call(stash.cache, id);
            docMightExist && modified.add(key);
          }

          // If dirty, queue for fetch, otherwise add to our JSON return.
          else {
            try {
              // Faster than one big parse at end.
              if (isDirty(record)) { modified.add(key); }
              else { yield [ key, reify<T>(record) ]; }
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
          // !this.options.lowMem && (this.documentMemo.set(id, data));
          yield [ doc.id, obj ];
        }
        else {
          batch.del(id);
          // this.documentMemo.delete(id);
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
          if (record && isClean(record) && !this.options.lowMem) {
            out[id] = reify<T>(record);
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
        const collectionPrefix = `${collection}/`;
        const sliceIdx = collectionPrefix.length;
        if (!toGet.length) {
          // Slightly faster to request the keys as strings instead of buffers.
          const iterator = this.level.iterator({ gt: collection, values: false, keys: true, keyAsBuffer: false, valueAsBuffer: false });
          for await (const [id] of iterator as unknown as AsyncIterableIterator<string>) {
            if (!id || (id.slice(0, sliceIdx) !== collectionPrefix)) { break; }
            // If is in the provided ID set, is not the collection itself, queue a get for this key
            if ((!ids.size || ids.has(id)) && id !== collection) { toGet.push(id); }
          }
        }

        let done = 0;
        let records: (Buffer | undefined)[] = [];
        for (const id of toGet) {
          if (done % GET_PAGINATION_SIZE === 0) {
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
            const docMightExist = !this.#watchers.get(collection) || Object.hasOwnProperty.call(stash.cache, id);
            docMightExist && modified.add(key);
          }

          // If dirty, queue for fetch, otherwise add to our JSON return.
          else {
            try {
              out[key] = reify<T>(record); // Faster than one big parse at end.
              isDirty(record) && modified.add(key);
            }
            catch (err) {
              this.log.error(`[FireStash] Error parsing batch get for '${collection}/${key}'.`, err);
              modified.add(key);
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
          const obj = out[doc.id] = (doc.data() as T | undefined) || null;
          if (obj) {
            const data = stringify(obj);
            setClean(data);
            batch.put(id, data);
            // !this.options.lowMem && (this.documentMemo.set(id, data));
          }
          else {
            batch.del(id);
            // this.documentMemo.delete(id);
          }
        }
        await batch.write();
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
          let localObj: Buffer | null = await this.safeGet(collection, key) || null;
          let val: object | null = localObj ? reify(localObj) : {};
          for (const obj of updates) {
            val = (obj === DELETE_RECORD) ? null : deepMerge(val, obj, { arrayMerge: overwriteMerge });
          }
          localObj = stringify(val);
          localObj && setClean(localObj);
          // this.documentMemo.set(id, localObj);
          (localObj === null) ? batch.del(id) : batch.put(id, localObj);
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
      this.timeout = setTimeout(() => this.saveCacheUpdate().then(resolve, reject), 1000);
    });

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

    return localPromise;
  }

  /**
   * Increment a single document's generation cache key. Syncs to remote once per second.
   * @param collection Collection Path
   */
  async delete(collection: string, key: string) {
    // Ensure we're ready to trigger a remote update on next cycle.
    const localPromise = this.timeoutPromise = this.timeoutPromise || new Promise((resolve, reject) => {
      this.timeout = setTimeout(() => this.saveCacheUpdate().then(resolve, reject), 1000);
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
   * @param collectionName Collection Path
   */
  async purge(collectionName: string) {
    // Ensure we are watching for remote updates.
    const pages = await getDocs(query(collection(this.db, 'firestash'), where('collection', '==', collectionName)));
    const localPages = await this.stashPages(collectionName);
    delete this.stashPagesMemo[collectionName];
    delete this.stashMemo[collectionName];

    for (const page of pages.docs) {
      await deleteDoc(doc(this.db, 'firestash', page.id));
    }

    const batch = this.level.batch();
    for (const page of Object.values(localPages)) {
      if (!page || !page.cache) { continue; }
      for (const id of Object.keys(page.cache)) {
        batch.del(`${collectionName}/${id}`);
      }
    }

    batch.del(collectionName);
    await batch.write();
    await this.allSettled();
  }

  /**
   * Ensure all documents in the collection are present in the cache. Will not update existing cache keys.
   * @param collectionName Collection Path
   */
  async ensure(collectionName: string, key?: string) {
    // Ensure we are watching for remote updates.
    await this.watch(collectionName);
    if (key) {
      const obj = (await getDoc(doc(this.db, collectionName, key))).data() || {};
      await this.update(collectionName, key, obj);
      await this.allSettled();
      return;
    }
    const stash = (await this.stash(collectionName)) || { collection: collectionName, cache: {} };
    const docs = (await getDocs(collection(this.db, collectionName))).docs;
    for (const doc of docs) {
      if ((key && key !== doc.id) || stash.cache[doc.id]) { continue; }
      this.update(collectionName, doc.id);
    }
    await this.allSettled();
  }

  /**
   * Balance the distribution of cache keys between all available pages.
   * @param collection Collection Path
   */
  async balance(collection: string) {
    const remote = await this.stashPages(collection);

    let recordCount = 0;
    for (const dat of Object.values(remote)) {
      if (!dat) { continue; }
      recordCount += Object.keys(dat.cache || {}).length;
    }
    const pageCount = Math.ceil(recordCount / pageSize());
    const updates: Record<string, IFireStashPage<FieldValue | number>> = {};

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

        updates[id].cache[key] = deleteField();
        delete page.cache[key];

        docCount += 2;
        if (docCount >= (MAX_BATCH_SIZE - (pageCount * 2)) || updateCount >= MAX_UPDATE_SIZE) {
          const batch = writeBatch(this.db)
          for (const [ id, page ] of Object.entries(updates)) {
            batch.set(doc(this.db, 'firestash', id), page, { merge: true });
            updates[id] = { collection, cache: {} };
          }
          await batch.commit();
          docCount = 0;
          updateCount = 0;
        }
      }
    }

    const batch = writeBatch(this.db)
    for (const [ id, page ] of Object.entries(updates)) {
      batch.set(doc(this.db, 'firestash', id), page, { merge: true });
      updates[id] = { collection, cache: {} };
    }
    await batch.commit();
    this.emit('balance', collection);
  }
}
