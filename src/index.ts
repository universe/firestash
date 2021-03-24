import type Firebase from 'firebase-admin';
import { EventEmitter } from 'events';
import * as crypto from 'crypto';
import * as fs from 'fs-extra';
import * as path from 'path';
import { clearTimeout } from 'timers';
import levelup, { LevelUp } from 'levelup';
import leveldown from 'leveldown';
import memdown from 'memdown';
import * as deepMerge from 'deepmerge';

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

/* eslint-disable-next-line */
const overwriteMerge = (_destinationArray: any[], sourceArray: any[]) => sourceArray;
const numId = (id: string) => [...crypto.createHash('md5').update(id).digest().values()].reduce((a, b) => a + b);

// We base64 encode page keys to safely represent deep collections, who's paths contain '/', in a flat list.
function encode(key: string) {
  return Buffer.from(key).toString('base64').replace(/=/g, '').replace(/\//g, '.');
}

function cacheKey(collection: string, page: number) { return encode(`${collection}-${page}`); }

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

interface InternalStash {
  __dirty__?: true;
}

export interface IFireStash<T = number> {
  collection: string;
  cache: Record<string, T>;
}

// function contextFor(collection: string) {
//   return [ 'firestash', ...collection.split('/').slice(0, -1) ].join('/');
// }

export interface FireStashOptions {
  lowMem: boolean;
  directory: string | null;
}

const DEFAULT_OPTIONS: FireStashOptions = {
  lowMem: false,
  directory: null,
};

export default class FireStash extends EventEmitter {
  toUpdate: Map<string, Map<string, (object | null)[]>> = new Map();
  firebase: typeof Firebase;
  app: Firebase.app.App;
  db: Firebase.firestore.Firestore;
  dir: string | null;
  log: typeof console;
  watchers: Map<string, () => void> = new Map();
  timeout: NodeJS.Timeout | null = null;
  timeoutPromise: Promise<void> = Promise.resolve();
  level: LevelUp;
  options: FireStashOptions = { ...DEFAULT_OPTIONS };

  /**
   * Create a new FireStash. Binds to the app database provided. Writes stash backups to disk if directory is provided.
   * @param firebase The Firebase Library.
   * @param app The Firebase App Instance.
   * @param directory The cache backup directory (optional)
   */
  constructor(firebase: typeof Firebase, app: Firebase.app.App, directory: string | Partial<FireStashOptions> | null = null) {
    super();
    this.firebase = firebase;
    this.app = app;
    this.db = this.app.firestore();
    this.dir = typeof directory === 'string' ? directory : directory?.directory || null;
    this.log = console;

    if (typeof directory === 'object') {
      Object.assign(this.options, directory);
    }

    if (this.dir) {
      fs.mkdirSync(this.dir, { recursive: true });
      this.level = levelup(leveldown(path.join(this.dir, '.firestash')));
    }
    else {
      this.level = levelup(memdown());
    }
  }

  cacheKey(collection: string, page: number) { return cacheKey(collection, page); }

  /**
   * Resolves when all previously called updates are written to remote. Like requestAnimationFrame for the collection cache.
   */
  allSettled() { return this.timeoutPromise; }

  private stashMemo: Record<string, IFireStash> = {};
  private stashPagesMemo: Record<string, Record<string, IFireStash | undefined>> = {};
  private async stashPages(collection: string): Promise<Record<string, IFireStash | undefined>> {
    if (this.stashPagesMemo[collection]) { return this.stashPagesMemo[collection]; }
    if (this.options.lowMem) {
      const out: Record<string, IFireStash> = {};
      const res = await this.db.collection('firestash').where('collection', '==', collection).get();
      for (const doc of res.docs) {
        const dat = doc.data() as IFireStash | undefined;
        dat && (out[doc.id] = dat);
      }
      return out;
    }
    try {
      return this.stashPagesMemo[collection] = (JSON.parse(await this.level.get(collection, { asBuffer: false }) || '{}') as Record<string, IFireStash>);
    }
    catch (_err) {
      return this.stashPagesMemo[collection] = {};
    }
  }

  /**
   * Get the entire stash cache for a collection.
   * @param collection Collection Path
   */
  async stash(collection: string): Promise<IFireStash> {
    if (this.stashMemo[collection]) { return this.stashMemo[collection]; }

    const pages = await this.stashPages(collection);
    const out: IFireStash = { collection, cache: {} };
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
    let count = 0;
    let updates = {};
    let batch = this.db.batch();
    const localBatch = this.level.batch();
    const entries = [...this.toUpdate.entries()];
    const collectionStashes: Map<string, Record<string, IFireStash | undefined>> = new Map();

    // Clear queue for further updates while we work.
    this.toUpdate.clear();

    // Ensure we are watching for remote updates. Intentional fire and forget here.
    // We do this at the end of an update queue to not throttle our watchers on many new collection additions.
    let working: string[] = [];
    const collectionStashPromises = entries.map(async([collection], i) => {
      // If this collection has a watcher, we know we're up to date with latest as all times. Use existing.
      if (this.watchers.has(collection)) {
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
          const data = stash.data() as IFireStash | undefined;
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
          documentCount += Object.keys(dat.cache).length;
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
          if (!updates[pageName]) { count++; }

          // Update remote object.
          const update: IFireStash<FirebaseFirestore.FieldValue> = updates[pageName] = updates[pageName] || { collection, cache: {} };
          update.cache[key] = FieldValue.increment(1);
          count += 1; // +1 for increment call.

          // Keep our local stash in sync to prevent unnessicary object syncs down the line.
          const page = localStash[pageName] = localStash[pageName] || { collection, cache: {} };
          page.cache[key] = (page.cache[key] || 0) + 1;

          // For each object we've been asked to update (run at least once even if no object was presented)...
          for (const obj of objects.length ? objects : [null]) {
            if (obj) {
              ensureFirebaseSafe(obj, FieldValue);
              batch.set(this.db.doc(`${collection}/${key}`), obj, { merge: true });
              count += 1; // +1 for object merge
            }
            else {
              if (!this.options.lowMem) {
                const localObj: (object & InternalStash) | null = await this.safeGet(collection, key) || {};
                const id = `${collection}/${key}`;
                localObj.__dirty__ = true;
                localBatch.put(id, JSON.stringify(localObj));
                this.documentMemo[id] = localObj;
              }
              const keys = events.get(collection) || new Set();
              keys.add(key);
              events.set(collection, keys);
            }

            // If we've hit the 500 write limit, batch write these objects.
            if (count >= 498) {
              for (const pageName of Object.keys(updates)) {
                batch.set(this.db.collection('firestash').doc(pageName), updates[pageName], { merge: true });
              }
              promises.push(batch.commit());
              this.emit('save');
              updates = {};
              count = 0;
              batch = this.db.batch();
            }
          }

          // Optimistically remove this key
          // TODO: Only remove after confirmed batch?
          keys.delete(key);
        }

        // Queue a commit of our collection's local state.
        if (!this.options.lowMem) {
          localStash ? localBatch.put(collection, JSON.stringify(localStash)) : localBatch.del(collection);
          this.stashPagesMemo[collection] = localStash;
        }

        // Re-calculate stashMemo on next stash() request.
        delete this.stashMemo[collection];
      }

      // Batch write the changes.
      for (const pageName of Object.keys(updates)) {
        batch.set(this.db.collection('firestash').doc(pageName), updates[pageName], { merge: true });
      }
      promises.push(batch.commit());
      this.emit('save');

      // Save our local stash
      promises.push(localBatch.write());

      // Once all of our batch writes are done, re-balance our caches if needed and resolve.
      await Promise.allSettled(promises);

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
      this.log.error(`[FireStash] Error persisting FireStash ${promises.length} data updates.`, err);
    }

    this.options.lowMem && collectionStashes.clear();
  }

  /**
   * Merge a remote updated collection stash with the local stash.
   * @param collection Collection Path
   * @param data IFireStash map of updates
   */
  private modified: Map<string, Set<string>> = new Map();
  private eventsTimer: NodeJS.Timeout | null = null;
  private async mergeRemote(collection: string, update: FirebaseFirestore.DocumentSnapshot[]): Promise<boolean> {
    // Fetch the local stash object for this collection that has just updated.
    const local: Record<string, IFireStash | undefined> = await this.stashPages(collection) || {};

    // Track modified document references to modified docs in this collection.
    const modified: [string, string][] = [];

    // Here we build our new local cache for the collection.
    const data: Record<string, IFireStash> = {};

    for (const doc of update) {
      const page = doc.id;
      const stash = doc.data() as IFireStash;
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
    batch.put(collection, JSON.stringify(local));
    this.stashPagesMemo[collection] = local;

    // Re-compute on next stash() request.
    delete this.stashMemo[collection];

    for (const [ collection, id ] of modified) {
      const obj = await this.safeGet<object & InternalStash>(collection, id) || {} as object & InternalStash;
      obj.__dirty__ = true;
      batch.put(`${collection}/${id}`, JSON.stringify(obj));
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

  async onThrottledSnapshot(
    documentPath: string,
    callback: (snapshot: FirebaseFirestore.DocumentSnapshot) => void,
    timeout: number = 1000
  ): Promise<void> {
    let callsThisTimeout = 0;
    let numNoChangeTimeouts = 0;
    let onSnapshotListener: (() => void) | null = null;

    const handleSnapshot = (snapshot: FirebaseFirestore.DocumentSnapshot) => {
      let timeoutId: NodeJS.Timeout;
      if (onSnapshotListener) {
        if (callsThisTimeout >= 3) {
          // Cancel the snapshot listener.
          onSnapshotListener();
  
          // Switch to polling for updates every timeout milliseconds.
          timeoutId = setTimeout(async() => {
            // If we go three timeout windows with no new data in the document while polling...
            if (numNoChangeTimeouts >= 3) {
              // Cancel the interval.
              clearTimeout(timeoutId);
              // Rebind the onSnapshot listener.
              onSnapshotListener = this.db.doc(documentPath).onSnapshot(handleSnapshot);
              callsThisTimeout = 0;
            } else {
              const doc = await this.db.doc(documentPath).get();
              const changed = await this.mergeRemote(documentPath, [doc]);
              if (changed) {
                callback(doc);
              } else {
                numNoChangeTimeouts++;
              }
            }
           },  timeout);
        } else {
          callback(snapshot);
          callsThisTimeout++;
        }
      }
    }

    // Set the snapshot listener.
    onSnapshotListener = this.db.doc(documentPath).onSnapshot(handleSnapshot);
  }

  /**
   * Watch for updates from a collection stash.
   * @param collection Collection Path
   */
  async watch(collection: string): Promise<() => void> {
    // If we've already started the watcher, return.
    const pending = this.watchers.get(collection);
    if (pending) { return pending; }

    // Return new promise that resolves after initial snapshot is done.
    return new Promise((resolve, reject) => {
      let firstCall: boolean | void = true;
      let callsThisSecond = 0;
      let lastUpdate = 0;
      let liveWatcher: (() => void) | null = null;
      const query = this.db.collection('firestash').where('collection', '==', collection);
      const handleSnapshot = async(update: FirebaseFirestore.QuerySnapshot<FirebaseFirestore.DocumentData>) => {
        const now = Date.now();
        const docs: FirebaseFirestore.DocumentSnapshot[] = [];
        update.docChanges().forEach((change) => docs.push(change.doc));

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

        if (!liveWatcher) {
          (!changed) ? (callsThisSecond += 1) : (callsThisSecond = 0);
          if (callsThisSecond >= 3) {
            callsThisSecond = 0;
            liveWatcher = query.onSnapshot(handleSnapshot, reject);
            this.watchers.set(collection, liveWatcher);
          }
          else {
            const timeoutId = setTimeout(() => query.get().then(handleSnapshot, reject), 1000);
            this.watchers.set(collection, () => clearTimeout(timeoutId));
          }
        }

        lastUpdate = now;
        firstCall && (firstCall = resolve());
      };

      liveWatcher = query.onSnapshot(handleSnapshot, reject);
      this.watchers.set(collection, liveWatcher);
    });
  }

  /**
   * Finish the last update and disconnect all watchers and timeouts.
   */
  async unwatch(collection: string) {
    const unsubscribe = this.watchers.get(collection);
    unsubscribe && unsubscribe();
    this.watchers.delete(collection);
    await this.allSettled();
  }

  /**
   * Finish the last update and disconnect all watchers and timeouts.
   */
  async stop() {
    for (const [ key, unsubscribe ] of this.watchers.entries()) {
      unsubscribe();
      this.watchers.delete(key);
    }
    await this.allSettled();
    this.timeout && clearTimeout(this.timeout);
    this.timeout = null;
  }

  private documentMemo: Record<string, object> = {};
  private async safeGet<T=object>(collection: string, key: string): Promise<T | null> {
    const id = `${collection}/${key}`;
    if (this.documentMemo[id]) { return this.documentMemo[id] as unknown as T; }
    if (this.options.lowMem) { return null; }
    try {
      const o = JSON.parse((await this.level.get(id, { asBuffer: false }) || '{}')) as T;
      !this.options.lowMem && (this.documentMemo[id] = o as unknown as object);
      return o;
    }
    catch (_err) { return null; }
  }

  private async safeGetAll<T=object>(collection: string, idSet: Set<string>): Promise<FirebaseFirestore.DocumentSnapshot<T>[]> {
    // Fetch all our updated documents.
    const start = Date.now();
    const documents: FirebaseFirestore.DocumentSnapshot<T>[] = [];
    let requestPageSize = 500;
    let missCount = 0;
    // Retry getAll fetches at least three times. getAll in firebase is unreliable.
    // You'll get most of the object very quickly, but some may take a second request.
    while (idSet.size && requestPageSize > 1) {
      const ids = [...idSet];
      this.log.info(`[FireCache] Fetching ${ids.length} "${collection}" records from remote. Attempt ${missCount + 1}.`);
      const promises: Promise<FirebaseFirestore.DocumentSnapshot<T>[]>[] = [];
      for (let i = 0; i < Math.ceil(ids.length / requestPageSize); i++) {
        const p = this.db.getAll(
          ...ids.slice(i * requestPageSize, (i + 1) * requestPageSize).map((id) => {
            this.emit('fetch', collection, id);
            return this.db.collection(collection).doc(id);
          }),
        ) as Promise<FirebaseFirestore.DocumentSnapshot<T>[]>;
        promises.push(p);
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
  /* eslint-disable no-dupe-class-members */
  async get<T=object>(collection: string): Promise<Record<string, T | null>>;
  async get<T=object>(collection: string, id: string): Promise<T | null>;
  async get<T=object>(collection: string, id: string[]): Promise<Record<string, T | null>>;
  async get<T=object>(collection: string, id?: string | string[]): Promise<Record<string, T | null> | T | null> {
  /* eslint-enable no-dupe-class-members */
    const ids = new Set((id ? Array.isArray(id) ? id : [id] : []));
    const memoKey = `${collection}${id ? `/${[...ids].join(',')}` : ''}`;

    if (this.getRequestsMemo.has(memoKey)) {
      return this.getRequestsMemo.get(memoKey) as Promise<Record<string, T | null> | T | null>;
    }
    this.getRequestsMemo.set(memoKey, (async() => {
      const out: Record<string, T | null> = {};
      const modified: Set<string> = new Set();
      if (ids.size > 0 && ids.size <= 30) {
        for (const id of ids) {
          const record = await this.safeGet<T & InternalStash>(collection, id);
          if (record && !record.__dirty__ && !this.options.lowMem) { out[id] = record; }
          else { modified.add(id); }
        }
      }
      else if (this.options.lowMem) {
        const ids = id ? (Array.isArray(id) ? id : [id]) : Object.keys((await this.stash(collection)).cache);
        for (const id of ids) {
          modified.add(id);
        }
      }
      else {
        await new Promise((resolve) => {
          const stream = this.level.createReadStream({ gt: collection, keyAsBuffer: false, valueAsBuffer: false });
          stream.on('data', (dat) => {
            const record = dat.value || '{}';

            const id = dat.key.toString('utf8') as string;

            // If the key doesn't start with the collection name, we're done!
            if (!id.startsWith(collection)) {
              /* eslint-disable-next-line */
              // @ts-ignore
              stream.destroy();
              resolve();
              return;
            }

            // Get the document key relative to this collection.
            const key = id.replace(collection + '/', '');

            // If is not in the provided ID set, is the collection itself, or a sub-collection, skip.
            if ((ids.size && !ids.has(key)) || id === collection || key.includes('/')) { return; }

            // If dirty, queue for fetch, otherwise add to our JSON return.
            if (record && !record.includes('__dirty__') && !this.options.lowMem) {
              try {
                out[key] = JSON.parse(record);
              }
              catch (err) {
                this.log.error(`[FireStash] Error parsing batch get for '${collection}/${key}'.`);
              }
            }
            else { modified.add(key); }
          });
          stream.on('error', () => resolve());
          stream.on('close', () => resolve());
          stream.on('end', () => resolve());
        });
      }

      /* eslint-disable-next-line */
      if (modified.size) {
        const documents = await this.safeGetAll(collection, modified);
        // Insert all stashes and docs into the local store.
        const batch = this.level.batch();
        for (const doc of documents) {
          const id = `${collection}/${doc.id}`;
          const obj = out[doc.id] = (doc.data() as (T & InternalStash)) || null;
          delete obj?.__dirty__;
          if (obj) {
            batch.put(id, JSON.stringify(obj));
            !this.options.lowMem && (this.documentMemo[id] = obj);
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

  private localEvents: Map<string, Map<string, Set<object> | null>> = new Map();
  private drainEventsTimer: NodeJS.Immediate | null = null;
  public drainEventsPromise: Promise<void> = Promise.resolve();
  private async drainEvents() {
    for (const [ collection, records ] of this.localEvents) {
      const keyIds: string[] = [];
      for (const [ key, updates ] of records) {
        const id = `${collection}/${key}`;
        if (collection && key && updates?.size) {
          let localObj: (object & InternalStash) = await this.safeGet(collection, key) || {};
          for (const obj of updates) {
            localObj && (localObj = deepMerge(localObj, obj, { arrayMerge: overwriteMerge }));
          }
          delete localObj.__dirty__;
          const val = JSON.stringify(localObj);
          await this.level.put(id, val);
          this.documentMemo[id] = JSON.parse(val);
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
    return this.timeoutPromise = this.timeout ? this.timeoutPromise : new Promise((resolve, reject) => {
      this.timeout = setTimeout(() => this.saveCacheUpdate().then(resolve, reject), 1000);
    });
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

    /* eslint-disable-next-line */
    const promises: Promise<any>[] = [];
    const remote = await this.stashPages(collection);

    let recordCount = 0;
    for (const dat of Object.values(remote)) {
      if (!dat) { continue; }
      recordCount += Object.keys(dat.cache).length;
    }
    const pageCount = Math.ceil(recordCount / pageSize());
    const updates: Record<string, IFireStash<FirebaseFirestore.FieldValue | number>> = {};

    for (let i = 0; i < pageCount; i++) {
      const pageName = cacheKey(collection, i);
      updates[pageName] = { collection, cache: {} };
      remote[pageName] = remote[pageName] || { collection, cache: {} };
    }

    let changeCount = 0;
    for (const [ id, dat ] of Object.entries(remote)) {
      if (!dat) { continue; }
      for (const [ key, value ] of Object.entries(dat.cache)) {
        const pageId = cacheKey(collection, numId(key) % pageCount);
        if (pageId === id) { continue; }
        const page = remote[pageId];
        if (!page) { continue; }

        updates[pageId].cache[key] = value;
        page.cache[key] = value;

        updates[id].cache[key] = FieldValue.delete();
        delete page.cache[key];

        changeCount += 2;
        if (changeCount >= (498 - (pageCount * 2))) {
          const batch = this.db.batch();
          for (const [ id, page ] of Object.entries(updates)) {
            batch.set(this.db.collection('firestash').doc(id), page, { merge: true });
            updates[id] = { collection, cache: {} };
          }
          promises.push(batch.commit());
          changeCount = 0;
        }
      }
    }

    const batch = this.db.batch();
    for (const [ id, page ] of Object.entries(updates)) {
      batch.set(this.db.collection('firestash').doc(id), page, { merge: true });
      updates[id] = { collection, cache: {} };
    }
    promises.push(batch.commit());
    this.emit('balance', collection);
    promises.length && await Promise.allSettled(promises);
  }
}
