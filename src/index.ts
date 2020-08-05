import Firebase from 'firebase-admin';
import { EventEmitter } from 'events';
import * as crypto from 'crypto';
import * as fs from 'fs-extra';
import * as path from 'path';
import { clearTimeout } from 'timers';
import levelup, { LevelUp } from 'levelup';
import encoding from 'encoding-down';
import rocksdb from 'rocksdb';
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
      NODE_ENV: 'development' | 'production';
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

const PAGINATION = 25000;
const BATCH_SIZE = 10;
function pageSize(): number {
  return process.env.FIRESTASH_PAGINATION || PAGINATION;
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

  /**
   * Create a new FireStash. Binds to the app database provided. Writes stash backups to disk if directory is provided.
   * @param firebase The Firebase Library.
   * @param app The Firebase App Instance.
   * @param directory The cache backup directory (optional)
   */
  constructor(firebase: typeof Firebase, app: Firebase.app.App, directory: string | null = null) {
    super();
    this.firebase = firebase;
    this.app = app;
    this.db = this.app.firestore();
    this.dir = directory;
    this.log = console;
    if (this.dir) {
      fs.mkdirSync(this.dir, { recursive: true });
      this.level = levelup(encoding(rocksdb(path.join(this.dir, '.firestash')), { keyEncoding: 'utf8', valueEncoding: 'json' }));
    }
    else {
      this.level = levelup(encoding(memdown(), { keyEncoding: 'utf8', valueEncoding: 'json' }));
    }
  }

  /**
   * Resolves when all previously called updates are written to remote. Like requestAnimationFrame for the collection cache.
   */
  allSettled() { return this.timeoutPromise; }

  private stashMemo: Record<string, IFireStash> = {};
  private stashPagesMemo: Record<string, Record<string, IFireStash | undefined>> = {};
  private async stashPages(collection: string): Promise<Record<string, IFireStash | undefined>> {
    if (this.stashPagesMemo[collection]) { return this.stashPagesMemo[collection]; }
    try {
      return this.stashPagesMemo[collection] = (await this.level.get(collection) || {} as Record<string, IFireStash>);
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
    this.timeout = null;
    const FieldValue = this.firebase.firestore.FieldValue;
    /* eslint-disable-next-line */
    const promises: Promise<any>[] = [];
    let count = 0;
    let updates = {};
    let batch = this.db.batch();
    const localBatch = this.level.batch();
    const collections = [...this.toUpdate.keys()];
    const collectionStashes: Map<string, Record<string, IFireStash | undefined>> = new Map();

    // Ensure we are watching for remote updates. Intentional fire and forget here.
    // We do this at the end of an update queue to not throttle our watchers on many new collection additions.
    let working: string[] = [];
    const collectionStashPromises = collections.map(async(collection, i) => {
      // If this collection has a watcher, we know we're up to date with latest as all times. Use existing.
      if (this.watchers.has(collection)) {
        collectionStashes.set(collection, await this.stashPages(collection));
        return;
      }

      // Push this collection name to our "to fetch" list
      working.push(collection);

      // Once we hit the limits of Firebase batch get queries, or the end of the list, fetch the latest stash cash.
      if (working.length === BATCH_SIZE || i === (collections.length - 1)) {
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

    try {
      for (const [ collection, keys ] of this.toUpdate.entries()) {
        // Get / ensure we have the remote cache object present.
        const localStash = collectionStashes.get(collection);
        if (!localStash) { continue; }

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
          let localObj: (object & InternalStash) = await this.safeGet(collection, key) || {};
          for (const obj of objects.length ? objects : [null]) {
            if (obj) {
              batch.set(this.db.doc(`${collection}/${key}`), obj, { merge: true });
              localObj && (localObj = deepMerge(localObj, obj, { arrayMerge: overwriteMerge }));
              count += 1; // +1 for object merge
            }
            else {
              localObj.__dirty__ = true;
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
          localObj && localBatch.put(`${collection}/${key}`, localObj);

          // Optimistically remove this key
          // TODO: Only remove after confirmed batch?
          keys.delete(key);
        }

        // Queue a commit of our collection's local state.
        localBatch.put(collection, localStash);
        this.stashPagesMemo[collection] = localStash;
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

      this.emit('settled');
    }
    catch (err) {
      this.log.error(`[FireStash] Error persisting FireStash ${promises.length} data updates.`, err);
    }
  }

  /**
   * Merge a remote updated collection stash with the local stash.
   * @param collection Collection Path
   * @param data IFireStash map of updates
   */
  private async mergeRemote(collection: string, update: FirebaseFirestore.QuerySnapshot<FirebaseFirestore.DocumentData>) {
    // Fetch the local stash object for this collection that has just updated.
    const local: Record<string, IFireStash | undefined> = await this.stashPages(collection) || {};

    // Track modified document references to modified docs in this collection.
    const modified: string[] = [];

    // Here we build our new local cache for the collection.
    const data: Record<string, IFireStash> = {};

    for (const doc of update.docs) {
      const page = doc.id;
      const stash = doc.data() as IFireStash;
      data[page] = stash;
      const localPage = local[page] = local[page] || { collection, cache: {} };
      for (const [ id, value ] of Object.entries(stash.cache)) {
        // If we have both the same cache generation id, and the actual object present, continue.
        if (localPage.cache[id] === value && !(await this.safeGet<object & InternalStash>(collection, id))?.__dirty__) { continue; }
        localPage.cache[id] = value;
        modified.push(`${collection}/${id}`);
      }
    }

    // Update the Cache Stash. For every updated object, delete it from our stash to force a re-fetch on next get().
    const batch = this.level.batch();
    batch.put(collection, local);
    this.stashPagesMemo[collection] = local;
    for (const doc of modified) { batch.del(doc); }
    await batch.write();

    // Destroy our memoized get() request if it exists for everything that has changed to ensure we fetch latest on next call.
    for (const doc of modified) { this.getRequestsMemo.delete(doc); }
    if (modified.length) { this.getRequestsMemo.delete(collection); }

    // Trigger our events!
    for (const doc of modified) { this.emit(doc); }
    if (modified.length) { this.emit(collection); }
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
      const unsubscribe = this.db.collection('firestash').where('collection', '==', collection).onSnapshot((update) => {
        this.mergeRemote(collection, update);
        firstCall && (firstCall = resolve());
      }, reject);
      this.watchers.set(collection, unsubscribe);
    });
  }

  /**
   * Finish the last update and disconnect all watchers and timeouts.
   */
  async unwatch(collection: string) {
    const unsubscribe = this.watchers.get(collection);
    unsubscribe && unsubscribe();
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

  private async safeGet<T=object>(collection: string, id: string): Promise<T | null> {
    try {
      return (await this.level.get(`${collection}/${id}`) || {}) as T;
    }
    catch (_err) {
      return null;
    }
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

  private getRequestsMemo: Map<string, Promise<any>> = new Map();

  /**
   * Get the entire stash cache for a collection.
   * @param collection Collection Path
   */
  /* eslint-disable no-dupe-class-members */
  async get<T=object>(collection: string): Promise<Record<string, T | null>>;
  async get<T=object>(collection: string, id: string): Promise<T | null>;
  async get<T=object>(collection: string, id?: string): Promise<Record<string, T | null> | T | null> {
  /* eslint-enable no-dupe-class-members */
    const memoKey = `${collection}/${id || ''}`;
    const stash = await this.stash(collection);
    const cache = id ? { [id]: stash.cache[id] } : stash.cache;
    if (this.getRequestsMemo.has(memoKey)) {
      return this.getRequestsMemo.get(memoKey) as Promise<Record<string, T | null> | T | null>;
    }
    this.getRequestsMemo.set(memoKey, (async() => {
      const out: Record<string, T | null> = {};
      const modified: Set<string> = new Set();
      for (const key of Object.keys(cache)) {
        const record = await this.safeGet<T & InternalStash>(collection, key);
        if (record && !record.__dirty__) { out[key] = record; }
        else { modified.add(key); }
      }

      if (modified.size) {
        const documents = await this.safeGetAll(collection, modified);

        // Insert all stashes and docs into the local store.
        const batch = this.level.batch();
        for (const doc of documents) {
          const obj = out[doc.id] = (doc.data() as (T & InternalStash)) || null;
          delete obj?.__dirty__;
          batch.put(`${collection}/${doc.id}`, obj);
        }
        await batch.write();
      }

      this.getRequestsMemo.delete(memoKey);
      return id ? (out[id] || null) : out;
    })());
    return this.getRequestsMemo.get(memoKey) as Promise<Record<string, T | null> | T | null>;
  }

  /**
   * Increment a single document's generation cache key. Syncs to remote once per second.
   * @param collection Collection Path
   */
  async update(collection: string, key: string, obj: object | null = null) {
    const keys = this.toUpdate.get(collection) || new Map();
    this.toUpdate.set(collection, keys);
    // If this collection+key combo has been called multiple times, don't try and set the local cache. Abort and pull form remote on next request.
    // TODO: Emulate FireStore's merge behavior here so we don't have to make the extra round drip?
    const updates = keys.get(key) || [];
    updates.push(obj);
    keys.set(key, updates);
    if (this.timeout) { return this.timeoutPromise; }
    return this.timeoutPromise = new Promise((resolve, reject) => {
      this.timeout = setTimeout(() => this.saveCacheUpdate().then(resolve, reject), 1000);
    });
  }

  /**
   * Bust all existing cache keys by incrementing by one.
   * @param collection Collection Path
   */
  async bust(collection: string) {
    // Ensure we are watching for remote updates.
    await this.watch(collection);
    const stash = await this.stashPages(collection);
    for (const page of Object.values(stash)) {
      if (!page) { continue; }
      for (const id of Object.keys(page.cache)) {
        this.update(collection, id);
      }
    }
    await this.allSettled();
  }

  /**
   * Ensure all documents in the collection are present in the cache. Will not update existing cache keys.
   * @param collection Collection Path
   */
  async ensure(collection: string) {
    // Ensure we are watching for remote updates.
    await this.watch(collection);
    const stash = (await this.stash(collection)) || { collection, cache: {} };
    const docs = (await this.db.collection(collection).get()).docs;
    for (const doc of docs) {
      if (stash.cache[doc.id]) { continue; }
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
