import Firebase from 'firebase-admin';
import { EventEmitter } from 'events';
import * as crypto from 'crypto';
import * as fs from 'fs-extra';
import * as path from 'path';
import { clearTimeout } from 'timers';
import levelup, { LevelUp } from 'levelup';
import rocksdb from 'rocksdb';

declare global {
  // https://github.com/DefinitelyTyped/DefinitelyTyped/issues/40366
  interface PromiseConstructor {
    /* eslint-disable-next-line */
    allSettled(promises: Array<Promise<any>>): Promise<Array<{status: 'fulfilled' | 'rejected'; value?: any; reason?: any}>>;
  }

  /* eslint-disable-next-line @typescript-eslint/no-namespace */
  namespace NodeJS {
    interface ProcessEnv {
      NODE_ENV: 'development' | 'production';
      FIRESTASH_PAGINATION: number | undefined;
    }
  }
}

const numId = (id: string) => [...crypto.createHash('md5').update(id).digest().values()].reduce((a, b) => a + b);

const PAGINATION = 20000;
function pageSize(): number {
  return process.env.FIRESTASH_PAGINATION || PAGINATION;
}

/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
export const debounce = <F extends (...args: any) => any>(func: F, timeout: number) => {
  let ref: NodeJS.Timeout;
  let promise: Promise<ReturnType<F>> | undefined;
  let finish: (val: ReturnType<F>) => void;
  return function(this: ThisParameterType<F>, ...args: Parameters<F>): Promise<ReturnType<F>> {
    promise = promise || new Promise((resolve) => finish = resolve);
    clearTimeout(ref);
    ref = setTimeout(() => (promise = undefined) || finish(func.apply(this, args)), timeout);
    return promise;
  };
};

export interface IFireStash<T = number> {
  collection: string;
  cache: Record<string, T>;
}

export default class FireStash extends EventEmitter {
  toUpdate: Map<string, Set<string>> = new Map();
  firebase: typeof Firebase;
  app: Firebase.app.App;
  db: Firebase.firestore.Firestore;
  dir: string | null;
  log: typeof console;
  watchers: Map<string, () => void> = new Map();
  remote: Map<string, Record<string, IFireStash>> = new Map();
  timeout: NodeJS.Timeout | null = null;
  timeoutPromise: Promise<void> = Promise.resolve();
  level: LevelUp | null = null;

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
      this.level = levelup(rocksdb(path.join(this.dir, '.firestash')));
    }
  }

  /**
   * Resolves when all previously called updates are written to remote. Like requestAnimationFrame for the collection cache.
   */
  allSettled() { return this.timeoutPromise; }
  private cacheKey(collection: string, page: number) { return `firestash/${collection}-${page}`; }

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

    try {
      for (const [ collection, keys ] of this.toUpdate.entries()) {
        // Ensure we are watching for remote updates.
        if (!this.watchers.has(collection)) { await this.watch(collection); }

        // Get / ensure we have the remote cache object present. Calculate the pagination.
        const remote = this.remote.get(collection) || { [this.cacheKey(collection, 0)]: { collection, cache: {} } };
        this.remote.set(collection, remote);
        let pageCount = Object.keys(remote).length;
        let documentCount = 0;
        for (const dat of Object.values(remote)) {
          documentCount += Object.keys(dat.cache).length;
        }

        // For each key queued, set the cache object.
        for (const key of keys.values()) {
          // Increment our document count to trigger page overflows in batched writes.
          documentCount += 1;

          // Expand the cache object pages to fit the page count balance.
          while (pageCount < Math.ceil(documentCount / pageSize())) {
            const pageName = this.cacheKey(collection, pageCount);
            await this.db.doc(pageName).set({ collection, cache: {} }, { merge: true });
            remote[pageName] = { collection, cache: {} };
            pageCount += 1;
          }

          // Calculate the page we're adding to. In re-balance this is a md5 hash as decimal, mod page size, but on insert we just append to the last page present.
          let page = pageCount - 1;

          // Check to make sure this key doesn't already exist on remote. If it does, use its existing page.
          // TODO: Perf? This is potentially a lot of looping.
          const hasPageNum = numId(key) % pageCount;
          if (remote[this.cacheKey(collection, hasPageNum)].cache[key]) {
            page = hasPageNum;
          }
          else {
            let i = 0;
            for (const dat of Object.values(remote)) {
              if (dat.cache[key]) { page = i; break; }
              i++;
            }
          }

          // Get our final cache page destination.
          const pageName = this.cacheKey(collection, page);

          // If this page does not exist in our update yet, add one extra write to our count for ensuring the collection name.
          if (!updates[pageName]) { count++; }

          // Update remote.
          const update: IFireStash<FirebaseFirestore.FieldValue> = updates[pageName] = updates[pageName] || { collection, cache: {} };
          update.cache[key] = FieldValue.increment(1);

          // Keep local copy of remote stash in sync.
          remote[pageName] = remote[pageName] || { collection, cache: {} };
          remote[pageName] && (remote[pageName].cache[key] = (remote[pageName]?.cache[key] || 0) + 1);

          // Optimistically remove this key
          keys.delete(key);

          // If we've hit the 500 write limit, batch write these objects.
          if (count++ >= 498) {
            const batch = this.db.batch();
            for (const pageName of Object.keys(updates)) {
              batch.set(this.db.doc(pageName), updates[pageName], { merge: true });
            }
            promises.push(batch.commit());
            this.emit('save');
            updates = {};
            count = 0;
          }
        }
        // Remove collection from toUpdate when all updated.
        this.toUpdate.delete(collection);
      }

      // Batch write the changes.
      const batch = this.db.batch();
      for (const pageName of Object.keys(updates)) {
        batch.set(this.db.doc(pageName), updates[pageName], { merge: true });
      }
      promises.push(batch.commit());
      this.emit('save');

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
  private async mergeRemote(collection: string, data: Record<string, IFireStash>) {
    this.remote.set(collection, data);
    const modified = [];
    if (this.level) {
      let local: Record<string, IFireStash> = {};
      const batch = this.level.batch();
      try { local = JSON.parse(await this.level.get(collection, { asBuffer: false })); }
      catch (_err) {}
      /* eslint-disable-next-line */
      const promises: Promise<any>[] = [];
      for (const [ page, stash ] of Object.entries(data)) {
        local[page] = local[page] || { collection, cache: {} };
        for (const [ id, value ] of Object.entries(stash.cache)) {
          if (local[page].cache[id] === value) { continue; }
          modified.push(id);
          promises.push(this.db.doc(`${collection}/${id}`).get().then((doc) => {
            local[page].cache[id] = value;
            return batch.put(`${collection}/${id}`, JSON.stringify(doc.data() || {}));
          }));
        }
      }
      batch.put(collection, JSON.stringify(data));
      await Promise.allSettled(promises);
      await batch.write();
    }
    const updated = this.get(collection);
    for (const id of modified) {
      this.emit(`${collection}/${id}`, updated[id]);
    }
    this.emit(collection, updated);
  }

  /**
   * Watch for updates from a collection stash.
   * @param collection Collection Path
   */
  async watch(collection: string): Promise<() => void> {
    return this.watchers.get(collection) || new Promise((resolve, reject) => {
      let firstCall: boolean | void = true;
      const unsubscribe = this.db.collection('firestash').where('collection', '==', collection).onSnapshot(debounce((update) => {
        const data: Record<string, IFireStash> = { [this.cacheKey(collection, 0)]: { collection, cache: {} } };
        for (const dat of update.docs) {
          data[`firestash/${dat.id}`] = dat.data() as IFireStash;
        }
        this.mergeRemote(collection, data);
        firstCall && (firstCall = resolve(unsubscribe));
      }, 300), reject);
      this.watchers.set(collection, unsubscribe);
    });
  }

  /**
   * Finish the last update and disconnect all watchers and timeouts.
   */
  async unwatch() {
    for (const unsubscribe of this.watchers.values()) {
      unsubscribe();
    }
    await this.allSettled();
    this.timeout && clearTimeout(this.timeout);
    this.timeout = null;
    this.remote = new Map();
  }

  /**
   * Get the entire stash cache for a collection.
   * @param collection Collection Path
   */
  async stash(collection: string): Promise<IFireStash> {
    if (!this.watchers.has(collection)) { await this.watch(collection); }
    const out: IFireStash = { collection, cache: {} };
    for (const dat of Object.values(this.remote.get(collection) || {})) {
      Object.assign(out.cache, dat.cache);
    }
    return out;
  }

  /**
   * Get the entire stash cache for a collection.
   * @param collection Collection Path
   */
  /* eslint-disable no-dupe-class-members */
  async get<T=object>(collection: string): Promise<Record<string, T | undefined>>;
  async get<T=object>(collection: string, id: string): Promise<T | null>;
  async get<T=object>(collection: string, id?: string): Promise<Record<string, T | undefined> | T | null> {
  /* eslint-enable no-dupe-class-members */
    if (id) {
      try {
        return JSON.parse(await this.level?.get(`${collection}/${id}`, { asBuffer: false })) as T;
      }
      catch (_err) {
        return null;
      }
    }
    const stash = await this.stash(collection);
    const out: Record<string, T> = {};
    for (const key of Object.keys(stash.cache)) {
      const record = await this.get<T>(collection, key);
      if (!record) { continue; }
      out[key] = record;
    }
    return out;
  }

  /**
   * Increment a single document's generation cache key. Syncs to remote once per second.
   * @param collection Collection Path
   */
  async update(collection: string, key: string) {
    const keys = this.toUpdate.get(collection) || new Set();
    this.toUpdate.set(collection, keys);
    keys.add(key);
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
    if (!this.watchers.has(collection)) { await this.watch(collection); }
    const stash = this.remote.get(collection) || {};
    for (const pageId of Object.keys(stash)) {
      const page = stash[pageId];
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
    if (!this.watchers.has(collection)) { await this.watch(collection); }
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

    const data = this.remote.get(collection);
    if (!data) { return; }
    let count = 0;
    for (const dat of Object.values(data)) {
      count += Object.keys(dat.cache).length;
    }
    const pageCount = Math.ceil(count / pageSize());
    const updates: Record<string, IFireStash<FirebaseFirestore.FieldValue | number>> = {};
    const remote = this.remote.get(collection) || { [this.cacheKey(collection, 0)]: { collection, cache: {} } };
    this.remote.set(collection, remote);

    for (let i = 0; i < pageCount; i++) {
      const pageName = this.cacheKey(collection, i);
      updates[pageName] = { collection, cache: {} };
      remote[pageName] = remote[pageName] || { collection, cache: {} };
    }

    for (const [ id, dat ] of Object.entries(data)) {
      for (const [ key, value ] of Object.entries(dat.cache)) {
        const page = numId(key) % pageCount;
        const pageId = this.cacheKey(collection, page);
        if (pageId === id) { continue; }

        updates[pageId].cache[key] = value;
        remote[pageId].cache[key] = value;

        updates[id].cache[key] = FieldValue.delete();
        delete remote[id].cache[key];

        count += 2;
        if (count >= (498 - (pageCount * 2))) {
          const batch = this.db.batch();
          for (const [ id, page ] of Object.entries(updates)) {
            batch.set(this.db.doc(id), page, { merge: true });
            updates[id] = { collection, cache: {} };
          }
          promises.push(batch.commit());
          count = 0;
        }
      }
    }
    const batch = this.db.batch();
    for (const [ id, page ] of Object.entries(updates)) {
      batch.set(this.db.doc(id), page, { merge: true });
      updates[id] = { collection, cache: {} };
    }
    promises.push(batch.commit());
    this.emit('balance', collection);
    promises.length && await Promise.allSettled(promises);
  }
}
