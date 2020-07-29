import Firebase from 'firebase-admin';
import { EventEmitter } from 'events';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import { clearTimeout } from 'timers';

const numId = (id: string) => [...crypto.createHash('md5').update(id).digest().values()].reduce((a, b) => a + b);

declare global {
  // https://github.com/DefinitelyTyped/DefinitelyTyped/issues/40366
  interface PromiseConstructor {
    /* eslint-disable-next-line */
    allSettled(promises: Array<Promise<any>>): Promise<Array<{status: 'fulfilled' | 'rejected'; value?: any; reason?: any}>>;
  }

  /* eslint-disable-next-line @typescript-eslint/no-namespace */
  namespace NodeJS {
    interface ProcessEnv {
      GOOGLE_APPLICATION_CREDENTIALS: string;
    }
  }
}

export interface FireStash<T = number> {
  collection: string;
  cache: Record<string, T>;
}

export default class FireCache extends EventEmitter {
  toUpdate: Map<string, Set<string>> = new Map();
  firebase: typeof Firebase;
  app: Firebase.app.App;
  db: Firebase.firestore.Firestore;
  dir: string | null;
  log: typeof console;
  watchers: Map<string, () => void> = new Map();
  local: Map<string, Record<string, FireStash>> = new Map();
  remote: Map<string, Record<string, FireStash>> = new Map();
  timeout: NodeJS.Timeout | null = null;
  timeoutPromise: Promise<void> = Promise.resolve();

  constructor(firebase: typeof Firebase, app: Firebase.app.App, directory: string | null = null) {
    super();
    this.firebase = firebase;
    this.app = app;
    this.db = this.app.firestore();
    this.dir = directory;
    this.log = console;
  }

  private cacheKey(collection: string, page: number) { return `firestash/${collection.replace(/\//g, '.')}-${page}`; }

  private async saveCacheUpdate() {
    this.timeout = null;
    const FieldValue = this.firebase.firestore.FieldValue;
    /* eslint-disable-next-line */
    const promises: Promise<any>[] = [];
    let count = 0;
    let updates = {};

    try {
      for (const [ collectionId, keys ] of this.toUpdate.entries()) {
        // Calculate our collection cache key
        const collection = collectionId.replace(/\//g, '.');

        // Ensure we are watching for remote updates.
        if (!this.watchers.has(collectionId)) { await this.watch(collectionId); }

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
          while (pageCount < Math.ceil(documentCount / 20000)) {
            const pageName = this.cacheKey(collection, pageCount);
            await this.db.doc(pageName).set({ collection, cache: {} }, { merge: true });
            remote[pageName] = { collection, cache: {} };
            pageCount += 1;
          }

          // Calculate the page we're adding to. In re-balance this is a md5 hash as decimal, mod page size, but on insert we just append to the last page present.
          const page = pageCount - 1;
          const pageName = this.cacheKey(collection, page);

          // If this page does not exist in our update yet, add one extra write to our count for ensuring the collection name.
          if (!updates[pageName]) { count++; }

          // Update remote.
          const update: FireStash<FirebaseFirestore.FieldValue> = updates[pageName] = updates[pageName] || { collection, cache: {} };
          update.cache[key] = FieldValue.increment(1);

          // Keep local in sync.
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
        this.toUpdate.delete(collectionId);
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

  async rebalance() {
    const FieldValue = this.firebase.firestore.FieldValue;

    /* eslint-disable-next-line */
    const promises: Promise<any>[] = [];

    for (const collection of this.remote.keys()) {
      const data = this.remote.get(collection);
      if (!data) { continue; }
      let count = 0;
      for (const dat of Object.values(data)) {
        count += Object.keys(dat.cache).length;
      }
      const pageCount = Math.ceil(count / 20000);
      const updates: Record<string, FireStash<FirebaseFirestore.FieldValue | number>> = {};
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
    }
    this.emit('re-balance');
    promises.length && await Promise.allSettled(promises);
  }

  mergeRemote(collection: string, data: Record<string, FireStash>) {
    this.remote.set(collection, data);
    this.emit(collection, this.get(collection));
    if (this.dir) {
      const dir = path.join(this.dir, collection);
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, '.firecache'), JSON.stringify(data));
    }
  }

  async watch(collection: string): Promise<() => void> {
    return this.watchers.get(collection) || new Promise((resolve, reject) => {
      let firstCall: boolean | void = true;
      const unsubscribe = this.db.collection('firestash').where('collection', '==', collection).onSnapshot((update) => {
        const data: Record<string, FireStash> = { [this.cacheKey(collection, 0)]: { collection, cache: {} } };
        for (const dat of update.docs) {
          data[`firestash/${dat.id}`] = dat.data() as FireStash;
        }
        this.mergeRemote(collection, data);
        firstCall && (firstCall = resolve(unsubscribe));
      }, reject);
      this.watchers.set(collection, unsubscribe);
    });
  }

  async unwatch() {
    await this.allSettled();
    for (const unsubscribe of this.watchers.values()) {
      unsubscribe();
    }
    this.remote = new Map();
    this.timeout && clearTimeout(this.timeout);
    this.timeout = null;
  }

  allSettled() { return this.timeoutPromise; }

  async get(collection: string) {
    if (!this.watchers.has(collection)) { await this.watch(collection); }
    const out: FireStash = { collection, cache: {} };
    for (const dat of Object.values(this.remote.get(collection) || {})) {
      Object.assign(out.cache, dat.cache);
    }
    return out;
  }

  async update(collection: string, key: string) {
    const keys = this.toUpdate.get(collection) || new Set();
    this.toUpdate.set(collection, keys);
    keys.add(key);
    if (this.timeout) { return this.timeoutPromise; }
    return this.timeoutPromise = new Promise((resolve, reject) => {
      this.timeout = setTimeout(() => this.saveCacheUpdate().then(resolve, reject), 1000);
    });
  }
}
