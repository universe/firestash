import Firebase from 'firebase-admin';
import { EventEmitter } from 'events';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';

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

  private async saveCacheUpdate() {
    this.timeout = null;
    const FieldValue = this.firebase.firestore.FieldValue;
    /* eslint-disable-next-line */
    const promises: Promise<any>[] = [];
    let count = 0;
    let updates = {};

    try {
      for (const [ collectionId, keys ] of this.toUpdate.entries()) {
        if (!this.watchers.has(collectionId)) { await this.watch(collectionId); }
        const collection = collectionId.replace(/\//g, '.');
        const remote = this.remote.get(collection) || { [`firestash/${collection}-0`]: { collection, cache: {} } };
        this.remote.set(collection, remote);
        const pageCount = Object.keys(remote).length;
        for (const key of keys.values()) {
          const page = numId(key) % pageCount;
          const pageName = `firestash/${collection}-${page}`;
          if (!updates[pageName]) { count++; }
          const update: FireStash<FirebaseFirestore.FieldValue> = updates[pageName] = updates[pageName] || { collection, cache: {} };
          update.cache[key] = FieldValue.increment(1);
          remote[pageName] = remote[pageName] || { collection, cache: {} };
          remote[pageName] && (remote[pageName].cache[key] = (remote[pageName]?.cache[key] || 0) + 1);
          keys.delete(key);
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
        this.toUpdate.delete(collectionId);
      }

      const batch = this.db.batch();
      for (const pageName of Object.keys(updates)) {
        batch.set(this.db.doc(pageName), updates[pageName], { merge: true });
      }
      promises.push(batch.commit());
      this.emit('save');

      await Promise.allSettled(promises);
      await this.rebalance();
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
      const remote = this.remote.get(collection) || { [`firestash/${collection}-0`]: { collection, cache: {} } };
      this.remote.set(collection, remote);

      if (pageCount <= Object.keys(data).length) { continue; }

      for (let i = 0; i < pageCount; i++) {
        const pageName = `firestash/${collection}-${i}`;
        updates[pageName] = { collection, cache: {} };
        remote[pageName] = remote[pageName] || { collection, cache: {} };
      }

      for (const [ id, dat ] of Object.entries(data)) {
        for (const [ key, value ] of Object.entries(dat.cache)) {
          const page = numId(key) % pageCount;
          const pageId = `firestash/${collection}-${page}`;
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
        const data: Record<string, FireStash> = { [`firestash/${collection}-0`]: { collection, cache: {} } };
        for (const dat of update.docs) {
          data[`firestash/${dat.id}`] = dat.data() as FireStash;
        }
        this.mergeRemote(collection, data);
        firstCall && (firstCall = resolve(unsubscribe));
      }, reject);
      this.watchers.set(collection, unsubscribe);
    });
  }

  unwatch() {
    for (const unsubscribe of this.watchers.values()) {
      unsubscribe();
    }
  }

  allSettled() {
    return this.timeoutPromise;
  }

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
