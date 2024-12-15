import 'dotenv/config';

import { describe, before, beforeEach, after, it } from 'mocha';
import * as assert from 'assert';
import * as path from 'path';
import  { initializeTestEnvironment } from '@firebase/rules-unit-testing';
import {
  collection,
  deleteDoc,
  doc,
  getDoc,
  getDocs,
  query,
  setDoc,
  where,
  writeBatch, 
} from 'firebase/firestore';
import { performance } from 'perf_hooks';
import { fileURLToPath } from 'url';
import { initializeApp, deleteApp } from 'firebase-admin/app';
import { getAuth } from 'firebase-admin/auth';

import FireStash, { cacheKey } from '../src/index.js';

const projectId = 'fire-stash';
const __dirname = fileURLToPath(new URL('.', import.meta.url));
const CONFIG = {
  projectId,
  apiKey: "TEST_API_KEY",
  appId: "1:testing",
  authDomain: `${projectId}.firebaseapp.com`,
  databaseURL: `https://${projectId}.firebaseio.com`,
  measurementId: "G-TEST",
  messagingSenderId: "1234567890",
  storageBucket: `${projectId}.appspot.com`,
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Connector', function() {
  describe('it should', function() {
    const fireTest = initializeTestEnvironment({ projectId });
    let appId = 0;
    let fireStash: FireStash;
    let customToken: string;

    before(async function() {
      const adminApp = initializeApp(CONFIG, 'firecache-tests');
      const adminAuth = getAuth(adminApp);
      await adminAuth.createUser({ uid: 'test-firecache-user', email: 'test-firecache-user@universe.test', emailVerified: true });
      customToken = await adminAuth.createCustomToken('test-firecache-user');
      await deleteApp(adminApp);
    });

    beforeEach(async function() {
      this.timeout(60000);
      try {
        await fireStash?.stop();
        await (await fireTest).clearFirestore();
        fireStash = new FireStash(CONFIG, {
          directory: path.join(__dirname, String(appId++)),
          customToken,
          worker: true,
        });
        await wait(1000);
      } catch (err) {
        console.error("Error running beforeEach. Retrying.", err);
        await (await fireTest).clearFirestore();
        fireStash = new FireStash(CONFIG, {
          directory: path.join(__dirname, String(appId++)),
          customToken,
          worker: true,
        });
        await wait(1000);
      }
    });

    after(async() => {
      try {
        await fireStash.stop();
        await (await fireTest).clearFirestore();
      } catch (err) {
        console.error('Error running afterAll', err);
      }
    });

    // This must come first because the emulators slow down with some of our other
    // stress tests, and will fail if it comes too late in the test suite.
    it.skip('massive operations run in low memory mode', async function() {
      try {
        this.timeout(300000);

        await fireStash.stop();
        fireStash = new FireStash(CONFIG, {
          directory: path.join(__dirname, String(appId++)),
          lowMem: true,
          customToken,
          worker: true,
        });

        const start = Date.now();
        const cache: Record<string, number> = {};
        const objects: Record<string, {id: number}> = {};
        let batch = writeBatch(fireStash.db);
        for (let i = 0; i < 15000; i++) {
          fireStash.update('collection2', `id${i}`);
          // fireStash.update(`collection2/id${i}/sub-page`, String(i));
          // fireStash.update(`irrelevent/id${i}/sub-page`, String(i));
          batch.set(doc(fireStash.db, `collection2/id${i}`), { id: i });
          cache[`id${i}`] = 1;
          objects[`collection2/id${i}`] = { id: i };
          if (i % 500 === 0) {
            console.log('WRITE', i, '/', 15000)
            await batch.commit();
            batch = writeBatch(fireStash.db);
          }
        }
        await batch.commit();
        console.log('commit');

        await fireStash.allSettled();
        console.log('alldone');

        const waypoint = Date.now();
        console.log('WRITTEN', waypoint - start, Object.keys((await fireStash.stash('collection2')).cache).length);
        assert.deepStrictEqual(Object.keys((await fireStash.stash('collection2')).cache).length, 15000, 'Writes an obscene amount of data.');

        const res = await fireStash.get('collection2');
        console.log('READ', Date.now() - waypoint, Object.keys(res).filter(Boolean).length);

        assert.deepStrictEqual(Object.keys(res).filter(Boolean).length, 15000, 'Fetches an obscene amount of data keys.');
        assert.deepStrictEqual(Object.values(res).filter(Boolean).length, 15000, 'Fetches an obscene amount of data values.');

        const dat = await getDocs(query(collection(fireStash.db, 'firestash'), where('collection', '==', 'collection2')));
        assert.deepStrictEqual(dat.docs.length, 1, '15,000 keys and below stay in a single page.');
        fireStash.update('collection2', `id${15000}`);
        await fireStash.allSettled();

        let dat2 = await await getDocs(query(collection(fireStash.db, 'firestash'), where('collection', '==', 'collection2')));
        assert.deepStrictEqual(dat2.docs.length, 2, 'Shards above 15,000 keys');

        let page0Count = Object.keys(dat2.docs[0]?.data()?.cache || {}).length;
        let page1Count = Object.keys(dat2.docs[1]?.data()?.cache || {}).length;
        assert.ok(page0Count === 15000, 'Initial cache overflows are simply append only.');
        assert.ok(page1Count === 1, 'Initial cache overflows are simply append only.');

        await fireStash.balance('collection2');
        dat2 = await getDocs(query(collection(fireStash.db, 'firestash'), where('collection', '==', 'collection2')));
        page0Count = Object.keys(dat2.docs[0]?.data()?.cache || {}).length;
        page1Count = Object.keys(dat2.docs[1]?.data()?.cache || {}).length;
        assert.ok((Math.abs(page0Count - page1Count) / 15000) * 100 < 3, 'Pages re-balance with less than 3% error.');
      }
      catch (err) { return err; }
    });

    it('is able to insert a key', async function() {
      this.timeout(3000);
      fireStash.update('contacts', 'id1');
      const start = Date.now();

      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: {} }, 'Throttles cache write and writes');

      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache write and writes');
      assert.deepStrictEqual(
        (await getDoc(doc(fireStash.db, `firestash/${cacheKey('contacts', 0)}`))).data(),
        { collection: 'contacts', cache: { id1: 1 } },
        'Throttles cache writes',
      );
      assert.ok((Date.now() - start) > 120, 'Resolves in ~100ms');
    });

    it('is able to purge collections', async function() {
      fireStash.update('purge', 'id1');
      fireStash.update('purge', 'id2');
      fireStash.update('purge', 'id3');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('purge'), { collection: 'purge', cache: { id1: 1, id2: 1, id3: 1 } }, 'Creates collection');
      await fireStash.purge('purge');
      assert.deepStrictEqual(await fireStash.stash('purge'), { collection: 'purge', cache: {} }, 'Purges collection');
    });

    it('creates in-memory local db if started with no root directory', async function() {
      const memStash = new FireStash(CONFIG, {
        customToken,
        worker: true,
      });
      await fireStash.allSettled();
      memStash.update('contacts', 'id1');
      await memStash.allSettled();
      assert.deepStrictEqual(await memStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache writes, resolved in 1s');
      await memStash.stop();
    });

    it('getting a non-existent key does not fetch from remote', async function() {
      const fetches: string[] = [];
      fireStash.on('fetch', (collection, id) => fetches.push(`${collection}/${id}`));
      assert.deepStrictEqual((await fireStash.watchers()).length, 0, 'No watchers by default.');
      assert.deepStrictEqual(await fireStash.get('tests', 'doc'), null, 'No document.');
      assert.deepStrictEqual((await fireStash.watchers()).length, 1, 'One watcher started after read.');
      assert.deepStrictEqual(fetches, [], 'Makes no fetch requests.');

      await fireStash.unwatch('tests');
      assert.deepStrictEqual(await fireStash.get('tests', 'doc'), null, 'No document.');
      assert.deepStrictEqual((await fireStash.watchers()).length, 1, 'One watcher started after read.');
      assert.deepStrictEqual(fetches, [], 'Makes no fetch requests.');
    });

    it('only calling update does start a watcher', async function() {
      fireStash.update('contacts', 'id1');
      await fireStash.allSettled();
      assert.deepStrictEqual((await fireStash.watchers()).length, 1, 'Watchers started.');
    });

    it('is able to insert a key for sub collection', async function() {
      fireStash.update('contacts/adam/notes', 'note1');
      assert.deepStrictEqual(await fireStash.stash('contacts/adam/notes'), { collection: 'contacts/adam/notes', cache: {} }, 'Throttles cache writes');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('contacts/adam/notes'), { collection: 'contacts/adam/notes', cache: { note1: 1 } }, 'Throttles cache writes, resolved in ~1s');
    });

    it('is able to update a key with an object to cache', async function() {
      const fetches: string[] = [];
      fireStash.on('fetch', (collection, id) => {
        fetches.push(`${collection}/${id}`);
      });
      fireStash.update('contacts', '1', { foo: 'bar' });
      setDoc(doc(fireStash.db, 'contacts/2'), { biz: 'baz' });
      fireStash.update('contacts', '2');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { 1: 1, 2: 1 } }, 'Stash correctly set');
      assert.deepStrictEqual(await fireStash.get('contacts'), { 1: { foo: 'bar' }, 2: { biz: 'baz' } }, 'Gets all data');
      assert.deepStrictEqual(fetches, ['contacts/1', 'contacts/2'], 'Fetches only what is necessary');
    });

    it('fetches only happen once', async function() {
      const fetches: string[] = [];
      let called = 0;
      fireStash.update('contacts', '1', { foo: 'bar' });
      setDoc(doc(fireStash.db, 'contacts/2'), { biz: 'baz' });
      fireStash.update('contacts', '2');
      await fireStash.allSettled();

      const otherFireStash = new FireStash(CONFIG, {
        directory: path.join(__dirname, String(appId + '-other')),
        customToken,
        worker: true,
      });
      await otherFireStash.allSettled();
      otherFireStash.on('fetch', (collection, id) => {
        fetches.push(`${collection}/${id}`);
        called++;
      });
      await otherFireStash.watch('contacts');
      assert.deepStrictEqual(await otherFireStash.stash('contacts'), { collection: 'contacts', cache: { 1: 1, 2: 1 } }, 'Stash correctly set');
      for (let i = 0; i < 10; i++) {
        // Intentional no await, testing request throttling.
        otherFireStash.get('contacts');
      }
      assert.deepStrictEqual(await otherFireStash.get('contacts'), { 1: { foo: 'bar' }, 2: { biz: 'baz' } }, 'Gets all data');
      await otherFireStash.allSettled();
      assert.deepStrictEqual(called, 2, 'Only called once per doc');
      assert.deepStrictEqual(fetches.sort(), [ 'contacts/1', 'contacts/2' ].sort(), 'Fetches only what is necessary');
      await otherFireStash.stop();
    });

    it('is able to update a key set to undefined', async function() {
      this.timeout(4000);
      await fireStash.update('contacts', '3', { foo: 'bar' });
      assert.deepStrictEqual(await fireStash.get('contacts', '3'), { foo: 'bar' });
      assert.deepStrictEqual((await getDoc(doc(fireStash.db, 'contacts/3'))).data(), { foo: 'bar' });
      await fireStash.update('contacts', '3', { foo: undefined });
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.get('contacts', '3'), { });
      assert.deepStrictEqual((await getDoc(doc(fireStash.db, 'contacts/3'))).data(), { });
    });

    it('handles multiple updates to the same key and object', async function() {
      const fetches: string[] = [];
      fireStash.on('fetch', (collection, id) => {
        fetches.push(`${collection}/${id}`);
      });
      fireStash.update('contacts', '1', { foo: 'bar', deep: { zip: 'zap' }, arr: [ 1, 2 ] });
      fireStash.update('contacts', '1', { biz: 'baz', deep: { zoop: 'zop' }, arr: [ 3, 4 ] });
      const expected = {
        foo: 'bar',
        biz: 'baz',
        deep: {
          zip: 'zap',
          zoop: 'zop',
        },
        arr: [ 3, 4 ],
      };

      await fireStash.allSettled();
      setTimeout(async() => {
        assert.deepStrictEqual(await fireStash.get('contacts', '1'), expected, 'Sets all data locally immediately');
        assert.deepStrictEqual(fetches, ["contacts/1"], 'Fetches only what is necessary after local fire.');
      }, 10);

      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.get('contacts', '1'), expected, 'Sets all data locally after sync');
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { 1: 1 } }, 'Stash correctly set');
      assert.deepStrictEqual((await getDoc(doc(fireStash.db, 'contacts/1'))).data(), expected, 'Gets all data on remote');
      assert.deepStrictEqual(fetches, ["contacts/1"], 'Fetches only what is necessary after remote update.');
    });

    it('is able to delete an object', async function() {
      this.timeout(4000);

      fireStash.update('contacts', '1', { foo: 'bar' });
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { 1: 1 } }, 'Stash correctly set');
      assert.deepStrictEqual(await fireStash.get('contacts'), { 1: { foo: 'bar' } }, 'Gets all data');
      fireStash.delete('contacts', '1');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { 1: 2 } }, 'Stash correctly set');
      assert.deepStrictEqual(await fireStash.get('contacts'), { 1: null }, 'Gets all data');
      assert.deepStrictEqual(await fireStash.get('contacts', '1'), null, 'Missing objects return null.');
      assert.deepStrictEqual((await getDoc(doc(fireStash.db, 'contacts/1'))).data(), undefined);
    });

    it('is able to delete then reify an object', async function() {
      this.timeout(4000);

      fireStash.update('contacts', '1', { foo: 'bar' });
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { 1: 1 } }, 'Stash correctly set');
      assert.deepStrictEqual(await fireStash.get('contacts'), { 1: { foo: 'bar' } }, 'Gets all data');
      fireStash.delete('contacts', '1');
      fireStash.update('contacts', '1', { foo: 'baz' });
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { 1: 2 } }, 'Stash correctly set');
      assert.deepStrictEqual(await fireStash.get('contacts'), { 1: { foo: 'baz' } }, 'Gets all data');
      assert.deepStrictEqual(await fireStash.get('contacts', '1'), { foo: 'baz' }, 'Missing objects return null.');
      assert.deepStrictEqual((await getDoc(doc(fireStash.db, 'contacts/1'))).data(), { foo: 'baz' });
    });

    it('is able to watch an object for deletion', async function() {
      this.timeout(50000);
      fireStash.update('contacts', '1', { foo: 'bar' });
      await fireStash.allSettled();
      fireStash.watch('contacts');
      const cacheKey = fireStash.cacheKey('contacts', 0);
      const cache = (await getDoc(doc(fireStash.db, 'firestash', cacheKey))).data() as { cache: Record<string, number> };
      cache.cache['1'] = (cache.cache['1'] || 0) + 1;
      await deleteDoc(doc(fireStash.db, 'contacts', '1'));
      await setDoc(doc(fireStash.db, 'firestash', cacheKey), cache);
      await wait(1000);
      assert.deepStrictEqual(await fireStash.get('contacts', '1'), null, 'Missing objects return null.');
    });

    it('one update with no content will force sync with remote', async function() {
      this.timeout(6000);
      const fetches: string[] = [];
      let objUpdates = 0;
      let collectionUpdates: string[] | null = null;
      fireStash.on('fetch', (collection, id) => {
        fetches.push(`${collection}/${id}`);
      });

      fireStash.on('contacts/1', () => objUpdates++);
      fireStash.on('contacts', (_, update) => collectionUpdates = update);

      fireStash.update('contacts', '1', { foo: 'bar', deep: { zip: 'zap' }, arr: [ 1, 2 ] });
      fireStash.update('contacts', '1', { biz: 'baz', deep: { zoop: 'zop' }, arr: [ 3, 4 ] });
      fireStash.update('contacts', '1');

      let ran = 0;
      await fireStash.allSettled();
      setTimeout(async() => {
        ran = 1;
        assert.strictEqual(objUpdates, 1, 'Object events triggered, de-duped , and occur immediately');
        assert.deepStrictEqual(collectionUpdates, ['1'], 'Collection events triggered, de-duped , and occur immediately');
      }, 10);

      const expected = {
        foo: 'bar',
        biz: 'baz',
        deep: {
          zip: 'zap',
          zoop: 'zop',
        },
        arr: [ 3, 4 ],
      };

      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { 1: 1 } }, 'Stash correctly set');
      assert.deepStrictEqual(await fireStash.get('contacts', '1'), expected, 'Sets all data locally');
      assert.deepStrictEqual((await getDoc(doc(fireStash.db, 'contacts/1'))).data(), expected, 'Gets all data on remote');
      assert.deepStrictEqual(fetches, ['contacts/1'], 'Fetches only what is necessary');
      assert.strictEqual(1, ran, 'Ran events tests');
    });

    it('is able to update deep collection keys with an object to cache', async function() {
      this.timeout(3000);
      const fetches: string[] = [];
      fireStash.on('fetch', (collection, id) => {
        fetches.push(`${collection}/${id}`);
      });
      fireStash.update('contacts/1/phones', '1234567890', { foo: 'bar' });
      setDoc(doc(fireStash.db, 'contacts/1/phones/0987654321'), { biz: 'baz' });
      fireStash.update('contacts/1/phones', '0987654321');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.get('contacts/1/phones'), { 1234567890: { foo: 'bar' }, '0987654321': { biz: 'baz' } }, 'Gets all data');
      assert.deepStrictEqual(fetches, ['contacts/1/phones/0987654321', 'contacts/1/phones/1234567890'], 'Fetches only what is necessary');
    });

    it('batches multiple key updates in the same collection', async function() {
      fireStash.update('contacts2', 'id1');
      fireStash.update('contacts2', 'id1');
      fireStash.update('contacts2', 'id2');
      assert.deepStrictEqual(await fireStash.stash('contacts2'), { collection: 'contacts2', cache: {} }, 'Throttles cache writes');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('contacts2'), { collection: 'contacts2', cache: { id1: 1, id2: 1 } }, 'Throttled cache bundles writes, resolved in ~1s');
    });

    it('batches multiple key updates in the same collection for sub collection', async function() {
      fireStash.update('contacts2/adam/notes', 'id1');
      fireStash.update('contacts2/adam/notes', 'id1');
      fireStash.update('contacts2/adam/notes', 'id2');
      assert.deepStrictEqual(await fireStash.stash('contacts2/adam/notes'), { collection: 'contacts2/adam/notes', cache: {} }, 'Throttles cache writes');
      await fireStash.allSettled();
      assert.deepStrictEqual(
        await fireStash.stash('contacts2/adam/notes'),
        { collection: 'contacts2/adam/notes', cache: { id1: 1, id2: 1 } },
        'Throttled cache bundles writes, resolved in ~1s',
      );
    });

    it('is able to get a list of ids', async function() {
      fireStash.update('multi-get', 'id1', { foo: 1 });
      fireStash.update('multi-get', 'id2', { foo: 2 });
      fireStash.update('multi-get', 'id3', { foo: 3 });
      await fireStash.allSettled();
      assert.deepStrictEqual(
        await fireStash.get('multi-get', 'id1'),
        { foo: 1 },
        'Fetches single document.',
      );
      assert.deepStrictEqual(
        await fireStash.get('multi-get', [ 'id1', 'id3' ]),
        { id1: { foo: 1 }, id3: { foo: 3 } },
        'Fetches multiple documents.',
      );
      assert.deepStrictEqual(
        await fireStash.get('multi-get'),
        { id1: { foo: 1 }, id2: { foo: 2 }, id3: { foo: 3 } },
        'Fetches entire collection.',
      );
    });

    it('batches large key updates within one collection', async function() {
      this.timeout(10000);
      let i = 0;
      fireStash.on('save', () => { i++; });

      // Contrived to show that we batch document updates in groups of 10, including collection names – the worst case size limit for Firestore.batch().
      const cache: Record<string, number> = {};
      for (let i = 0; i < 1000; i++) {
        fireStash.update('collection', `id${i}`, { i });
        cache[`id${i}`] = 1;
      }
      assert.deepStrictEqual(await fireStash.stash('collection'), { collection: 'collection', cache: {} }, 'Throttles cache writes');

      assert.strictEqual(i, 0, 'Throttles large multi-collection writes in batches of 10');

      await fireStash.allSettled();

      assert.strictEqual(i, 101, 'Throttles large multi-collection writes in batches of 10'); // Batches of 10, plus cache page updates.
      assert.deepStrictEqual(await fireStash.stash('collection'), { collection: 'collection', cache }, 'Throttles cache writes');
    });

    it('shards massive caches within one collection', async function() {
      this.timeout(600000);
      const COUNT = 15000;

      const cache: Record<string, number> = {};
      const objects: Record<string, { id: number }> = {};
      const promises: Promise<void>[] = [];
      let batch = writeBatch(fireStash.db);
      for (let i = 0; i < COUNT; i++) {
        if (i % 100 === 0) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }
        fireStash.update('collection2', `id${i}`);
        batch.set(doc(fireStash.db, `collection2/id${i}`), { id: i });
        cache[`id${i}`] = 1;
        objects[`collection2/id${i}`] = { id: i };
        if (i % 500 === 0) {
          console.log('WRITE', i, '/', COUNT);
          promises.push(batch.commit());
          batch = writeBatch(fireStash.db);
        }
      }
      await batch.commit();
      console.log('WRITE', COUNT, '/', COUNT);
      await Promise.allSettled(promises);
      await fireStash.allSettled();
      console.log('All Settled');
      assert.deepStrictEqual(Object.keys((await fireStash.stash('collection2')).cache).length, COUNT, 'Writes an obscene amount of data.');

      const res = await fireStash.get('collection2');
      assert.deepStrictEqual(Object.keys(res).filter(Boolean).length, COUNT, 'Fetches an obscene amount of data keys.');
      assert.deepStrictEqual(Object.values(res).filter(Boolean).length, COUNT, 'Fetches an obscene amount of data values.');

      const dat = await getDocs(query(collection(fireStash.db, 'firestash'), where('collection', '==', 'collection2')));
      assert.deepStrictEqual(dat.docs.length, 1, '15,000 keys and below stay in a single page.');

      fireStash.update('collection2', `id${COUNT}`);
      await fireStash.allSettled();
      await new Promise(resolve => setTimeout(resolve, 3000));

      let dat2 = await getDocs(query(collection(fireStash.db, 'firestash'), where('collection', '==', 'collection2')));
      console.log('Page One', Object.keys(dat2?.docs?.[0]?.data()?.cache || {}).length);
      console.log('Page Two', Object.keys(dat2?.docs?.[1]?.data()?.cache || {}).length);
      assert.deepStrictEqual(dat2.docs.length, 2, 'Shards above 15,000 keys');

      let page0Count = Object.keys(dat2.docs[0]?.data()?.cache || {}).length;
      let page1Count = Object.keys(dat2.docs[1]?.data()?.cache || {}).length;
      assert.ok(page0Count === COUNT, 'Initial cache overflows are simply append only.');
      assert.ok(page1Count === 1, 'Initial cache overflows are simply append only.');

      console.log("AHH BALANCE");
      await fireStash.balance('collection2');
      dat2 = await getDocs(query(collection(fireStash.db, 'firestash'), where('collection', '==', 'collection2')));
      page0Count = Object.keys(dat2.docs[0]?.data()?.cache || {}).length;
      page1Count = Object.keys(dat2.docs[1]?.data()?.cache || {}).length;
      assert.ok((Math.abs(page0Count - page1Count) / COUNT) * 100 < 3, 'Pages re-balance with less than 3% error.');
    });

    it('handles empty collection requests', async function() {
      const res = await fireStash.get('missingcollection');
      return assert.strictEqual(Object.keys(res).length, 0, 'Fetches all values');
    });

    it('large streaming gets are performant', async function() {
      this.timeout(240000);

      const promises: Promise<void>[] = [];
      const bigString = 'x'.repeat(30 * 1024);
      const ids: string[] = [];
      performance.mark('updateStart');
      console.log('Writing')
      for (let i = 0; i < 15000; i++) {
        ids.push(`id${i}`);
        promises.push(fireStash.update('bulkcollection', `id${i}`, { id: bigString }));
      }

      await Promise.allSettled(promises);
      performance.mark('updateEnd');
      performance.measure('bulkUpdate', 'updateStart', 'updateEnd');
      console.log('Written');
      await fireStash.allSettled();
      console.log('Settled');

      console.log('Fetching');
      performance.mark('bulkGetStart');
      let now = performance.now();
      const res = await fireStash.get('bulkcollection');
      let done = performance.now();
      performance.mark('bulkGetEnd');
      performance.measure('bulkGet', 'bulkGetStart', 'bulkGetEnd');
      // console.log('got', Object.keys(res).length);

      assert.strictEqual(Object.keys(res).length, 15000, 'Fetches all values');
      // console.log('time', done - now);
      // console.log((performance as any).getEntriesByName('bulkGet'));
      // console.log((performance as any).getEntriesByName('bulkUpdate'));
      console.log('Fetched in', done - now, 'ms');
      assert.ok(done - now < 120000, 'Get time is not blown out.'); // TODO: 1.5s should be the goal here...

      now = performance.now();
      const res2 = await fireStash.get('bulkcollection', ids);
      done = performance.now();
      console.log('Fetched in', done - now, 'ms');
      assert.strictEqual(Object.keys(res2).length, 15000, 'Fetches all values');
      assert.ok(done - now < 8000, 'Get time is not blown out.'); // TODO: 1.5s should be the goal here...
    });

    it('cache-only updates are batched in groups of 490', async function() {
      this.timeout(30000);
      let saveCount = 0;
      fireStash.on('save', () => { saveCount++; });

      for (let i = 0; i < (490 * 2) + 1; i++) {
        fireStash.update('collection2', `id${i}`);
      }
      await fireStash.allSettled();
      assert.strictEqual(saveCount, 3);
    });

    it('batches massive key updates across many collection', async function() {
      this.timeout(30000);
      let saveCount = 0;
      fireStash.on('save', () => { saveCount++; });

      // Contrived to show that we batch document updates in groups of 500 – the limit for Firestore.batch().
      for (let i = 0; i < 1000; i++) {
        fireStash.update(`collection${i}`, `id${i}`, { i });
      }

      assert.deepStrictEqual(await fireStash.stash('collection0'), { collection: 'collection0', cache: {} }, 'Throttles cache writes');
      assert.deepStrictEqual(await fireStash.stash('collection999'), { collection: 'collection999', cache: {} }, 'Throttles cache writes');
      assert.strictEqual(saveCount, 0, 'Throttles large multi-collection writes in batches of 10, which include collection names');

      await fireStash.allSettled();
      assert.strictEqual(saveCount, 201, 'Throttles large multi-collection writes in batches of 10');
      assert.deepStrictEqual(await fireStash.stash('collection0'), { collection: 'collection0', cache: { id0: 1 } }, 'Throttles cache writes');
      assert.deepStrictEqual(await fireStash.stash('collection999'), { collection: 'collection999', cache: { id999: 1 } }, 'Throttles cache writes');
    });

    it('bust increments all previously known ids', async function() {
      this.timeout(6000);
      await setDoc(doc(fireStash.db, 'collection3/foo'), { a: 1 });
      await setDoc(doc(fireStash.db, 'collection3/bar'), { b: 2 });
      await setDoc(doc(fireStash.db, 'collection3/biz'), { c: 3 });
      fireStash.update('collection3', 'foo');
      fireStash.update('collection3', 'bar');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 1, bar: 1 } }, 'Initial cache correct');
      await fireStash.bust('collection3');
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 2, bar: 2 } }, 'Known cache busted 1');
      await setDoc(doc(fireStash.db, 'collection/baz'), { c: 3 });
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 2, bar: 2 } }, 'Known cache busted 2');
      await fireStash.bust('collection3');
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 3, bar: 3 } }, 'Known cache busted 3');
      await fireStash.update('collection3', 'biz');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 3, bar: 3, biz: 1 } }, 'Known cache busted 4');
      await fireStash.bust('collection3');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 4, bar: 4, biz: 2 } }, 'Known cache busted 5');
    });

    it('ensure generates a new stash from scratch', async function() {
      this.timeout(3000);

      await setDoc(doc(fireStash.db, 'collection4/foo'), { a: 1 });
      await setDoc(doc(fireStash.db, 'collection4/bar'), { b: 2 });
      await setDoc(doc(fireStash.db, 'collection4/biz'), { c: 3 });

      assert.deepStrictEqual(await fireStash.stash('collection4'), { collection: 'collection4', cache: {} }, 'No cache initially');
      await fireStash.ensure('collection4');
      assert.deepStrictEqual(await fireStash.stash('collection4'), { collection: 'collection4', cache: { foo: 1, bar: 1, biz: 1 } }, 'Full cache after ensure');
      await setDoc(doc(fireStash.db, 'collection4/baz'), { d: 4 });
      assert.deepStrictEqual(await fireStash.stash('collection4'), { collection: 'collection4', cache: { foo: 1, bar: 1, biz: 1 } }, 'Cache unchanged after bare addition');
      await fireStash.ensure('collection4');
      assert.deepStrictEqual(await fireStash.stash('collection4'), { collection: 'collection4', cache: { foo: 1, bar: 1, biz: 1, baz: 1 } }, 'Missing key added after ensure');
    });

    it('pagination update tests', async function() {
      this.timeout(3000);
      process.env.FIRESTASH_PAGINATION = 10;

      const batch = writeBatch(fireStash.db)
      const collection: Record<string, Record<'id', number>> = {};
      for (let i = 0; i < 100; i++) {
        batch.set(doc(fireStash.db, `collection5/id${i}`), { id: i });
        collection[`id${i}`] = { id: i };
        fireStash.update('collection5', `id${i}`);
      }

      await batch.commit();
      await fireStash.allSettled();

      assert.deepStrictEqual((await fireStash.stash('collection5')).cache.id50, 1, 'Updates existing cache entries on multiple pages 1.');
      assert.deepStrictEqual((await fireStash.get('collection5', 'id50')), { id: 50 }, 'Updates existing cache entries on multiple pages 2.');
      assert.deepStrictEqual((await fireStash.get('collection5')), collection, 'Updates existing cache entries on multiple pages 3.');

      await setDoc(doc(fireStash.db, 'collection5/id50'), { id: 500 });
      await fireStash.update('collection5', 'id50');
      await fireStash.allSettled();
      collection.id50.id = 500;

      assert.deepStrictEqual((await fireStash.stash('collection5')).cache.id50, 2, 'Updates existing cache entries on multiple pages 4.');
      assert.deepStrictEqual((await fireStash.get('collection5', 'id50')), { id: 500 }, 'Updates existing cache entries on multiple pages 5.');
      assert.deepStrictEqual((await fireStash.get('collection5')), collection, 'Updates existing cache entries on multiple pages 6.');

      process.env.FIRESTASH_PAGINATION = undefined;
    });

    it('pagination update tests with deep collections', async function() {
      this.timeout(3000);
      process.env.FIRESTASH_PAGINATION = 10;

      const batch = writeBatch(fireStash.db)
      const collection: Record<string, Record<'id', number>> = {};
      for (let i = 0; i < 100; i++) {
        batch.set(doc(fireStash.db, `collection5/element/page/id${i}`), { id: i });
        collection[`id${i}`] = { id: i };
        fireStash.update('collection5/element/page', `id${i}`);
      }

      await batch.commit();
      await fireStash.allSettled();

      console.log((await fireStash.stash('collection5/element/page')).cache.id50)

      assert.deepStrictEqual((await fireStash.stash('collection5/element/page')).cache.id50, 1, 'Updates existing cache entries on multiple pages 1.');
      assert.deepStrictEqual((await fireStash.get('collection5/element/page', 'id50')), { id: 50 }, 'Updates existing cache entries on multiple pages 2.');
      assert.deepStrictEqual((await fireStash.get('collection5/element/page')), collection, 'Updates existing cache entries on multiple pages 3.');

      await setDoc(doc(fireStash.db, 'collection5/element/page/id50'), { id: 500 });
      await fireStash.update('collection5/element/page', 'id50');
      await fireStash.allSettled();

      collection.id50.id = 500;
      assert.deepStrictEqual((await fireStash.stash('collection5/element/page')).cache.id50, 2, 'Updates existing cache entries on multiple pages 4.');
      assert.deepStrictEqual((await fireStash.get('collection5/element/page', 'id50')), { id: 500 }, 'Updates existing cache entries on multiple pages 5.');
      assert.deepStrictEqual((await fireStash.get('collection5/element/page')), collection, 'Updates existing cache entries on multiple pages 6.');

      process.env.FIRESTASH_PAGINATION = undefined;
    });

    it('listens to remote', async function() {
      this.timeout(6000);

      let called = 0;
      const cacheKey = fireStash.cacheKey('contacts', 0);
      await fireStash.update('contacts', 'id1');
      fireStash.on('contacts', () => called++);
      await fireStash.watch('contacts');
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache write and writes');
      const cache = (await getDoc(doc(fireStash.db, 'firestash', cacheKey))).data();
      assert.deepStrictEqual(await fireStash.stash('contacts'), cache, 'Local and remote are synced');
      if (!cache) throw new Error('No Cache Object');

      cache.cache.id1++;
      await setDoc(doc(fireStash.db, 'firestash', cacheKey), cache, { merge: true });
      cache.cache.id1++;
      await setDoc(doc(fireStash.db, 'firestash', cacheKey), cache, { merge: true });
      await wait(1200);

      cache.cache.id1++;
      await setDoc(doc(fireStash.db, 'firestash', cacheKey), cache, { merge: true });
      await wait(1200);

      assert.strictEqual(called, 2, 'Listens for remote updates');
    });

    it('throttles listener when updates exceed one per second', async function() {
      this.timeout(10000);
      let called = 0;
      const cacheKey = fireStash.cacheKey('contacts', 0);
      await fireStash.update('contacts', 'id1');
      fireStash.on('contacts', () => ++called);
      await fireStash.watch('contacts');
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache write and writes');
      const cache = (await getDoc(doc(fireStash.db, 'firestash', cacheKey))).data();
      assert.deepStrictEqual(await fireStash.stash('contacts'), cache, 'Local and remote are synced');
      if (!cache) throw new Error('No Cache Object');

      for (let i = 0; i < 4; i++) {
        cache.cache.id1++;
        await setDoc(doc(fireStash.db, 'firestash', cacheKey), cache, { merge: true });
        await wait(180);
      }

      await wait(5000);
      assert.strictEqual(called, 3, 'Listens for remote updates');
    });

    it('throttles listener when updates exceed a consistent one per second and updates', async function() {
      this.timeout(10000);
      let called = 0;
      const cacheKey = fireStash.cacheKey('contacts', 0);
      await fireStash.update('contacts', 'id1');
      fireStash.on('contacts', () => ++called);
      await fireStash.watch('contacts');
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache write and writes');
      const cache = (await getDoc(doc(fireStash.db, 'firestash', cacheKey))).data();
      assert.deepStrictEqual(await fireStash.stash('contacts'), cache, 'Local and remote are synced');
      if (!cache) throw new Error('No Cache Object');

      for (let i = 0; i < 30; i++) {
        cache.cache.id1++;
        await setDoc(doc(fireStash.db, 'firestash', cacheKey), cache, { merge: true });
        await wait(200);
      }
      await fireStash.allSettled();
      assert.strictEqual(called, 7, 'Listens for remote updates');
    });

    it('batches many update calls', async function() {
      this.timeout(80000);
      await fireStash.watch('contacts');
      for (let i = 0; i < 10; i++) {
        await Promise.all([
          fireStash.update('contacts', 'id1'),
          fireStash.update('contacts', 'id2'),
        ]);
        await fireStash.allSettled();
      }

      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { id1: 10, id2: 10 } }, 'Throttles cache write and writes');
    });

    it('overflows cache tables gracefully', async function() {
      this.timeout(60000);
      (await setDoc(doc(fireStash.db, `firestash/${cacheKey('overflow', 0)}`), { big: '0'.repeat(1048576 - (1500 * 3)), }, { merge: true }));
      // Max key size is 1500 bytes
      const BIG_STRING = '0'.repeat(1500);
      // Basically a half megabyte string. Will cause a page overflow on update 3.
      const max = 3;
      for (let i = 0; i < max; i++) {
        const key = `${BIG_STRING.slice(0, (String(i).length * -1) - 1)}_${i}`;
        await fireStash.update('overflow', key, { id: i });
        try {

          assert.deepStrictEqual((await fireStash.get<{ id: number }>('overflow', key)), { id: i });
          assert.deepStrictEqual((await getDoc(doc(fireStash.db, `overflow/${key}`))).data(), { id: i });
          assert.strictEqual((await getDoc(doc(fireStash.db, `firestash/${cacheKey('overflow', 0)}`))).exists(), true);
          assert.strictEqual((await getDoc(doc(fireStash.db, `firestash/${cacheKey('overflow', 1)}`))).exists(), i >= (max - 1));
        }
        catch (err) {
          console.error(err);
          throw err;
        }
      }

      assert.deepStrictEqual(Object.values((await getDoc(doc(fireStash.db, `firestash/${cacheKey('overflow', 0)}`))).data()?.cache), [ 1, 1 ]);
      assert.deepStrictEqual(Object.values((await getDoc(doc(fireStash.db, `firestash/${cacheKey('overflow', 1)}`))).data()?.cache), [1]);
    });

    it('small remote updates', async function() {
      this.timeout(60000);
      fireStash.update('remote-changes', '1', { id: 0 });
      await fireStash.allSettled();
      await setDoc(doc(fireStash.db, 'remote-changes/1'), { id: 1 });
      await setDoc(doc(fireStash.db, `firestash/${cacheKey('remote-changes', 0)}`), { collection: 'remote-changes', cache: { 1: 1 } });
      await fireStash.stop();
      fireStash = new FireStash(CONFIG, {
        directory: path.join(__dirname, String(appId)),
        customToken,
        worker: true,
      });
      await fireStash.allSettled();
      const data = await fireStash.get('remote-changes', ['1']);
      assert.deepStrictEqual(data, { 1: { id: 1 } });
    });

    it('large remote updates', async function() {
      this.timeout(60000);
      const ids = [];
      const expected: Record<string, { id: number }> = {};
      for (let i = 0; i < 50; i++) {
        await setDoc(doc(fireStash.db, `remote-changes/${i}`), { id: i });
        await setDoc(doc(fireStash.db, `firestash/${cacheKey('remote-changes', 0)}`), { collection: 'remote-changes', cache: { [i]: i } }, { merge: true });
        ids.push(`${i}`);
        expected[i] = { id: i };
      }
      const data = await fireStash.get('remote-changes', ids);
      assert.deepStrictEqual(data, expected);
    });
  });
});