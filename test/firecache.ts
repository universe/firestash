/* global describe, it, after, afterEach */
import * as assert from 'assert';
import * as path from 'path';
import * as fireTest from '@firebase/testing';
import Firebase from 'firebase-admin';

import FireStash from '../src';

const projectId = 'fire-stash';

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Connector', function() {
  describe('it should', function() {
    const app = Firebase.initializeApp({ projectId });
    let appId = 0;
    let fireStash = new FireStash(Firebase, app, path.join(__dirname, String(appId++)));

    afterEach(async function() {
      this.timeout(5000);
      await fireStash.stop();
      await fireTest.clearFirestoreData({ projectId });
      fireStash = new FireStash(Firebase, app, path.join(__dirname, String(appId++)));
      await wait(300);
    });

    after(async() => {
      await fireStash.stop();
      await fireTest.clearFirestoreData({ projectId });
      await app.delete();
      process.exit(0);
    });

    it('is able to insert a key', async function() {
      fireStash.update('contacts', 'id1');
      const start = Date.now();
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: {} }, 'Throttles cache writes');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache write and writes');
      assert.ok((Date.now() - start) < 2000, 'Resolves in ~1s');
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
      const memStash = new FireStash(Firebase, app);
      memStash.update('contacts', 'id1');
      await memStash.allSettled();
      assert.deepStrictEqual(await memStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache writes, resolved in 1s');
      await memStash.stop();
    });

    it('only calling update does start a watcher', async function() {
      fireStash.update('contacts', 'id1');
      await fireStash.allSettled();
      assert.deepStrictEqual(fireStash.watchers.size, 0, 'No watchers started.');
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
      fireStash.db.doc('contacts/2').set({ biz: 'baz' });
      fireStash.update('contacts', '2');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { 1: 1, 2: 1 } }, 'Stash correctly set');
      assert.deepStrictEqual(await fireStash.get('contacts'), { 1: { foo: 'bar' }, 2: { biz: 'baz' } }, 'Gets all data');
      assert.deepStrictEqual(fetches, ['contacts/2'], 'Fetches only what is necessary');
    });

    it('is able to update a key set to undefined', async function() {
      await fireStash.update('contacts', '3', { foo: 'bar' });
      assert.deepStrictEqual(await fireStash.get('contacts', '3'), { foo: 'bar' });
      assert.deepStrictEqual((await fireStash.db.doc('contacts/3').get()).data(), { foo: 'bar' });

      await fireStash.update('contacts', '3', { foo: undefined });
      assert.deepStrictEqual(await fireStash.get('contacts', '3'), { });
      assert.deepStrictEqual((await fireStash.db.doc('contacts/3').get()).data(), { });
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

      await fireStash.drainEventsPromise;
      setTimeout(async() => {
        assert.deepStrictEqual(await fireStash.get('contacts', '1'), expected, 'Sets all data locally immediately');
        assert.deepStrictEqual(fetches, [], 'Fetches only what is necessary after local fire.');
      }, 10);

      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.get('contacts', '1'), expected, 'Sets all data locally after sync');
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { 1: 1 } }, 'Stash correctly set');
      assert.deepStrictEqual((await fireStash.db.doc('contacts/1').get()).data(), expected, 'Gets all data on remote');
      assert.deepStrictEqual(fetches, [], 'Fetches only what is necessary after remote update.');
    });

    it('one update with no content will force sync with remote', async function() {
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
      await fireStash.drainEventsPromise;
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
      assert.deepStrictEqual((await fireStash.db.doc('contacts/1').get()).data(), expected, 'Gets all data on remote');
      assert.deepStrictEqual(fetches, ['contacts/1'], 'Fetches only what is necessary');
      assert.strictEqual(1, ran, 'Ran events tests');
    });

    it('is able to update deep collection keys with an object to cache', async function() {
      const fetches: string[] = [];
      fireStash.on('fetch', (collection, id) => {
        fetches.push(`${collection}/${id}`);
      });
      fireStash.update('contacts/1/phones', '1234567890', { foo: 'bar' });
      fireStash.db.doc('contacts/1/phones/0987654321').set({ biz: 'baz' });
      fireStash.update('contacts/1/phones', '0987654321');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('contacts/1/phones'), { collection: 'contacts/1/phones', cache: { 1234567890: 1, '0987654321': 1 } }, 'Stash correctly set');
      assert.deepStrictEqual(await fireStash.get('contacts/1/phones'), { 1234567890: { foo: 'bar' }, '0987654321': { biz: 'baz' } }, 'Gets all data');
      assert.deepStrictEqual(fetches, ['contacts/1/phones/0987654321'], 'Fetches only what is necessary');
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
      let i = 0;
      fireStash.on('save', () => { i++; });

      // Contrived to show that we batch document updates in groups of 500, including collection names – the limit for Firestore.batch().
      const cache = {};
      for (let i = 0; i < 1000; i++) {
        fireStash.update('collection', `id${i}`);
        cache[`id${i}`] = 1;
      }

      assert.deepStrictEqual(await fireStash.stash('collection'), { collection: 'collection', cache: {} }, 'Throttles cache writes');
      assert.strictEqual(i, 0, 'Throttles large multi-collection writes in batches of 500');
      await fireStash.allSettled();
      assert.strictEqual(i, 3, 'Throttles large multi-collection writes in batches of 500');
      assert.deepStrictEqual(await fireStash.stash('collection'), { collection: 'collection', cache }, 'Throttles cache writes');
    });

    it('shards massive caches within one collection', async function() {
      this.timeout(6000);

      const cache = {};
      const objects = {};
      const promises: Promise<FirebaseFirestore.WriteResult[]>[] = [];

      let batch = fireStash.db.batch();
      for (let i = 0; i < 15000; i++) {
        fireStash.update('collection2', `id${i}`);
        fireStash.update(`collection2/id${i}/sub-page`, String(i));
        fireStash.update(`irrelevent/id${i}/sub-page`, String(i));
        batch.set(fireStash.db.doc(`collection2/id${i}`), { id: i });
        cache[`id${i}`] = 1;
        objects[`collection2/id${i}`] = { id: i };
        if (i % 500 === 0) {
          promises.push(batch.commit());
          batch = fireStash.db.batch();
        }
      }
      promises.push(batch.commit());
      await Promise.allSettled(promises);
      await fireStash.allSettled();
      assert.deepStrictEqual(Object.keys((await fireStash.stash('collection2')).cache).length, 15000, 'Writes an obscene amount of data.');

      const res = await fireStash.get('collection2');
      assert.deepStrictEqual(Object.keys(res).filter(Boolean).length, 15000, 'Fetches an obscene amount of data keys.');
      assert.deepStrictEqual(Object.values(res).filter(Boolean).length, 15000, 'Fetches an obscene amount of data values.');

      const dat = await fireStash.db.collection('firestash').where('collection', '==', 'collection2').get();
      assert.deepStrictEqual(dat.docs.length, 1, '15,000 keys and below stay in a single page.');

      fireStash.update('collection2', `id${15000}`);
      await fireStash.allSettled();

      let dat2 = await fireStash.db.collection('firestash').where('collection', '==', 'collection2').get();
      assert.deepStrictEqual(dat2.docs.length, 2, 'Shards above 15,000 keys');

      let page0Count = Object.keys(dat2.docs[0]?.data()?.cache || {}).length;
      let page1Count = Object.keys(dat2.docs[1]?.data()?.cache || {}).length;
      assert.ok(page0Count === 15000, 'Initial cache overflows are simply append only.');
      assert.ok(page1Count === 1, 'Initial cache overflows are simply append only.');

      await fireStash.balance('collection2');
      dat2 = await fireStash.db.collection('firestash').where('collection', '==', 'collection2').get();
      page0Count = Object.keys(dat2.docs[0]?.data()?.cache || {}).length;
      page1Count = Object.keys(dat2.docs[1]?.data()?.cache || {}).length;
      assert.ok((Math.abs(page0Count - page1Count) / 15000) * 100 < 3, 'Pages re-balance with less than 3% error.');
    });

    it('massive operations run in low memory mode', async function() {
      this.timeout(6000);

      fireStash.options.lowMem = true;

      const cache = {};
      const objects = {};
      const promises: Promise<FirebaseFirestore.WriteResult[]>[] = [];
      let batch = fireStash.db.batch();
      for (let i = 0; i < 15000; i++) {
        fireStash.update('collection2', `id${i}`);
        fireStash.update(`collection2/id${i}/sub-page`, String(i));
        fireStash.update(`irrelevent/id${i}/sub-page`, String(i));
        batch.set(fireStash.db.doc(`collection2/id${i}`), { id: i });
        cache[`id${i}`] = 1;
        objects[`collection2/id${i}`] = { id: i };
        if (i % 500 === 0) {
          promises.push(batch.commit());
          batch = fireStash.db.batch();
        }
      }
      promises.push(batch.commit());
      await Promise.allSettled(promises);
      await fireStash.allSettled();
      assert.deepStrictEqual(Object.keys((await fireStash.stash('collection2')).cache).length, 15000, 'Writes an obscene amount of data.');

      const res = await fireStash.get('collection2');
      assert.deepStrictEqual(Object.keys(res).filter(Boolean).length, 15000, 'Fetches an obscene amount of data keys.');
      assert.deepStrictEqual(Object.values(res).filter(Boolean).length, 15000, 'Fetches an obscene amount of data values.');

      const dat = await fireStash.db.collection('firestash').where('collection', '==', 'collection2').get();
      assert.deepStrictEqual(dat.docs.length, 1, '15,000 keys and below stay in a single page.');

      fireStash.update('collection2', `id${15000}`);
      await fireStash.allSettled();

      let dat2 = await fireStash.db.collection('firestash').where('collection', '==', 'collection2').get();
      assert.deepStrictEqual(dat2.docs.length, 2, 'Shards above 15,000 keys');

      let page0Count = Object.keys(dat2.docs[0]?.data()?.cache || {}).length;
      let page1Count = Object.keys(dat2.docs[1]?.data()?.cache || {}).length;
      assert.ok(page0Count === 15000, 'Initial cache overflows are simply append only.');
      assert.ok(page1Count === 1, 'Initial cache overflows are simply append only.');

      await fireStash.balance('collection2');
      dat2 = await fireStash.db.collection('firestash').where('collection', '==', 'collection2').get();
      page0Count = Object.keys(dat2.docs[0]?.data()?.cache || {}).length;
      page1Count = Object.keys(dat2.docs[1]?.data()?.cache || {}).length;
      assert.ok((Math.abs(page0Count - page1Count) / 15000) * 100 < 3, 'Pages re-balance with less than 3% error.');
    });

    it('batches massive key updates across many collection', async function() {
      let saveCount = 0;
      fireStash.on('save', () => { saveCount++; });

      // Contrived to show that we batch document updates in groups of 500 – the limit for Firestore.batch().
      for (let i = 0; i < 1000; i++) {
        fireStash.update(`collection${i}`, `id${i}`);
      }

      assert.deepStrictEqual(await fireStash.stash('collection0'), { collection: 'collection0', cache: {} }, 'Throttles cache writes');
      assert.deepStrictEqual(await fireStash.stash('collection999'), { collection: 'collection999', cache: {} }, 'Throttles cache writes');
      assert.strictEqual(saveCount, 0, 'Throttles large multi-collection writes in batches of 500, which include collection names');

      await fireStash.allSettled();

      assert.strictEqual(saveCount, 5, 'Throttles large multi-collection writes in batches of 500');
      assert.deepStrictEqual(await fireStash.stash('collection0'), { collection: 'collection0', cache: { id0: 1 } }, 'Throttles cache writes');
      assert.deepStrictEqual(await fireStash.stash('collection999'), { collection: 'collection999', cache: { id999: 1 } }, 'Throttles cache writes');
    });

    it('bust increments all previously known ids', async function() {
      this.timeout(6000);
      await fireStash.db.doc('collection3/foo').set({ a: 1 });
      await fireStash.db.doc('collection3/bar').set({ b: 2 });
      await fireStash.db.doc('collection3/biz').set({ c: 3 });
      fireStash.update('collection3', 'foo');
      fireStash.update('collection3', 'bar');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 1, bar: 1 } }, 'Initial cache correct');
      await fireStash.bust('collection3');
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 2, bar: 2 } }, 'Known cache busted 1');
      await fireStash.db.doc('collection/baz').set({ c: 3 });
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 2, bar: 2 } }, 'Known cache busted 2');
      await fireStash.bust('collection3');
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 3, bar: 3 } }, 'Known cache busted 3');
      await fireStash.update('collection3', 'biz');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 3, bar: 3, biz: 1 } }, 'Known cache busted 4');
      await fireStash.bust('collection3');
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 4, bar: 4, biz: 2 } }, 'Known cache busted 5');
    });

    it('ensure generates a new stash from scratch', async function() {
      this.timeout(3000);

      await fireStash.db.doc('collection4/foo').set({ a: 1 });
      await fireStash.db.doc('collection4/bar').set({ b: 2 });
      await fireStash.db.doc('collection4/biz').set({ c: 3 });

      assert.deepStrictEqual(await fireStash.stash('collection4'), { collection: 'collection4', cache: {} }, 'No cache initially');
      await fireStash.ensure('collection4');
      assert.deepStrictEqual(await fireStash.stash('collection4'), { collection: 'collection4', cache: { foo: 1, bar: 1, biz: 1 } }, 'Full cache after ensure');
      await fireStash.db.doc('collection4/baz').set({ d: 4 });
      assert.deepStrictEqual(await fireStash.stash('collection4'), { collection: 'collection4', cache: { foo: 1, bar: 1, biz: 1 } }, 'Cache unchanged after bare addition');
      await fireStash.ensure('collection4');
      assert.deepStrictEqual(await fireStash.stash('collection4'), { collection: 'collection4', cache: { foo: 1, bar: 1, biz: 1, baz: 1 } }, 'Missing key added after ensure');
    });

    it('pagination update tests', async function() {
      this.timeout(3000);
      process.env.FIRESTASH_PAGINATION = 10;

      const batch = fireStash.db.batch();
      const collection: Record<string, Record<'id', number>> = {};
      for (let i = 0; i < 100; i++) {
        batch.set(fireStash.db.doc(`collection5/id${i}`), { id: i });
        collection[`id${i}`] = { id: i };
        fireStash.update('collection5', `id${i}`);
      }

      await batch.commit();
      await fireStash.allSettled();

      assert.deepStrictEqual((await fireStash.stash('collection5')).cache.id50, 1, 'Updates existing cache entries on multiple pages 1.');
      assert.deepStrictEqual((await fireStash.get('collection5', 'id50')), { id: 50 }, 'Updates existing cache entries on multiple pages 2.');
      assert.deepStrictEqual((await fireStash.get('collection5')), collection, 'Updates existing cache entries on multiple pages 3.');

      await fireStash.db.doc('collection5/id50').set({ id: 500 });
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

      const batch = fireStash.db.batch();
      const collection: Record<string, Record<'id', number>> = {};
      for (let i = 0; i < 100; i++) {
        batch.set(fireStash.db.doc(`collection5/element/page/id${i}`), { id: i });
        collection[`id${i}`] = { id: i };
        fireStash.update('collection5/element/page', `id${i}`);
      }

      await batch.commit();
      await fireStash.allSettled();

      assert.deepStrictEqual((await fireStash.stash('collection5/element/page')).cache.id50, 1, 'Updates existing cache entries on multiple pages 1.');
      assert.deepStrictEqual((await fireStash.get('collection5/element/page', 'id50')), { id: 50 }, 'Updates existing cache entries on multiple pages 2.');
      assert.deepStrictEqual((await fireStash.get('collection5/element/page')), collection, 'Updates existing cache entries on multiple pages 3.');

      await fireStash.db.doc('collection5/element/page/id50').set({ id: 500 });
      await fireStash.update('collection5/element/page', 'id50');
      await fireStash.allSettled();

      collection.id50.id = 500;
      assert.deepStrictEqual((await fireStash.stash('collection5/element/page')).cache.id50, 2, 'Updates existing cache entries on multiple pages 4.');
      assert.deepStrictEqual((await fireStash.get('collection5/element/page', 'id50')), { id: 500 }, 'Updates existing cache entries on multiple pages 5.');
      assert.deepStrictEqual((await fireStash.get('collection5/element/page')), collection, 'Updates existing cache entries on multiple pages 6.');

      process.env.FIRESTASH_PAGINATION = undefined;
    });

    it('listens to remote', async function() {
      let called = 0;
      const cacheKey = fireStash.cacheKey('contacts', 0);
      await fireStash.update('contacts', 'id1');
      fireStash.on('contacts', () => called++);
      await fireStash.watch('contacts');
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache write and writes');
      const cache = (await fireStash.db.collection('firestash').doc(cacheKey).get()).data();
      assert.deepStrictEqual(await fireStash.stash('contacts'), cache, 'Local and remote are synced');
      if (!cache) throw new Error('No Cache Object');

      cache.cache.id1++;
      await fireStash.db.collection('firestash').doc(cacheKey).set(cache, { merge: true });
      cache.cache.id1++;
      await fireStash.db.collection('firestash').doc(cacheKey).set(cache, { merge: true });
      await wait(1200);

      cache.cache.id1++;
      await fireStash.db.collection('firestash').doc(cacheKey).set(cache, { merge: true });
      await wait(1200);

      assert.strictEqual(called, 2, 'Listens for remote updates');
    });

    it('throttles listener when updates exceed one per second', async function() {
      let called = 0;
      const cacheKey = fireStash.cacheKey('contacts', 0);
      await fireStash.update('contacts', 'id1');
      fireStash.on('contacts', () => ++called);
      await fireStash.watch('contacts');
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache write and writes');
      const cache = (await fireStash.db.collection('firestash').doc(cacheKey).get()).data();
      assert.deepStrictEqual(await fireStash.stash('contacts'), cache, 'Local and remote are synced');
      if (!cache) throw new Error('No Cache Object');

      for (let i = 0; i < 4; i++) {
        cache.cache.id1++;
        await fireStash.db.collection('firestash').doc(cacheKey).set(cache, { merge: true });
        await wait(180);
      }

      await wait(5000);
      assert.strictEqual(called, 4, 'Listens for remote updates');
    });

    it('throttles listener when updates exceed a consistent one per second and updates', async function() {
      let called = 0;
      const cacheKey = fireStash.cacheKey('contacts', 0);
      await fireStash.update('contacts', 'id1');
      fireStash.on('contacts', () => ++called && console.log(Date.now()));
      await fireStash.watch('contacts');
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache write and writes');
      const cache = (await fireStash.db.collection('firestash').doc(cacheKey).get()).data();
      assert.deepStrictEqual(await fireStash.stash('contacts'), cache, 'Local and remote are synced');
      if (!cache) throw new Error('No Cache Object');

      for (let i = 0; i < 30; i++) {
        cache.cache.id1++;
        await fireStash.db.collection('firestash').doc(cacheKey).set(cache, { merge: true });
        await wait(200);
      }

      await wait(5000);
      assert.strictEqual(called, 9, 'Listens for remote updates');
    });

    it.only('uses throttled onSnapshot to listen to remote', async function() {
      const path = 'foo/bar1';
      let called = 0;
      await fireStash.onThrottledSnapshot(path, () => called++);

      fireStash.db.doc(path).set({ foo: 'a' });
      await wait(200);
      fireStash.db.doc(path).set({ foo: 'b' });
      await wait(1500);
      assert.strictEqual(called, 3, 'Listens for remote updates');
    });

    it.only('if onSnapshot listener gets more than 3 updates in a timeout window, it falls back to polling', async function() {
      const path = 'foo/bar2';
      let called = 0;
      await fireStash.onThrottledSnapshot(path, () => called++, 2000);

      for (let i = 0; i <= 30; i++) {
        await fireStash.db.doc(path).set({ foo: i });
        await wait(50);
      }
      await wait(2500);
      assert.strictEqual(called, 5, 'Num called after snapshot updates');
    });

    it.only('if onSnapshot listener gets more than 3 updates with no changes while polling, it returns to using onSnapshot', async function() {
      const path = 'foo/bar3';
      let called = 0;
      await fireStash.onThrottledSnapshot(path, () => called++);

      for (let i = 0; i <= 5; i++) {
        await fireStash.db.doc(path).set({ foo: i });
        await wait(50);
      }
      await wait(850);

      // Wait for polling updates...
      await wait(2950);

      // Back to listening for regular snapshots.
      for (let i = 0; i <= 10; i++) { // 3 before polling, 1 after polling
        await fireStash.db.doc(path).set({ foo: i });
        await wait(100);
      }
      assert.strictEqual(called, 12, 'Final num calls');
    });

    it.only('throttled snapshot listener can handle multiple callbacks', async function() {
      const path = 'foo/bar4';
      const called = { a: 0, b: 0, c: 0 };
      await fireStash.onThrottledSnapshot(path, () => called.a += 1);
      await fireStash.onThrottledSnapshot(path, () => called.b += 1);
      await fireStash.onThrottledSnapshot(path, () => called.c += 1);

      await wait(100);
      await fireStash.db.doc(path).set({ foo: 1 });
      await wait(100);
      await fireStash.db.doc(path).set({ foo: 2 });
      await wait(100);

      assert.deepStrictEqual(called, { a: 3, b: 3, c: 3 }, 'Final num called');
    });

    it.only('unsubscribe onSnapshot listener', async function() {
      const called = { a: 0, b: 0 };
      const path = 'foo/bar5';
      const a = await fireStash.onThrottledSnapshot(path, () => called.a += 1);
      const b = await fireStash.onThrottledSnapshot(path, () => called.b += 1);
      await wait(200);
      assert.deepStrictEqual(called, { a: 1, b: 1 }, 'Initial num called');

      await fireStash.db.doc(path).set({ foo: 1 });
      await wait(300);
      assert.deepStrictEqual(called, { a: 2, b: 2 }, 'Num called after first change');

      // Unsubscribe the first callback.
      a();
      await fireStash.db.doc(path).set({ foo: 2 });
      await wait(700);
      assert.deepStrictEqual(called, { a: 2, b: 3 }, 'Num called after unsubscribe');

      // Resubscribe the first callback.
      const c = await fireStash.onThrottledSnapshot(path, () => called.a += 1);
      await wait(100);
      await fireStash.db.doc(path).set({ foo: 3 });
      assert.deepStrictEqual(called, { a: 4, b: 5 }, 'Num called after resubscribe');

      await wait(1500);

      // Make polling kick in....
      for (let i = 0; i <= 40; i++) {
        await fireStash.db.doc(path).set({ foo: i });
      }
      await wait(100);
      assert.deepStrictEqual(called, { a: 10, b: 11 }, 'Num called after polling kicks in');

      // Unsubscribe both.
      b(); c();
      await fireStash.db.doc(path).set({ foo: 4 });
      await wait(200);
      assert.deepStrictEqual(called, { a: 10, b: 11 }, 'Num after  both unsubscribed');

      // Verify that unsubscribe clears polling as well.
      await wait(3000);
      assert.deepStrictEqual(called, { a: 10, b: 11 }, 'Final num called');
    });
  });
});
