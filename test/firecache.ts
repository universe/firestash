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
      await fireStash.unwatch();
      await fireTest.clearFirestoreData({ projectId });
      fireStash = new FireStash(Firebase, app, path.join(__dirname, String(appId++)));
      await wait(300);
    });

    after(async() => {
      await app.delete();
    });

    it('is able to insert a key', async function() {
      fireStash.update('contacts', 'id1');
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: {} }, 'Throttles cache writes');
      await wait(1300);
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache writes, resolved in ~1s');
    });

    it('creates in-memory local db if started with no root directory', async function() {
      const memStash = new FireStash(Firebase, app);
      memStash.update('contacts', 'id1');
      await memStash.allSettled();
      assert.deepStrictEqual(await memStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache writes, resolved in 1s');
      await memStash.unwatch();
    });

    it('only calling update does not start a watcher', async function() {
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
      for (let i = 0; i < 20000; i++) {
        fireStash.update('collection2', `id${i}`);
        cache[`id${i}`] = 1;
      }
      await fireStash.allSettled();
      assert.deepStrictEqual(Object.keys((await fireStash.stash('collection2')).cache).length, 20000, 'Writes an obscene amount of data.');

      const dat = await fireStash.db.collection('firestash').where('collection', '==', 'collection2').get();
      assert.deepStrictEqual(dat.docs.length, 1, '20,000 keys and below stay in a single page.');

      fireStash.update('collection2', `id${20000}`);
      await fireStash.allSettled();

      let dat2 = await fireStash.db.collection('firestash').where('collection', '==', 'collection2').get();
      assert.deepStrictEqual(dat2.docs.length, 2, 'Shards above 20,000 keys');

      let page0Count = Object.keys(dat2.docs[0]?.data()?.cache || {}).length;
      let page1Count = Object.keys(dat2.docs[1]?.data()?.cache || {}).length;
      assert.ok(page0Count === 20000, 'Initial cache overflows are simply append only.');
      assert.ok(page1Count === 1, 'Initial cache overflows are simply append only.');

      await fireStash.balance('collection2');
      dat2 = await fireStash.db.collection('firestash').where('collection', '==', 'collection2').get();
      page0Count = Object.keys(dat2.docs[0]?.data()?.cache || {}).length;
      page1Count = Object.keys(dat2.docs[1]?.data()?.cache || {}).length;
      assert.ok((Math.abs(page0Count - page1Count) / 20000) * 100 < 3, 'Pages re-balance with less than 3% error.');
    });

    it('batches massive key updates across many collection', async function() {
      let saveCount = 0;
      fireStash.on('save', () => { saveCount++; });

      console.timeStamp('Update-Calls-Start');
      // Contrived to show that we batch document updates in groups of 500 – the limit for Firestore.batch().
      for (let i = 0; i < 1000; i++) {
        fireStash.update(`collection${i}`, `id${i}`);
      }

      assert.deepStrictEqual(await fireStash.stash('collection0'), { collection: 'collection0', cache: {} }, 'Throttles cache writes');
      assert.deepStrictEqual(await fireStash.stash('collection999'), { collection: 'collection999', cache: {} }, 'Throttles cache writes');
      assert.strictEqual(saveCount, 0, 'Throttles large multi-collection writes in batches of 500, which include collection names');

      await fireStash.allSettled();

      assert.strictEqual(saveCount, 4, 'Throttles large multi-collection writes in batches of 500');
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
  });
});
