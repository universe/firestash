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
    const app = fireTest.initializeAdminApp({ projectId });
    let appId = 0;
    let fireStash: FireStash = new FireStash(fireTest as unknown as typeof Firebase, app as unknown as Firebase.app.App, path.join(__dirname, String(appId++)));

    afterEach(async function() {
      this.timeout(5000);
      await fireStash.unwatch();
      await fireTest.clearFirestoreData({ projectId });
      fireStash = new FireStash(fireTest as unknown as typeof Firebase, app as unknown as Firebase.app.App, path.join(__dirname, String(appId++)));
      await wait(300);
    });

    after(async() => {
      await app.delete();
    });

    it('is able to insert a key', async function() {
      this.timeout(10000);
      fireStash.update('contacts', 'id1');
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: {} }, 'Throttles cache writes');
      await wait(1001);
      assert.deepStrictEqual(await fireStash.stash('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache writes, resolved in 1s');
    });

    it('is able to insert a key for sub collection', async function() {
      this.timeout(10000);
      fireStash.update('contacts/adam/notes', 'note1');
      assert.deepStrictEqual(await fireStash.stash('contacts/adam/notes'), { collection: 'contacts/adam/notes', cache: {} }, 'Throttles cache writes');
      await wait(1001);
      assert.deepStrictEqual(await fireStash.stash('contacts/adam/notes'), { collection: 'contacts/adam/notes', cache: { note1: 1 } }, 'Throttles cache writes, resolved in 1s');
    });

    it('batches multiple key updates in the same collection', async function() {
      this.timeout(10000);
      fireStash.update('contacts2', 'id1');
      fireStash.update('contacts2', 'id1');
      fireStash.update('contacts2', 'id2');
      assert.deepStrictEqual(await fireStash.stash('contacts2'), { collection: 'contacts2', cache: {} }, 'Throttles cache writes');
      await wait(1001);
      assert.deepStrictEqual(await fireStash.stash('contacts2'), { collection: 'contacts2', cache: { id1: 1, id2: 1 } }, 'Throttled cache bundles writes');
    });

    it('batches multiple key updates in the same collection for sub collection', async function() {
      this.timeout(10000);
      fireStash.update('contacts2/adam/notes', 'id1');
      fireStash.update('contacts2/adam/notes', 'id1');
      fireStash.update('contacts2/adam/notes', 'id2');
      assert.deepStrictEqual(await fireStash.stash('contacts2/adam/notes'), { collection: 'contacts2/adam/notes', cache: {} }, 'Throttles cache writes');
      await wait(1001);
      assert.deepStrictEqual(await fireStash.stash('contacts2/adam/notes'), { collection: 'contacts2/adam/notes', cache: { id1: 1, id2: 1 } }, 'Throttled cache bundles writes');
    });

    it('batches large key updates within one collection', async function() {
      this.timeout(30000);

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
      this.timeout(60000);

      // fireStash.on('commit', () => console.log('commit'));
      // fireStash.on('balance', () => console.log('balance'));
      // fireStash.on('settled', () => console.log('settled'));

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
      this.timeout(60000);

      let i = 0;
      fireStash.on('save', () => { i++; });

      // Contrived to show that we batch document updates in groups of 500 – the limit for Firestore.batch().
      for (let i = 0; i < 1000; i++) {
        fireStash.update(`collection${i}`, `id${i}`);
      }

      assert.deepStrictEqual(await fireStash.stash('collection0'), { collection: 'collection0', cache: {} }, 'Throttles cache writes');
      assert.deepStrictEqual(await fireStash.stash('collection999'), { collection: 'collection999', cache: {} }, 'Throttles cache writes');
      assert.strictEqual(i, 0, 'Throttles large multi-collection writes in batches of 500, which include collection names');
      await fireStash.allSettled();
      assert.strictEqual(i, 5, 'Throttles large multi-collection writes in batches of 500');
      assert.deepStrictEqual(await fireStash.stash('collection0'), { collection: 'collection0', cache: { id0: 1 } }, 'Throttles cache writes');
      assert.deepStrictEqual(await fireStash.stash('collection999'), { collection: 'collection999', cache: { id999: 1 } }, 'Throttles cache writes');
    });

    it('bust increments all previously known ids', async function() {
      this.timeout(30000);
      await wait(5000);
      await fireStash.db.doc('collection3/foo').set({ a: 1 });
      await fireStash.db.doc('collection3/bar').set({ b: 2 });
      await fireStash.db.doc('collection3/biz').set({ c: 3 });
      fireStash.update('collection3', 'foo');
      fireStash.update('collection3', 'bar');
      await fireStash.allSettled();
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 1, bar: 1 } }, 'Initial cache correct');
      await fireStash.bust('collection3');
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 2, bar: 2 } }, 'Known cache busted');
      await fireStash.db.doc('collection/baz').set({ c: 3 });
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 2, bar: 2 } }, 'Known cache busted');
      await fireStash.bust('collection3');
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 3, bar: 3 } }, 'Known cache busted');
      await fireStash.update('collection3', 'biz');
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 3, bar: 3, biz: 1 } }, 'Known cache busted');
      await fireStash.bust('collection3');
      assert.deepStrictEqual(await fireStash.stash('collection3'), { collection: 'collection3', cache: { foo: 4, bar: 4, biz: 2 } }, 'Known cache busted');
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
      this.timeout(30000);
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
      await wait(3000);

      assert.deepStrictEqual((await fireStash.stash('collection5')).cache.id50, 1, 'Updates existing cache entries on multiple pages.');
      assert.deepStrictEqual((await fireStash.get('collection5', 'id50')), { id: 50 }, 'Updates existing cache entries on multiple pages.');
      assert.deepStrictEqual((await fireStash.get('collection5')), collection, 'Updates existing cache entries on multiple pages.');

      await fireStash.db.doc('collection5/id50').set({ id: 500 });
      await fireStash.update('collection5', 'id50');
      await fireStash.allSettled();
      await wait(3000);
      collection.id50.id = 500;

      assert.deepStrictEqual((await fireStash.stash('collection5')).cache.id50, 2, 'Updates existing cache entries on multiple pages.');
      assert.deepStrictEqual((await fireStash.get('collection5', 'id50')), { id: 500 }, 'Updates existing cache entries on multiple pages.');
      assert.deepStrictEqual((await fireStash.get('collection5')), collection, 'Updates existing cache entries on multiple pages.');

      process.env.FIRESTASH_PAGINATION = undefined;
    });
  });
});
