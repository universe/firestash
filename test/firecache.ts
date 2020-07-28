/* global describe, it, after, afterEach */
import * as assert from 'assert';
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
    const fireStash = new FireStash(fireTest as unknown as typeof Firebase, app as unknown as Firebase.app.App, __dirname);

    afterEach(async function() {
      fireStash.unwatch();
      await fireTest.clearFirestoreData({ projectId });
    });

    after(async() => {
      await app.delete();
    });

    it('is able to insert a key', async function() {
      this.timeout(10000);
      fireStash.update('contacts', 'id1');
      assert.deepStrictEqual(await fireStash.get('contacts'), { collection: 'contacts', cache: {} }, 'Throttles cache writes');
      await wait(1001);
      assert.deepStrictEqual(await fireStash.get('contacts'), { collection: 'contacts', cache: { id1: 1 } }, 'Throttles cache writes, resolved in 1s');
    });

    it('batches multiple key updates in the same collection', async function() {
      this.timeout(10000);
      fireStash.update('contacts2', 'id1');
      fireStash.update('contacts2', 'id1');
      fireStash.update('contacts2', 'id2');
      assert.deepStrictEqual(await fireStash.get('contacts2'), { collection: 'contacts2', cache: {} }, 'Throttles cache writes');
      await wait(1001);
      assert.deepStrictEqual(await fireStash.get('contacts2'), { collection: 'contacts2', cache: { id1: 1, id2: 1 } }, 'Throttled cache bundles writes');
    });

    it('batches massive key updates within one collection', async function() {
      this.timeout(30000);

      let i = 0;
      fireStash.on('save', () => { i++; });

      // Contrived to show that we batch document updates in groups of 500, including collection names – the limit for Firestore.batch().
      const cache = {};
      for (let i = 0; i < 1000; i++) {
        fireStash.update('collection', `id${i}`);
        cache[`id${i}`] = 1;
      }

      assert.deepStrictEqual(await fireStash.get('collection'), { collection: 'collection', cache: {} }, 'Throttles cache writes');
      assert.strictEqual(i, 0, 'Throttles large multi-collection writes in batches of 500');
      await fireStash.allSettled();
      assert.strictEqual(i, 3, 'Throttles large multi-collection writes in batches of 500');
      assert.deepStrictEqual(await fireStash.get('collection'), { collection: 'collection', cache }, 'Throttles cache writes');
    });

    it('shards massive caches within one collection', async function() {
      this.timeout(60000);

      fireStash.on('commit', () => console.log('commit'));
      fireStash.on('re-balance', () => console.log('re-balance'));
      fireStash.on('settled', () => console.log('settled'));

      const cache = {};
      for (let i = 0; i < 20000; i++) {
        fireStash.update('collection', `id${i}`);
        cache[`id${i}`] = 1;
      }
      await fireStash.allSettled();
      assert.deepStrictEqual(Object.keys((await fireStash.get('collection')).cache).length, 20000, 'Writes an obscene amount of data.');
      const dat = await fireStash.db.collection('firestash').where('collection', '==', 'collection').get();
      assert.deepStrictEqual(dat.docs.length, 1, '20,000 keys and below stay in a single page.');
      fireStash.update('collection', `id${20001}`);
      await fireStash.allSettled();
      const dat2 = await fireStash.db.collection('firestash').where('collection', '==', 'collection').get();
      assert.deepStrictEqual(dat2.docs.length, 2, 'Shards above 20,000 keys');
      const page0Count = Object.keys(dat2.docs[0]?.data()?.cache || {}).length;
      const page1Count = Object.keys(dat2.docs[1]?.data()?.cache || {}).length;
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

      assert.deepStrictEqual(await fireStash.get('collection0'), { collection: 'collection0', cache: {} }, 'Throttles cache writes');
      assert.deepStrictEqual(await fireStash.get('collection999'), { collection: 'collection999', cache: {} }, 'Throttles cache writes');
      assert.strictEqual(i, 0, 'Throttles large multi-collection writes in batches of 500, which include collection names');
      await fireStash.allSettled();
      assert.strictEqual(i, 5, 'Throttles large multi-collection writes in batches of 500');
      assert.deepStrictEqual(await fireStash.get('collection0'), { collection: 'collection0', cache: { id0: 1 } }, 'Throttles cache writes');
      assert.deepStrictEqual(await fireStash.get('collection999'), { collection: 'collection999', cache: { id999: 1 } }, 'Throttles cache writes');
    });
  });
});
