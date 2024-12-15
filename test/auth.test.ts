import 'dotenv/config';

import { describe, before, beforeEach, after, it } from 'mocha';
import * as assert from 'assert';
import * as path from 'path';
import { initializeTestEnvironment } from '@firebase/rules-unit-testing';
import { fileURLToPath } from 'url';
import { initializeApp, deleteApp } from 'firebase-admin/app';
import { getAuth } from 'firebase-admin/auth';


import FireStash from '../src/lib.js';

const projectId = 'fire-stash';
const __dirname = fileURLToPath(new URL('.', import.meta.url));
const CONFIG = {
  projectId,
  apiKey: "TEST_API_KEY",
  appId: "1:testing",
}


function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Connector', function() {
  describe('it should', function() {
    const firebaseTest = initializeTestEnvironment({ projectId });
    let appId = 0;
    let fireStash: FireStash;
    let customToken: string;

    before(async () => {
      const adminApp = initializeApp(CONFIG, 'auth-tests');
      const adminAuth = getAuth(adminApp);
      await adminAuth.createUser({ uid: 'test-auth-user', email: 'test-auth-user@universe.test', emailVerified: true });
      customToken = await adminAuth.createCustomToken('test-auth-user');
      await deleteApp(adminApp);
    });

    beforeEach(async function() {
      this.timeout(60000);
      await fireStash?.stop();
      await (await firebaseTest).clearFirestore();
      await wait(300);
    });

    after(async() => {
      await fireStash.stop();
      await (await firebaseTest).clearFirestore();
    });

    it('throws if unauthenticated', async function() {
      this.timeout(3000);
      fireStash = new FireStash(CONFIG, { datastore: 'sqlite', directory: path.join(__dirname, String(appId++)) });
      await assert.rejects(async() => {
        await fireStash.update('private', '1', { id: 1 });
      });
    });

    it('runs if authenticated', async function() {
      this.timeout(3000);
      fireStash = new FireStash(CONFIG, { datastore: 'sqlite', directory: path.join(__dirname, String(appId++)), customToken });
      fireStash.update('private', '1', { id: 1 });
      await fireStash.allSettled();
      assert.deepStrictEqual(
        await fireStash.get('private', '1'),
        { id: 1 },
        'Writes data',
      );
      assert.deepStrictEqual(await fireStash.stash('private'), { collection: 'private', cache: { 1: 1 } }, 'Stores cache key');
    });
  });
});
