import FireStash from './lib';
import * as path from 'path';
import { assert } from 'chai';

import { performance } from 'perf_hooks';

require('dotenv/config');

const projectId = 'fire-stash';

async function run() {
  let appId = 0;
  const fireStash = new FireStash(projectId, { directory: path.join(__dirname, String(appId++)) });

  const promises: Promise<void>[] = [];
  const bigString = 'x'.repeat(30 * 1024);
  const ids: string[] = [];
  performance.mark('updateStart');
  for (let i = 0; i < 15000; i++) {
    ids.push(`id${i}`);
    promises.push(fireStash.update('bulkcollection', `id${i}`, { id: bigString }));
  }
  console.log('awaiting');
  await Promise.allSettled(promises);
  performance.mark('updateEnd');
  performance.measure('bulkUpdate', 'updateStart', 'updateEnd');

  console.log('done');
  await fireStash.allSettled();
  console.log('settled');
  performance.mark('bulkGetStart');
  let now = performance.now();
  const res = await fireStash.get('bulkcollection');
  performance.mark('bulkGetEnd');
  performance.measure('bulkGet', 'bulkGetStart', 'bulkGetEnd');
  console.log('got', Object.keys(res).length);
  let done = performance.now();

  assert.strictEqual(Object.keys(res).length, 15000, 'Fetches all values');
  console.log('time', done - now);
  // assert.ok(done - now < 3000, 'Get time is not blown out.'); // TODO: 1.5s should be the goal here...

  now = performance.now();
  const res2 = await fireStash.get('bulkcollection', ids);
  done = performance.now();
  console.log('fetched', done - now);
  assert.strictEqual(Object.keys(res2).length, 15000, 'Fetches all values');
  process.exit(0);
  // assert.ok(done - now < 3000, 'Get time is not blown out.');
}

run();
