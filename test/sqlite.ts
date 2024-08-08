import { describe, beforeEach, after, it } from 'mocha';
import { assert } from 'chai';
import * as path from 'path';
import SQLite from '../src/sqlite.js';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));

describe('Connector', function() {
  describe('it should', function() {
    let appId = 0;
    let db = new SQLite(path.join(__dirname, String(appId++) + '-sqlite'));

    beforeEach(async function() {
      this.timeout(60000);
      db.close();
      db = new SQLite(path.join(__dirname, String(appId++) + '-sqlite'));
    });

    after(async() => {
      db.close();
      process.exit(0);
    });

    it('is able to insert and read a key', async function() {
      const val = Buffer.from('bar');
      await db.put('foo', val);
      const res = await db.get('foo');
      assert.ok(res ? val.equals(res) : false, 'Gets and sets buffers.');
    });

    it('is able to delete a key', async function() {
      const val = Buffer.from('bar');
      await db.put('foo', val);
      const res = await db.get('foo');
      assert.ok(res ? val.equals(res) : false, 'Gets and sets buffers.');
      await db.del('foo');
      const gone = await db.get('foo');
      assert.strictEqual(gone, undefined, 'Value is gone');
    });

    it('is able to batch updates', async function() {
      const batch = db.batch();
      const bar = Buffer.from('bar');
      const baz = Buffer.from('baz');
      const buzz = Buffer.from('buzz');
      batch.put('foo', bar);
      batch.put('biz', baz);
      batch.put('fizz', buzz);
      batch.del('biz');
      await batch.write();

      assert.ok(bar.equals(await db.get('foo') as Buffer), 'Gets and sets buffers.');
      assert.ok(buzz.equals(await db.get('fizz') as Buffer), 'Gets and sets buffers.');
      assert.strictEqual(await db.get('biz'), undefined, 'Value is gone');
    });

    it('is able to iterate with gt and lt', async function() {
      const batch = db.batch();
      const bar = Buffer.from('bar');
      const baz = Buffer.from('baz');
      const buzz = Buffer.from('buzz');
      batch.put('foo', bar);
      batch.put('biz', baz);
      batch.put('fizz', buzz);
      await batch.write();

      let run = true;
      const values: string[] = [];
      const iter = db.iterator({ gt: 'bizz', lt: 'fizzz' });
      while (run) {
        iter.next((_err, val) => {
          if (!val) { run = false; }
          else { values.push(val[0]); }
        });
      }
      iter.end();
      assert.deepStrictEqual(values, ['fizz']);
    });

    it('is able to iterate just gt', async function() {
      const batch = db.batch();
      const bar = Buffer.from('bar');
      const baz = Buffer.from('baz');
      const buzz = Buffer.from('buzz');
      batch.put('foo', bar);
      batch.put('biz', baz);
      batch.put('fizz', buzz);
      await batch.write();

      let run = true;
      const values: string[] = [];
      const iter = db.iterator({ gt: 'bizz' });
      while (run) {
        iter.next((_err, val) => {
          if (!val) { run = false; }
          else { values.push(val[0]); }
        });
      }
      iter.end();
      assert.deepStrictEqual(values, [ 'fizz', 'foo' ]);
    });

    it('is able to iterate just gte', async function() {
      const batch = db.batch();
      const bar = Buffer.from('bar');
      const baz = Buffer.from('baz');
      const buzz = Buffer.from('buzz');
      batch.put('foo', bar);
      batch.put('biz', baz);
      batch.put('fizz', buzz);
      await batch.write();

      let run = true;
      const values: string[] = [];
      const iter = db.iterator({ gte: 'biz' });
      while (run) {
        iter.next((_err, val) => {
          if (!val) { run = false; }
          else { values.push(val[0]); }
        });
      }
      iter.end();
      assert.deepStrictEqual(values, [ 'biz', 'fizz', 'foo' ]);
    });

    it('is able to iterate just lt', async function() {
      const batch = db.batch();
      const bar = Buffer.from('bar');
      const baz = Buffer.from('baz');
      const buzz = Buffer.from('buzz');
      batch.put('foo', bar);
      batch.put('biz', baz);
      batch.put('fizz', buzz);
      await batch.write();

      let run = true;
      const values: string[] = [];
      const iter = db.iterator({ lt: 'fizzz' });
      while (run) {
        iter.next((_err, val) => {
          if (!val) { run = false; }
          else { values.push(val[0]); }
        });
      }
      iter.end();
      assert.deepStrictEqual(values, [ 'biz', 'fizz' ]);
    });

    it('is able to iterate just lte', async function() {
      const batch = db.batch();
      const bar = Buffer.from('bar');
      const baz = Buffer.from('baz');
      const buzz = Buffer.from('buzz');
      batch.put('foo', bar);
      batch.put('biz', baz);
      batch.put('fizz', buzz);
      await batch.write();

      let run = true;
      const values: string[] = [];
      const iter = db.iterator({ lte: 'fizz' });
      while (run) {
        iter.next((_err, val) => {
          if (!val) { run = false; }
          else { values.push(val[0]); }
        });
      }
      iter.end();
      assert.deepStrictEqual(values, [ 'biz', 'fizz' ]);
    });

    it('is able to iterate with no constraints', async function() {
      const batch = db.batch();
      const bar = Buffer.from('bar');
      const baz = Buffer.from('baz');
      const buzz = Buffer.from('buzz');
      batch.put('foo', bar);
      batch.put('biz', baz);
      batch.put('fizz', buzz);
      await batch.write();

      let run = true;
      const values: string[] = [];
      const iter = db.iterator();
      while (run) {
        iter.next((_err, val) => {
          if (!val) { run = false; }
          else { values.push(val[0]); }
        });
      }
      iter.end();
      assert.deepStrictEqual(values, [ 'biz', 'fizz', 'foo' ]);
    });

    it('is able to iterate with no constraints in reverse', async function() {
      const batch = db.batch();
      const bar = Buffer.from('bar');
      const baz = Buffer.from('baz');
      const buzz = Buffer.from('buzz');
      batch.put('foo', bar);
      batch.put('biz', baz);
      batch.put('fizz', buzz);
      await batch.write();

      let run = true;
      const values: string[] = [];
      const iter = db.iterator({ reverse: true });
      while (run) {
        iter.next((_err, val) => {
          if (!val) { run = false; }
          else { values.push(val[0]); }
        });
      }
      iter.end();
      assert.deepStrictEqual(values, [ 'foo', 'fizz', 'biz' ]);
    });

    it('is able to iterate concurrently', async function() {
      const batch = db.batch();
      for (let i = 0; i < 1000; i++) {
        batch.put(`${i}`, Buffer.from(`${i}`));
      }
      await batch.write();

      let i = 0;
      async function iterate() {
        for await (const _ of db.iterator({ reverse: true })) {
          i++;
        }
      }
      await Promise.all([
        iterate(),
        iterate(),
        iterate(),
        iterate(),
        iterate(),
      ]);

      assert.deepStrictEqual(i, 5000);
    });
  });
});
