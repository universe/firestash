import { describe, beforeEach, after, it } from 'mocha';
import { assert } from 'chai';
import * as path from 'path';
import SQLite from '../src/sqlite';

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
      assert.strictEqual(gone, null, 'Value is gone');
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
      assert.strictEqual(await db.get('biz'), null, 'Value is gone');
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
          else { values.push(val); }
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
          else { values.push(val); }
        });
      }
      iter.end();
      assert.deepStrictEqual(values, [ 'fizz', 'foo' ]);
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
          else { values.push(val); }
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
          else { values.push(val); }
        });
      }
      iter.end();
      assert.deepStrictEqual(values, [ 'biz', 'fizz', 'foo' ]);
    });
  });
});
