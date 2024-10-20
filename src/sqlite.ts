import { Database, Statement, default as SQLiteConstructor } from 'better-sqlite3';
import * as fs from 'fs';

export interface LevelSQLiteBatch {
  put(key: string, value: Buffer | string): void;
  del(key: string): void;
  write(): Promise<void>;
}

export interface LevelSQLiteIterator {
  next(cb: (err: Error | undefined, id: [string] | undefined) => void): void;
  end(cb?: () => void): void;
  [Symbol.asyncIterator](): AsyncIterableIterator<[string]>;
}

function initDb(path: string, readonly: boolean): Database {
  if (!SQLiteConstructor) { throw new Error('Missing optional peer dependency "better-sqlite3".'); }
  let db: Database;
  try { db = new SQLiteConstructor(path, { readonly }); }
  catch {
    fs.existsSync(path) && fs.unlinkSync(path);
    fs.existsSync(`${path}-shm`) && fs.unlinkSync(`${path}-shm`);
    fs.existsSync(`${path}-wal`) && fs.unlinkSync(`${path}-wal`);
    db = new SQLiteConstructor(path);
  }
  db.pragma('journal_mode = WAL');
  db.unsafeMode(true);
  if (!readonly) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS store (
        "key" TEXT NOT NULL,
        "value" BLOB,
        CONSTRAINT PrimaryKey PRIMARY KEY ("key")
      ) WITHOUT ROWID;
    `);
  }
  return db;
}

export default class LevelSQLite {
  public readonly path: string;
  public readonly iterDb: Database;
  public readonly db: Database;
  public open: boolean = true;
  private _get: Statement;
  private _getAll: Statement;
  private _put: Statement;
  private _del: Statement;
  constructor(path: string) {
    this.path = path;
    this.db = initDb(path, false);
    this.iterDb = initDb(path, true);
    this._get = this.db.prepare<[string]>('SELECT "value" FROM "store" WHERE "key" = ?').pluck();
    this._getAll = this.db.prepare<[string[]]>(`SELECT "key", "value" FROM "store" WHERE "key" IN (${new Array(100).fill('?').join(', ')})`);
    this._put = this.db.prepare<{ key: string; value: Buffer; }>('INSERT INTO "store" ("key","value") VALUES (@key, @value) ON CONFLICT ("key") DO UPDATE SET "value" = @value;');
    this._del = this.db.prepare<[string]>('DELETE FROM "store" WHERE "key" = ?');
  }

  async get(key: string, _options?: { asBuffer?: boolean }, cb?: (err: Error | undefined, value: Buffer | null) => void): Promise<Buffer | undefined> {
    const res = this._get.get(key) as Buffer || undefined;
    cb?.(undefined, res);
    return res;
  }

  async getMany(keys: string[], _options?: { asBuffer?: boolean }, cb?: (err: Error | undefined, value: Buffer[] | null) => void): Promise<(Buffer | undefined)[]> {
    const data = new Array(100);
    const out = new Array(keys.length).fill(null);
    const reverseLookup: Record<string, number> = {};
    for (let page = 0; page < Math.ceil(keys.length / 100); page++) {
      for (let idx = 0; idx < 100; idx++) {
        data[idx] = keys[(page * 100) + idx];
        reverseLookup[keys[(page * 100) + idx]] = (page * 100) + idx;
      }
      const res = (this._getAll.all(data) || []) as { key: string; value: string; }[];
      for (const obj of res) {
        out[reverseLookup[obj.key]] = obj.value || null;
      }
    }
    cb?.(undefined, out);
    return out;
  }

  async del(key: string): Promise<void> {
    this._del.run(key);
  }

  async put(key: string, value: Buffer | string): Promise<void> {
    if (typeof value === 'string') { value = Buffer.from(value); }
    this._put.run({ key, value });
  }

  iterator(options?: {
    gte?: string | null;
    lte?: string | null;
    gt?: string | null;
    lt?: string | null;
    filter?: string | null;
    reverse?: boolean;
    values?: boolean;
    keys?: boolean;
    keyAsBuffer?: boolean;
    valueAsBuffer?: boolean;
  }): LevelSQLiteIterator {
    const iterDb = initDb(this.path, true);
    const _iteratorAsc = iterDb.prepare(`
      SELECT "key" FROM "store"
      WHERE
        (@gt ISNULL OR "key" > @gt) AND
        (@lt ISNULL OR "key" < @lt) AND
        (@gte ISNULL OR "key" >= @gte) AND
        (@lte ISNULL OR "key" <= @lte)
      ORDER BY "key" ASC
    `).raw();
    const _iteratorDesc = iterDb.prepare(`
      SELECT "key" FROM "store"
      WHERE
        (@gt ISNULL OR "key" > @gt) AND
        (@lt ISNULL OR "key" < @lt) AND
        (@gte ISNULL OR "key" >= @gte) AND
        (@lte ISNULL OR "key" <= @lte)
      ORDER BY "key" DESC
    `).raw();
    const iter = options?.reverse === true
      ? _iteratorDesc.iterate({ gt: options?.gt || null, lt: options?.lt || null, gte: options?.gte || null, lte: options?.lte || null, filter: options?.filter ? Buffer.from(options?.filter) : null })
      : _iteratorAsc.iterate({ gt: options?.gt || null, lt: options?.lt || null, gte: options?.gte || null, lte: options?.lte || null, filter: options?.filter ? Buffer.from(options?.filter) : null });
    return {
      next: (cb: (err: Error | undefined, id: [string] | undefined) => void) => {
        const value = iter.next().value as [string] | undefined;
        if (!value) {
          iter?.return?.();
          iterDb?.close();
        }
        cb(undefined, value);
      },
      end: (cb: () => void) => {
        iter?.return?.();
        iterDb?.close();
        cb?.();
      },
      async * [Symbol.asyncIterator]() {
        let value: IteratorResult<[string]>;
        try { while ((value = iter.next() as IteratorResult<[string]>) && !value.done) { yield value.value; } }
        finally {
          iter.return?.();
          iterDb?.close();
        }
      },
    };
  }

  batch(): LevelSQLiteBatch {
    // this.db.exec('BEGIN TRANSACTION');
    return {
      put: (key: string, value: Buffer | string) => this.put(key, value),
      del: (key: string) => this.del(key),
      write: () => {
        // this.db.exec('COMMIT');
        return Promise.resolve();
      },
    };
  }

  close(): void {
    this.open = false;
    this.db?.close();
    this.iterDb?.close();
  }
}
