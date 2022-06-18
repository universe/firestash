import type * as SQLite from 'better-sqlite3';

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

export default class LevelSQLite {
  private path: string;
  private db: SQLite.Database;
  private _get: SQLite.Statement;
  private _getAll: SQLite.Statement;
  private _put: SQLite.Statement;
  private _del: SQLite.Statement;
  private _iteratorAsc: SQLite.Statement;
  private _iteratorDesc: SQLite.Statement;
  constructor(path: string) {
    this.path = path;
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    let SQLiteConstructor: typeof SQLite | undefined;
    try { SQLiteConstructor = require('better-sqlite3'); }
    catch { 1; }
    if (!SQLiteConstructor) { throw new Error('Missing optional peer dependency "better-sqlite3".'); }
    this.db = new SQLiteConstructor(this.path);
    this.db.unsafeMode(true);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS store (
        "key" TEXT NOT NULL,
        "value" BLOB,
        CONSTRAINT PrimaryKey PRIMARY KEY ("key")
      ) WITHOUT ROWID;
    `);
    this._get = this.db.prepare<[string]>('SELECT "value" FROM "store" WHERE "key" = ?').pluck();
    this._getAll = this.db.prepare<[string[]]>(`SELECT "key", "value" FROM "store" WHERE "key" IN (${new Array(100).fill('?').join(', ')})`);
    this._put = this.db.prepare<{ key: string; value: Buffer; }>('INSERT INTO "store" ("key","value") VALUES (@key, @value) ON CONFLICT ("key") DO UPDATE SET "value" = @value;');
    this._del = this.db.prepare<[string]>('DELETE FROM "store" WHERE "key" = ?');
    this._iteratorAsc = this.db.prepare(`
      SELECT "key" FROM "store"
      WHERE
        (@gt ISNULL OR "key" > @gt) AND
        (@lt ISNULL OR "key" < @lt) AND
        (@gte ISNULL OR "key" >= @gte) AND
        (@lte ISNULL OR "key" <= @lte)
      ORDER BY "key" ASC
    `).raw();
    this._iteratorDesc = this.db.prepare(`
      SELECT "key" FROM "store"
      WHERE
        (@gt ISNULL OR "key" > @gt) AND
        (@lt ISNULL OR "key" < @lt) AND
        (@gte ISNULL OR "key" >= @gte) AND
        (@lte ISNULL OR "key" <= @lte)
      ORDER BY "key" DESC
    `).raw();
  }

  async get(key: string, _options?: { asBuffer?: boolean }, cb?: (err: Error | undefined, value: Buffer | null) => void): Promise<Buffer | undefined> {
    const res = this._get.get(key) || undefined;
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
      const res = this._getAll.all(data) || [];
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
    reverse?: boolean;
    values?: boolean;
    keys?: boolean;
    keyAsBuffer?: boolean;
    valueAsBuffer?: boolean;
  }): LevelSQLiteIterator {
    const iter = options?.reverse === true
      ? this._iteratorDesc.iterate({ gt: options?.gt || null, lt: options?.lt || null, gte: options?.gte || null, lte: options?.lte || null })
      : this._iteratorAsc.iterate({ gt: options?.gt || null, lt: options?.lt || null, gte: options?.gte || null, lte: options?.lte || null });
    return {
      next: (cb: (err: Error | undefined, id: [string] | undefined) => void) => {
        const value = iter.next().value as [string] | undefined;
        if (!value) iter.return?.();
        cb(undefined, value);
      },
      end: (cb: () => void) => {
        iter.return?.();
        cb?.();
      },
      async * [Symbol.asyncIterator]() {
        let value: IteratorResult<[string]>;
        try { while ((value = iter.next()) && !value.done) { yield value.value; } }
        finally { iter.return?.(); }
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
    this.db.close();
  }
}
