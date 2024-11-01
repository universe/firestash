import * as path from 'path';
import { fork, ChildProcess } from 'child_process';
import Firebase from 'firebase-admin';
import { nanoid } from 'nanoid';
import { fileURLToPath } from 'url';

import AbstractFireStash, { IFireStash, IFireStashPage, FireStashOptions, ServiceAccount } from './types.js';
import FireStashLib, { cacheKey } from './lib.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
type Awaited<T> = T extends PromiseLike<infer U> ? U : T
export default class FireStash extends AbstractFireStash {
  #worker: ChildProcess | null = null;
  #messageId = 0;
  #tasks: Record<number | string, [(value: any) => void, (err: Error) => void, string]> = {};
  #iterators: Record<number | string, [(res: { value: [string, any | null]; done: boolean }) => any, (val: any) => void]> = {};
  #listeners: Record<string, (snapshot?: unknown) => any> = {};

  app!: Firebase.app.App;
  db!: Firebase.firestore.Firestore;
  firebase!: typeof Firebase;

  constructor(project: ServiceAccount | string | null, options?: Partial<FireStashOptions> | undefined) {
    super(project, options);

    if (options?.worker === false) { return new FireStashLib(project, options) as unknown as FireStash; }

    // Bind listener methods.
    this.onWorkerMessage = this.onWorkerMessage.bind(this);
    this.killWorker = this.killWorker.bind(this);

    // Start our worker and listen for worker messages.
    this.#worker = fork(
      path.join(__dirname, './worker.js'),
      [ JSON.stringify(project), JSON.stringify(options || {}), String(process.pid) ],
      process.env.NODE_ENV === 'development' ? { execArgv: ['--inspect=40894'], serialization: 'advanced' } : { serialization: 'advanced' },
    );
    this.#worker?.on?.('message', this.onWorkerMessage);

    // Start our own firebase connection for in-process access.
    const creds = typeof project === 'string' ? { projectId: project } : { projectId: project?.projectId, credential: project ? Firebase.credential.cert(project) : undefined };
    this.firebase = Firebase;
    this.app = Firebase.initializeApp(creds, `${creds.projectId}-firestash-${nanoid()}`);
    this.db = this.app.firestore();

    // Save ourselves from annoying throws. This cases should be handled in-library anyway.
    this.db.settings({ ignoreUndefinedProperties: true });

    // Ensure we don't leave zombies around.
    process.on('exit', this.killWorker);
    process.on('SIGHUP', this.killWorker);
    process.on('SIGINT', this.killWorker);
    process.on('SIGTERM', this.killWorker);
  }

  private killWorker() {
    this.#worker?.killed || this.#worker?.kill?.();
  }

  private async onWorkerMessage([ type, id, res, err ]: ['method' | 'snapshot' | 'iterator'| 'event', string, any, string]) {
    if (type === 'method') {
      const cb = this.#tasks[id];
      delete this.#tasks[id];
      err ? cb[1](new Error(err)) : cb[0](res);
    }
    else if (type === 'event') { this.emit(id, ...res); }
    else if (type === 'snapshot') { this.#listeners[id]?.(res); }
    else if (type === 'iterator') {
      const cb = this.#iterators[id];
      delete this.#iterators[id];
      if (err) { cb[1](new Error(err)); }
      else { cb[0](res || null); }
    }
  }

  private async runInWorker<M extends Exclude<keyof IFireStash, 'db' | 'app'>>(
    message: [M, Parameters<IFireStash[M]>] | ['unsubscribe', [number]],
  ): Promise<Awaited<ReturnType<IFireStash[M]>>> {
    return new Promise<Awaited<ReturnType<IFireStash[M]>>>((resolve: (value: Awaited<ReturnType<IFireStash[M]>>) => void, reject: (err: Error) => void) => {
      const id = this.#messageId = (this.#messageId + 1) % Number.MAX_SAFE_INTEGER;
      this.#tasks[id] = [ resolve, reject, message[0] ];
      this.#worker?.send?.([ id, message ]);
    });
  }

  public cacheKey(collection: string, page: number): string { return cacheKey(collection, page); }
  public allSettled(): Promise<void> { return this.runInWorker([ 'allSettled', []]); }
  public stash(collection: string): Promise<IFireStashPage> { return this.runInWorker([ 'stash', [collection]]); }

  public async watch(collection: string): Promise<() => void> {
    await this.runInWorker([ 'watch', [collection]]);
    return () => this.unwatch(collection);
  }

  public watchers() { return this.runInWorker([ 'watchers', []]); }
  public unwatch(collection: string): Promise<void> { return this.runInWorker([ 'unwatch', [collection]]); }

  private deleted = false;
  public async stop(): Promise<void> {
    if (this.deleted === true) { return; }
    this.deleted = true;
    // Clean up our worker. Give it max three seconds to do it's business
    try {
      new Promise((resolve, reject) => {
        setTimeout(reject, 3000);
        this.runInWorker([ 'stop', []]).then(resolve);
      })
    } catch {
      this.#worker?.connected && this.#worker?.disconnect?.();
      this.#worker?.killed || this.#worker?.kill?.();
    }
    this.#worker?.off?.('message', this.onWorkerMessage);
    process.off('exit', this.killWorker);
    process.off('SIGHUP', this.killWorker);
    process.off('SIGINT', this.killWorker);
    process.off('SIGTERM', this.killWorker);

    // Stop the in-process firebase app.
    await this.app?.delete();
    for (const [resolve, reject, method] of Object.values(this.#tasks)) {
      method === 'stop' ? resolve(void 0) : reject(new Error('FireStash Stopped'));
    }
    for (const [, reject] of Object.values(this.#iterators)) {
      reject(new Error('FireStash Stopped'));
    }

    // Clean up our state.
    this.#tasks = {};
    this.#iterators = {};
    this.#listeners = {};
  }

  async * stream<T=object>(collection: string, id: string | string[] | null = null, filter: string | null = null): AsyncGenerator<[string, T | null], void, void> {
    let val: { value: [string, T | null]; done: boolean } = { value: [ '', null ], done: false };
    const iteratorId = this.#messageId = (this.#messageId + 1) % Number.MAX_SAFE_INTEGER;
    let firstRun = true;
    try {
      while (!val.done) {
        val = await new Promise<{ value: [string, T | null]; done: boolean }>((resolve, reject) => {
          this.#iterators[iteratorId] = [ resolve, reject ];
          this.#worker?.send?.([ iteratorId, [ 'stream', firstRun ? [ collection, id, filter ] : null ]]);
        });
        firstRun = false;
        if (val.done) { break; }
        yield val.value;
      }
    }
    finally {
      this.#worker?.send?.([ iteratorId, [ 'stream-end', null ]]);
    }
  }

  /* eslint-disable no-dupe-class-members */
  public get<T=object>(collection: string): Promise<Record<string, T | null>>;
  public get<T=object>(collection: string, id: string): Promise<T | null>;
  public get<T=object>(collection: string, id: string[]): Promise<Record<string, T | null>>;
  public get<T=object>(collection: string, id?: string | string[]): Promise<Record<string, T | null> | T | null> {
    return this.runInWorker([ 'get', [ collection, id ]]) as Promise<Record<string, T | null> | T | null>;
  }

  public update(collection: string, key: string, obj: object | null = null): Promise<void> { return this.runInWorker([ 'update', [ collection, key, obj ]]); }
  public delete(collection: string, key: string): Promise<void> { return this.runInWorker([ 'delete', [ collection, key ]]); }
  public bust(collection: string, key?: string): Promise<void> { return this.runInWorker([ 'bust', [ collection, key ]]); }
  public purge(collection: string): Promise<void> { return this.runInWorker([ 'purge', [collection]]); }
  public ensure(collection: string, key?: string): Promise<void> { return this.runInWorker([ 'ensure', [ collection, key ]]); }
  public balance(collection: string): Promise<void> { return this.runInWorker([ 'balance', [collection]]); }
}
