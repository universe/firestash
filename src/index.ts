import * as path from 'path';
import { fork, ChildProcess } from 'child_process';
import { FirebaseApp, initializeApp, deleteApp } from 'firebase/app';
import { Auth, getAuth, signInWithCustomToken, connectAuthEmulator } from 'firebase/auth';
import { Firestore, initializeFirestore, connectFirestoreEmulator } from 'firebase/firestore';
import { nanoid } from 'nanoid';
import { fileURLToPath } from 'url';

import FireStashLib from './lib.js';
import AbstractFireStash, { cacheKey, IFireStash, IFireStashPage, FireStashOptions, FirebaseConfig } from './types.js';


export { cacheKey, type FireStashOptions, type FirebaseConfig, type IFireStash, type IFireStashPage }

function getDirname() {
  const parts = fileURLToPath(import.meta.url).split('/');
  parts.pop();
  return parts.join('/');
}

const IS_DEV = process.env.NODE_ENV !== 'production';

type Awaited<T> = T extends PromiseLike<infer U> ? U : T
export default class FireStash extends AbstractFireStash {
  #worker: ChildProcess | null = null;
  #messageId = 0;
  #tasks: Record<number | string, [(value: any) => void, (err: Error) => void, string]> = {};
  #iterators: Record<number | string, [(res: { value: [string, any | null]; done: boolean }) => any, (val: any) => void]> = {};
  #listeners: Record<string, (snapshot?: unknown) => any> = {};

  app!: FirebaseApp;
  db!: Firestore;
  auth!: Auth;

  constructor(config: FirebaseConfig, options?: Partial<FireStashOptions> | undefined) {
    super(config, options);

    if (options?.worker === false) { return new FireStashLib(config, options) as unknown as FireStash; }

    // Bind listener methods.
    this.onWorkerMessage = this.onWorkerMessage.bind(this);
    this.killWorker = this.killWorker.bind(this);

    // Start our worker and listen for worker messages.
    this.#worker = fork(
      path.join(getDirname(), './worker.js'),
      [ JSON.stringify(config || {}), JSON.stringify(options || {}), String(process.pid) ],
      // IS_DEV ? { serialization: 'advanced', execArgv: ['--inspect-brk=40894'], } : { serialization: 'advanced', execArgv: ['--inspect-brk=40894'], },
      IS_DEV ? { serialization: 'advanced' } : { serialization: 'advanced' },
    );
    this.#worker?.on?.('message', this.onWorkerMessage);

    // Start our own firebase connection for in-process access.
    this.app = initializeApp(config, `${config.projectId}-firestash-${nanoid()}`);

    // If a custom token was provided, sign the user in. Intentional fire and forget here.
    this.auth = getAuth(this.app);
    process.env.FIREBASE_AUTH_EMULATOR_HOST && connectAuthEmulator(this.auth, `http://${process.env.FIREBASE_AUTH_EMULATOR_HOST}`);
    options?.customToken && (signInWithCustomToken(this.auth, options.customToken));

    // Save ourselves from annoying throws. This cases should be handled in-library anyway.
    this.db = initializeFirestore(this.app, { ignoreUndefinedProperties: true });
    process.env.FIRESTORE_EMULATOR_HOST && connectFirestoreEmulator(
      this.db,
      process.env.FIRESTORE_EMULATOR_HOST?.split(':')?.[0] || 'localhost',
      parseInt(process.env.FIRESTORE_EMULATOR_HOST?.split(':')?.[1] || '5050') || 5050,
    );

    // Ensure we don't leave zombies around.
    process.on('exit', this.killWorker);
    process.on('SIGHUP', this.killWorker);
    process.on('SIGINT', this.killWorker);
    process.on('SIGTERM', this.killWorker);
  }

  private killWorker() {
    this.#worker?.connected && this.#worker?.disconnect?.();
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
      await new Promise((resolve, reject) => {
        setTimeout(reject, 3000);
        this.runInWorker([ 'stop', []]).then(resolve);
      });
    } catch (e) { 1; }
    try { this.killWorker(); } catch { 1; }

    this.#worker?.off?.('message', this.onWorkerMessage);
    process.off('exit', this.killWorker);
    process.off('SIGHUP', this.killWorker);
    process.off('SIGINT', this.killWorker);
    process.off('SIGTERM', this.killWorker);

    // Stop the in-process firebase app.
    await deleteApp(this.app);

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
