import * as path from 'path';
import { fork, ChildProcess } from 'child_process';
import Firebase from 'firebase-admin';
import { nanoid } from 'nanoid';

import AbstractFireStash, { IFireStash, IFireStashPage, FireStashOptions, ServiceAccount } from './types';
import { cacheKey } from './lib';

type Awaited<T> = T extends PromiseLike<infer U> ? U : T
export default class FireStash extends AbstractFireStash {
  #worker: ChildProcess;
  #messageId = 0;
  #tasks: Record<number, [Function, Function]> = {};
  #iterators: Record<number, [(res: { value: [string, any | null]; done: boolean }) => any, Function]> = {};
  #listeners: Record<string, (snapshot?: unknown) => any> = {};

  app: Firebase.app.App;
  db: Firebase.firestore.Firestore;
  firebase: typeof Firebase;

  constructor(project: ServiceAccount | string | null, options?: Partial<FireStashOptions> | undefined) {
    super(project, options);
    this.#worker = fork(path.join(__dirname, './worker.js'), [ JSON.stringify(project), JSON.stringify(options), String(process.pid) ]);
    this.#worker.on('message', ([ type, id, res, err ]: ['method' | 'snapshot' | 'iterator'| 'event', string, any, string]) => {
      if (type === 'method') { this.#tasks[id][0](res); }
      else if (type === 'event') { this.emit(id, ...res); }
      else if (type === 'snapshot') { this.#listeners[id]?.(res); }
      else if (type === 'iterator') {
        if (err) { this.#iterators[id][1](new Error(err)); }
        else { this.#iterators[id][0](res || null); }
      }
    });

    // Start our own firebase connection for in-process access.
    const creds = typeof project === 'string' ? { projectId: project } : { projectId: project?.projectId, credential: project ? Firebase.credential.cert(project) : undefined };
    this.firebase = Firebase;
    this.app = Firebase.initializeApp(creds, `${creds.projectId}-firestash-${nanoid()}`);
    this.db = this.app.firestore();

    // Save ourselves from annoying throws. This cases should be handled in-library anyway.
    this.db.settings({ ignoreUndefinedProperties: true });

    // Ensure we dont leave zombies around.
    process.on('exit', () => this.#worker.kill());
    process.on('SIGHUP', () => this.#worker.kill());
    process.on('SIGINT', () => this.#worker.kill());
    process.on('SIGTERM', () => this.#worker.kill());
  }

  private async runInWorker<M extends Exclude<keyof IFireStash, 'db' | 'app'>>(message: [M, Parameters<IFireStash[M]>] | ['unsubscribe', [number]]): Promise<Awaited<ReturnType<IFireStash[M]>>> {
    return new Promise<Awaited<ReturnType<IFireStash[M]>>>((resolve, reject) => {
      const id = this.#messageId++;
      this.#tasks[id] = [ resolve, reject ];
      if (message[0] === 'onSnapshot') {
        this.#listeners[id] = message[1][1] as (snapshot?: unknown) => any;
        message[1][1] = undefined; // Don't send functions over IPC.
      }
      if (message[0] === 'unsubscribe') {
        delete this.#listeners[message[1][0] as number];
      }
      this.#worker.send([ id, message ]);
    });
  }

  public cacheKey(collection: string, page: number): string { return cacheKey(collection, page); }
  public allSettled(): Promise<void> { return this.runInWorker([ 'allSettled', []]); }
  public stash(collection: string): Promise<IFireStashPage> { return this.runInWorker([ 'stash', [collection]]); }

  public async watch(collection: string): Promise<() => void> {
    await this.runInWorker([ 'watch', [collection]]);
    return () => this.unwatch(collection);
  }

  public async onSnapshot<Data = any>(documentPath: string, callback: (snapshot?: Data) => any, timeout = 1000): Promise<() => void> {
    const id = this.#messageId;
    await this.runInWorker([ 'onSnapshot', [ documentPath, callback as (snapshot?: unknown) => any, timeout ]]);
    return () => { this.#worker.send([ id, [ 'unsubscribe', []]]); };
  }

  public watchers() { return this.runInWorker([ 'watchers', []]); }
  public unwatch(collection: string): Promise<void> { return this.runInWorker([ 'unwatch', [collection]]); }
  public stop(): Promise<void> { return this.runInWorker([ 'stop', []]); }

  async * stream<T=object>(collection: string, id?: string | string[]): AsyncGenerator<[string, T | null], void, void> {
    let val: { value: [string, T | null]; done: boolean } = { value: [ '', null ], done: false };
    const iteratorId = this.#messageId++;
    let firstRun = true;
    while (!val.done) {
      val = await new Promise<{ value: [string, T | null]; done: boolean }>((resolve, reject) => {
        this.#iterators[iteratorId] = [ resolve, reject ];
        this.#worker.send([ iteratorId, [ 'stream', firstRun ? [ collection, id ] : null ]]);
      });
      firstRun = false;
      if (val.done) { break; }
      yield val.value;
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
