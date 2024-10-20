import type Firebase from 'firebase-admin';
import { EventEmitter } from 'events';

export type ServiceAccount = Firebase.ServiceAccount;

export interface FireStashOptions {
  datastore: 'sqlite' | 'rocksdb' | 'leveldown' | 'memdown';
  readOnly: boolean;
  lowMem: boolean;
  directory: string | null;
}

export interface IFireStashPage<T = number> {
  collection: string;
  cache: Record<string, T>;
}

const DEFAULT_OPTIONS: FireStashOptions = {
  datastore: 'sqlite',
  readOnly: false,
  lowMem: false,
  directory: null,
};

export interface IFireStash {
  app: Firebase.app.App;
  db: Firebase.firestore.Firestore;
  cacheKey(collection: string, page: number): string;
  watchers(): Promise<string[]>;
  allSettled(): Promise<void>;
  stash(collection: string): Promise<IFireStashPage>;
  watch(collection: string): Promise<() => void>;
  unwatch(collection: string): Promise<void>;
  stop(): Promise<void>;
  stream<T=object>(collection: string, id?: string | string[]): AsyncGenerator<[string, T | null], void, void>;
  get<T=object>(collection: string): Promise<Record<string, T | null>>;
  get<T=object>(collection: string, id: string): Promise<T | null>;
  get<T=object>(collection: string, id: string[]): Promise<Record<string, T | null>>;
  get<T=object>(collection: string, id?: string | string[]): Promise<Record<string, T | null> | T | null>;
  update(collection: string, key: string, obj: object | null | undefined): Promise<void>;
  delete(collection: string, key: string): Promise<void>;
  bust(collection: string, key?: string): Promise<void>;
  purge(collection: string): Promise<void>;
  ensure(collection: string, key?: string): Promise<void>;
  balance(collection: string): Promise<void>;
}

declare interface AbstractFireStash {
  on(event: 'fetch', listener: (collection: string, id: string) => any): this;
  on(event: 'save', listener: () => any): this;
  on(event: string, listener: (collection: string, updates: string[]) => any): this;
}

abstract class AbstractFireStash extends EventEmitter implements IFireStash {
  protected project: ServiceAccount | string | null;
  protected options: FireStashOptions = { ...DEFAULT_OPTIONS };

  public readonly abstract app: Firebase.app.App;
  public readonly abstract db: Firebase.firestore.Firestore;
  public readonly abstract firebase: typeof Firebase;

  /**
   * Create a new FireStash. Binds to the app database provided. Writes stash backups to disk if directory is provided.
   * @param firebase The Firebase Library.
   * @param app The Firebase App Instance.
   * @param directory The cache backup directory (optional)
   */
  constructor(project: ServiceAccount | string | null, options?: Partial<FireStashOptions> | undefined) {
    super();
    this.project = project;
    this.options = Object.assign(this.options, options);
  }

  abstract cacheKey(collection: string, page: number): string;

  /**
   * Resolves with all collections currently being watched for updates.
   */
  abstract watchers(): Promise<string[]>;

  /**
   * Resolves when all previously called updates are written to remote. Like requestAnimationFrame for the collection cache.
   */
  abstract allSettled(): Promise<void>;

  /**
   * Get the entire stash cache for a collection.
   * @param collection Collection Path
   */
  abstract stash(collection: string): Promise<IFireStashPage>;

  /**
   * Watch for updates from a collection stash.
   * @param collection Collection Path
   */
  abstract watch(collection: string): Promise<() => void>;

  /**
   * Finish the last update and disconnect all watchers and timeouts.
   */
  abstract unwatch(collection: string): Promise<void>;

  /**
   * Finish the last update and disconnect all watchers and timeouts.
   */
  abstract stop(): Promise<void>;

  /**
   * Stream the entire stash cache for a collection or specific set of model IDs.
   * @param collection Collection Path
   */
  /* eslint-disable no-dupe-class-members */
  abstract stream<T=object>(collection: string, id?: string | string[]): AsyncGenerator<[string, T | null], void, void>;

  /**
   * Get the entire stash cache for a collection or specific set of model IDs.
   * @param collection Collection Path
   */
  /* eslint-disable no-dupe-class-members */
  abstract get<T=object>(collection: string): Promise<Record<string, T | null>>;
  abstract get<T=object>(collection: string, id: string): Promise<T | null>;
  abstract get<T=object>(collection: string, id: string[]): Promise<Record<string, T | null>>;
  abstract get<T=object>(collection: string, id?: string | string[]): Promise<Record<string, T | null> | T | null>;

  /**
   * Increment a single document's generation cache key. Syncs to remote once per second.
   * @param collection Collection Path
   */
  abstract update(collection: string, key: string, obj: object | null | undefined): Promise<void>;

  /**
   * Increment a single document's generation cache key. Syncs to remote once per second.
   * @param collection Collection Path
   */
  abstract delete(collection: string, key: string): Promise<void>;

  /**
   * Bust all existing cache keys by incrementing by one.
   * @param collection Collection Path
   */
  abstract bust(collection: string, key?: string): Promise<void>;

  /**
   * Destroys any record of the collection in the stash.
   * @param collection Collection Path
   */
  abstract purge(collection: string): Promise<void>;

  /**
   * Ensure all documents in the collection are present in the cache. Will not update existing cache keys.
   * @param collection Collection Path
   */
  abstract ensure(collection: string, key?: string): Promise<void>;

  /**
   * Balance the distribution of cache keys between all available pages.
   * @param collection Collection Path
   */
  abstract balance(collection: string): Promise<void>;
}

export default AbstractFireStash;
