# FireStash

FireStash is a way to easily maintain an aggressive local cache of FireStore collections to reduce read costs and simplify data sync logic.

```ts
FireStash.get<Obj = object>(collection: string, documentId: string | string[]): Promise<Obj>;
```

```ts
FireStash.update(collection: string, documentId: string, object?: Json): Promise<void>;
```

```ts
FireStash.delete(collection: string, documentId: string): Promise<void>;
```

```ts
FireStash.watch(collection: string): Promise<() => void>;
```

```ts
FireStash.unwatch(collection: string): Promise<void>;
```

```ts
FireStash.stop(): Promise<void>;
```

```ts
FireStash.bust(collection: string, documentId?: string): Promise<void>;
```

```ts
FireStash.ensure(collection: string, documentId?: string): Promise<void>;
```

```ts
FireStash.purge(collection: string): Promise<void>;
```

```ts
FireStash.stash(collection: string): Promise<Record<string, number>>;
```

```ts
FireStash.cacheKey(collection: string, page: number): string;
```