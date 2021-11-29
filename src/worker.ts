import FireStash, { IFireStash, FireStashOptions, ServiceAccount } from './lib';

const [ credential, options ]: [ ServiceAccount, FireStashOptions ] = [ JSON.parse(process.argv[2]), JSON.parse(process.argv[3]) ];

class WorkerFireStash extends FireStash {
  emit(type: string | symbol, ...args: any[]): boolean {
    const res = super.emit(type, ...args);
    process.send?.([ 'event', type, args ]);
    return res;
  }
}

const firestash = new WorkerFireStash(credential, options);

const CALLBACK_METHODS = { watch: 1, onSnapshot: 1 };
const unsubscribeCallbacks: Record<number, () => any> = {};
const STREAMS: Record<number, AsyncIterableIterator<any>> = {};

process.on('message', async function <M extends Exclude<keyof IFireStash, 'db' | 'app'>>([ id, [ method, args ]]: [number, [ M, Parameters<IFireStash[M]>] | ['unsubscribe', []]]) {
  if (method === 'unsubscribe') {
    unsubscribeCallbacks[id]?.();
    process.send?.([ 'unsubscribe', id ]);
    return;
  }
  if (method === 'stream') {
    /* eslint-disable prefer-spread */
    const stream = STREAMS[id] = STREAMS[id] || firestash.stream.apply(firestash, args);
    try {
      const value = await stream.next();
      process.send?.([ 'iterator', id, value ]);
    }
    catch (err) {
      process.send?.([ 'iterator', id, null, err.message ]);
    }
    return;
  }
  if (method === 'onSnapshot') {
    args[1] = (doc) => process.send?.([ 'snapshot', id, doc ]);
  }
  /* eslint-disable prefer-spread */
  Promise.resolve()
    .then(() => firestash[method].apply(firestash, args))
    .then((res) => { method === 'onSnapshot' && (unsubscribeCallbacks[id] = res); return res; })
    .then((res) => process.send?.([ 'method', id, CALLBACK_METHODS[method as string] ? undefined : res ]));
});

process.on('exit', () => firestash.stop());
process.on('SIGHUP', () => process.exit(128 + 1));
process.on('SIGINT', () => process.exit(128 + 2));
process.on('SIGTERM', () => process.exit(128 + 15));
