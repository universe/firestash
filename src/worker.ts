import FireStash, { IFireStash, FireStashOptions, ServiceAccount } from './lib';

const [ credential, options, parentPid ]: [ ServiceAccount, FireStashOptions, number ] = [ JSON.parse(process.argv[2]), JSON.parse(process.argv[3]), parseInt(process.argv[4]) ];

class WorkerFireStash extends FireStash {
  emit(type: string | symbol, ...args: any[]): boolean {
    const res = super.emit(type, ...args);
    process.send?.([ 'event', type, args ]);
    return res;
  }
}

// If the parent process dies and we become a zombie, shut ourselves down.
// The "0" signal will test for the existence of the process.
setInterval(() => {
  try { process.kill(parentPid, 0); }
  catch { process.exit(); }
}, 1000 * 10);

const firestash = new WorkerFireStash(credential, options);
const CALLBACK_METHODS = { watch: 1, onSnapshot: 1 };
const unsubscribeCallbacks: Record<number, () => any> = {};
const STREAMS: Record<number, AsyncIterableIterator<any>> = {};
// eslint-disable-next-line max-len
process.on('message', async function <M extends Exclude<keyof IFireStash, 'db' | 'app'>>([ id, [ method, args ]]: [number, [ M, Parameters<IFireStash[M]>] | ['unsubscribe', []] | ['stream-end', []]]) {
  if (method === 'unsubscribe') {
    unsubscribeCallbacks[id]?.();
    process.send?.([ 'unsubscribe', id ]);
    return;
  }
  if (method === 'stream-end') {
    STREAMS[id]?.return?.();
    delete STREAMS[id];
    return;
  }
  if (method === 'stream') {
    try {
      /* eslint-disable prefer-spread */
      const stream = STREAMS[id] = STREAMS[id] || firestash.stream.apply(firestash, args);
      const value = await stream.next();
      process.send?.([ 'iterator', id, value ]);
      if (value.done) { delete STREAMS[id]; }
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
    .then((res) => process.send?.([ 'method', id, CALLBACK_METHODS[method as string] ? undefined : res ]))
    .catch((err) => process.send?.([ 'method', id, undefined, err ]));
});
