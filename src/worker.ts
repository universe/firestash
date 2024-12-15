import FireStash, { IFireStash, FireStashOptions, FirebaseConfig } from './lib.js';

const [ credential, options, parentPid ]: [ FirebaseConfig, FireStashOptions, number ] = [ JSON.parse(process.argv[2]), JSON.parse(process.argv[3]), parseInt(process.argv[4]) ];

class WorkerFireStash extends FireStash {
  emit(type: string | symbol, ...args: any[]): boolean {
    const res = super.emit(type, ...args);
    isConnected() && process.send?.([ 'event', type, args ]);
    return res;
  }
}

let isDisconnected = false;
process.on('disconnect', () => (isDisconnected = true));
function isConnected() { return process.connected && !isDisconnected; }

// If the parent process dies and we become a zombie, shut ourselves down.
// The "0" signal will test for the existence of the process.
const interval = setInterval(() => {
  try { process.kill(parentPid, 0); }
  catch { process.exit(0); }
}, 1000 * 10);

const firestash = new WorkerFireStash(credential, options);
const CALLBACK_METHODS = { watch: 1, onSnapshot: 1 };
const STREAMS: Record<number, AsyncIterableIterator<any>> = {};

// eslint-disable-next-line max-len
process.on('message', async function <M extends Exclude<keyof IFireStash, 'db' | 'app'>>([ id, [ method, args ]]: [number, [ M, Parameters<IFireStash[M]>] | ['stream-end', []]]) {
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
      isConnected() && process.send?.([ 'iterator', id, value ]);
      if (value.done) { delete STREAMS[id]; }
    }
    catch (err) {
      isConnected() && process.send?.([ 'iterator', id, null, err.message ]);
    }
    return;
  }

  /* eslint-disable prefer-spread */
  await Promise.resolve()
    .then(() => firestash[method].apply(firestash, args))
    .then((res) => (isConnected() && process.send?.([ 'method', id, CALLBACK_METHODS[method as string] ? undefined : res ])))
    .catch((err) => (isConnected() && process.send?.([ 'method', id, undefined, err ])));

  // If this is a stop command, commit seppuku after we make sure a response is returned.
  if (method === 'stop') {
    clearInterval(interval);
    setTimeout(() => {
      isConnected() && process.disconnect();
      process.exit(0);
    }, 300);
  }
});
