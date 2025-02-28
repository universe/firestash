// A deferred promise that can be externally resolved or rejected.
export class Deferred<T = void> extends Promise<T> {
  public resolve!: (value: T | PromiseLike<T>) => void;
  public reject!: (err: Error) => void;
  constructor() {
    let res!: (value: T | PromiseLike<T>) => void;
    let rej!: (err: Error) => void;
    super((resolve, reject) => { res = resolve; rej = reject; });
    this.resolve = res;
    this.reject = rej;
  }

  static get [Symbol.species](): typeof Promise { return Promise; }
  get [Symbol.toStringTag](): string { return 'Deferred'; }
}

export class PromisePool {
  running = false;
  max = 0;
  count = 0;
  pending: [() => Promise<unknown>, Deferred<any>][] = [];
  constructor(max: number) {
    this.max = max;
  }

  working: Set<Promise<void>> = new Set();
  async exhaust() {
    if (this.running) { return; }
    this.running = true;
    let i = 0;
    while (this.pending.length) {
      const val = this.pending.shift();
      if (!val) { continue; }
      const [ fn, deferred ] = val;
      if (this.working.size >= this.max) {
        await Promise.race(Array.from(this.working));
      }
      const work = fn().then(deferred.resolve).catch(deferred.reject).finally(() => {
        this.working.delete(work);
      });
      this.working.add(work);

      // Task list timeslice based on live working count backpressure.
      !(i++ % (this.max - this.working.size)) && await new Promise(resolve => setTimeout(resolve, 10));
    }
    this.running = false;
  }

  run<T>(fn: () => Promise<T>): Deferred<Awaited<ReturnType<typeof fn>>> {
    const deferred = new Deferred<Awaited<ReturnType<typeof fn>>>();
    this.pending.push([fn, deferred]);
    this.exhaust();
    return deferred;
  }
}
