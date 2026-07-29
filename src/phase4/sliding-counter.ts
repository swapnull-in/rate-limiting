/**
 * Phase 4 — SLIDING WINDOW COUNTER: the practical middle ground. Run: npm run phase4
 *
 * The sliding LOG is exact but stores every timestamp. The sliding COUNTER keeps
 * just TWO integers per key — the count in the current fixed window and the
 * previous one — and estimates the rolling count by weighting the previous
 * window by how much of it still overlaps the rolling window:
 *
 *   estimate = currentCount + previousCount × (fraction of previous window still in view)
 *
 * Example: 1s windows, we're 30% into the current window, so the last 1000ms is
 * "70% of the previous window + all of the current". If that estimate < limit, allow.
 *
 *   PRO: O(1) memory (two counters), no boundary burst in practice.
 *   CON: slightly approximate — it assumes the previous window's requests were
 *        spread evenly. Off by a few percent; totally fine for real limiting.
 *
 * This is what most production limiters (and Redis-based ones) actually use.
 */

import { log } from "../lib/log.ts";

class SlidingWindowCounter {
  private windows = new Map<string, { window: number; curr: number; prev: number }>();
  private limit: number;
  private windowMs: number;

  constructor(limit: number, windowMs: number) {
    this.limit = limit;
    this.windowMs = windowMs;
  }

  private estimate(key: string, now: number): number {
    const window = Math.floor(now / this.windowMs);
    let e = this.windows.get(key);
    if (!e) e = { window, curr: 0, prev: 0 };                             // brand-new key
    else if (window === e.window + 1) e = { window, curr: 0, prev: e.curr }; // slid one window
    else if (window > e.window + 1) e = { window, curr: 0, prev: 0 };     // long gap → both empty
    this.windows.set(key, e); // always persist so allow() can find it
    const elapsedInWindow = now - window * this.windowMs;
    const prevWeight = 1 - elapsedInWindow / this.windowMs; // how much of prev window still overlaps
    return e.curr + e.prev * prevWeight;
  }

  allow(key: string, now: number): boolean {
    const est = this.estimate(key, now);
    if (est < this.limit) {
      const e = this.windows.get(key)!;
      e.curr += 1;
      return true;
    }
    return false;
  }

  peek(key: string, now: number) { return this.estimate(key, now).toFixed(2); }
}

function main() {
  const limiter = new SlidingWindowCounter(5, 1000); // 5 per rolling 1s

  log("═══ Boundary burst, again — now with just two counters per key ═══");
  let allowed = 0;
  for (let i = 0; i < 5; i++) if (limiter.allow("user:1", 950 + i)) allowed++;   // fills window 0
  log(`   5 requests near t=950ms → allowed ${allowed} (window 0 now has 5)`);

  // Just into window 1: the previous window still weighs ~95%, so estimate ≈ 4.75 + new.
  let allowedNext = 0;
  for (let i = 0; i < 5; i++) if (limiter.allow("user:1", 1000 + i)) allowedNext++;
  log(`   estimate just after boundary (t≈1000ms): ${limiter.peek("user:1", 1005)}`);
  log(`   → only ${allowedNext} allowed at the boundary (previous window still counts ~95%)`);
  log("   ✓ no 2×limit leak, and we stored 2 numbers instead of 10 timestamps.");

  log("");
  log("═══ Capacity returns gradually as the previous window's weight decays ═══");
  for (const t of [1300, 1600, 1900]) {
    const estBefore = limiter.peek("user:1", t); // the estimate the decision is based on
    const ok = limiter.allow("user:1", t);
    log(`   req @ ${t}ms → ${ok ? "✓ allowed" : "✗ throttled"} (estimate was ${estBefore})`);
  }

  log("");
  log("Two integers per key, no boundary burst, a few % approximate. This is the");
  log("sweet spot most real rate limiters ship — and it maps cleanly onto Redis (Phase 5).");
  process.exit(0);
}

main();
