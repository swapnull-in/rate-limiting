/**
 * Phase 6 — LEAKY BUCKET & GCRA: the last two members of the algorithm family.
 * Run: node "src/phase6/leaky-gcra.ts"
 *
 * Phases 1–4 covered fixed-window, token-bucket, sliding-log, sliding-counter.
 * Two classics remain, and they're two views of the SAME idea:
 *
 *   LEAKY BUCKET (as a queue) — requests pour into a bucket (a FIFO queue) that
 *   has a hole in the bottom draining at a fixed rate (e.g. 2/sec). The queue has
 *   a MAX DEPTH; requests arriving when it's full spill over and are dropped. The
 *   OUTPUT is perfectly smooth — one request leaves every emission interval, no
 *   matter how bursty the input. That smoothness is the whole point... and the
 *   whole cost: a queued request has to WAIT (added latency), and a burst that
 *   overflows the depth gets dropped. Leaky bucket is burst-HOSTILE.
 *
 *   GCRA (Generic Cell Rate Algorithm) — "a leaky bucket as a single TIMESTAMP."
 *   Instead of a queue, store ONE value per key: the TAT (Theoretical Arrival
 *   Time) — the earliest time the next request "should" arrive under the sustained
 *   rate. No queue, no background refill loop, no token counter. Just one number.
 *   This is what Redis-Cell (CL.THROTTLE) implements, and it behaves EXACTLY like
 *   a token bucket (bursts up to a tolerance, then metered to the rate) — the same
 *   admit/deny decisions, but with minimal state.
 *
 * Everything below runs on a VIRTUAL CLOCK: `now` is passed in as a number (ms),
 * so there's no Date.now / Math.random and the output is fully deterministic.
 */

import { log } from "../lib/log.ts";

// ─────────────────────────────────────────────────────────────────────────────
// A) LEAKY BUCKET as a QUEUE — smooths OUTPUT, at the cost of latency.
// ─────────────────────────────────────────────────────────────────────────────

class LeakyBucketQueue {
  private queue: number[];      // effective completion (drain) time of each queued request
  private maxDepth: number;     // how many requests can wait before we drop
  private drainIntervalMs: number; // one request leaves every this-many ms
  private nextDrainAt: number;  // the earliest time the next request may drain

  constructor(maxDepth: number, drainPerSec: number, now: number) {
    this.queue = [];
    this.maxDepth = maxDepth;
    this.drainIntervalMs = 1000 / drainPerSec;
    this.nextDrainAt = now;
  }

  // Remove any requests whose drain time has already passed — they've "left".
  private evict(now: number): void {
    while (this.queue.length > 0 && this.queue[0] < now) {
      this.queue.shift();
    }
  }

  // Try to enqueue a request. Returns its effective completion time, or null if dropped.
  offer(now: number): number | null {
    this.evict(now);

    if (this.queue.length >= this.maxDepth) {
      return null; // bucket full → overflow → dropped
    }

    // The next slot drains at max(now, nextDrainAt); each admit pushes the drain
    // clock forward by one interval, producing a perfectly smooth output cadence.
    const drainAt = Math.max(now, this.nextDrainAt);
    this.nextDrainAt = drainAt + this.drainIntervalMs;
    this.queue.push(drainAt);
    return drainAt;
  }

  depth(now: number): number {
    this.evict(now);
    return this.queue.length;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// B) GCRA — the leaky bucket as ONE timestamp (the TAT). No queue, no refill.
// ─────────────────────────────────────────────────────────────────────────────

class GCRA {
  private tat: number;          // Theoretical Arrival Time — the ONLY stored state
  private emissionInterval: number; // T = 1/rate: the sustained spacing between requests
  private tolerance: number;    // τ: how far ahead of the TAT a burst may run

  constructor(ratePerSec: number, burst: number, now: number) {
    this.emissionInterval = 1000 / ratePerSec; // T, in ms
    // Burst tolerance τ = (burst - 1) × T — how much "early arrival" we forgive.
    // burst = how many requests may fire back-to-back before metering kicks in.
    this.tolerance = (burst - 1) * this.emissionInterval;
    this.tat = now; // nothing owed yet
  }

  // Returns { ok, retryAfter }. All state is the single `tat` number.
  allow(now: number): { ok: boolean; retryAfter: number } {
    const T = this.emissionInterval;
    const allowAt = this.tat - this.tolerance; // earliest `now` we'll admit at

    if (now < allowAt) {
      // Too soon: even the burst allowance is used up. Tell them when to retry.
      return { ok: false, retryAfter: allowAt - now };
    }

    // Admit. Advance the TAT by one emission interval from max(now, tat):
    // an idle client resets to `now` (no free hoarding beyond τ), a busy one
    // keeps stacking T's until it bumps into the tolerance ceiling.
    this.tat = Math.max(now, this.tat) + T;
    return { ok: true, retryAfter: 0 };
  }

  peekTat(): string {
    return `${this.tat.toFixed(0)}ms`;
  }
}

// ─────────────────────────────────────────────────────────────────────────────

function main() {
  log("═══ A) LEAKY BUCKET (queue): a burst of 6 arrives at t=0, drains 2/sec ═══");
  // maxDepth 4, drain 2/sec → one request leaves every 500ms.
  const leaky = new LeakyBucketQueue(4, 2, 0);
  for (let i = 1; i <= 6; i++) {
    const drainAt = leaky.offer(0);
    if (drainAt === null) {
      log(`   req ${i} @ 0ms → ✗ dropped (queue full, depth ${leaky.depth(0)})`);
    } else {
      const wait = drainAt - 0;
      log(`   req ${i} @ 0ms → ✓ queued, drains @ ${drainAt}ms  (waits ${wait}ms)`);
    }
  }
  log("   → 4 queue and drain one-per-500ms (0, 500, 1000, 1500ms); the last 2 overflow.");
  log("   Output is perfectly SMOOTH regardless of the bursty input — but req 4 waited");
  log("   1500ms. Latency is the price, and anything past the depth is dropped. Burst-hostile.");

  log("");
  log("═══ B) GCRA: same 2/sec, burst tolerance = 3, using ONE stored value (TAT) ═══");
  const gcra = new GCRA(2, 3, 0);
  const burst = [0, 0, 0, 0, 0]; // 5 requests all at t=0
  for (let i = 0; i < burst.length; i++) {
    const r = gcra.allow(burst[i]);
    const verdict = r.ok
      ? `✓ allowed`
      : `✗ denied (retry after ${r.retryAfter.toFixed(0)}ms)`;
    log(`   req ${i + 1} @ ${burst[i]}ms → ${verdict}  (TAT now ${gcra.peekTat()})`);
  }
  log("   → 3 allowed instantly (the burst tolerance), then metered: the 4th/5th are");
  log("     denied because now < TAT − τ. No queue, no refill loop — just the TAT moved.");

  log("");
  log("═══ B, continued) let the sustained rate catch up — one every 500ms ═══");
  for (const t of [500, 1000, 1500]) {
    const r = gcra.allow(t);
    const verdict = r.ok ? "✓ allowed" : `✗ denied (retry ${r.retryAfter.toFixed(0)}ms)`;
    log(`   req @ ${t}ms → ${verdict}  (TAT now ${gcra.peekTat()})`);
  }
  log("   → once the clock advances into the allowance, requests pass at the 2/sec rate.");
  log("   This is exactly token-bucket behavior (bursts up to tolerance, then metered) —");
  log("   but stored as a SINGLE timestamp. This is what Redis-Cell / CL.THROTTLE does.");

  log("");
  log("═══ C) CONTRAST: leaky bucket vs token bucket vs GCRA ═══");
  log("   ┌───────────────┬──────────────────┬──────────────┬─────────────────────┐");
  log("   │ algorithm     │ shapes           │ bursts?      │ state per key       │");
  log("   ├───────────────┼──────────────────┼──────────────┼─────────────────────┤");
  log("   │ leaky bucket  │ OUTPUT (smooth)  │ hostile      │ a queue + drain time│");
  log("   │ token bucket  │ INPUT            │ allows them  │ tokens + timestamp  │");
  log("   │ GCRA          │ INPUT (≡ token)  │ allows them  │ ONE timestamp (TAT) │");
  log("   └───────────────┴──────────────────┴──────────────┴─────────────────────┘");
  log("   • Leaky bucket smooths OUTPUT by draining a queue at a fixed rate: perfectly");
  log("     smooth, but adds latency (requests wait) and drops bursts past the depth.");
  log("   • Token bucket (phase 2) smooths INPUT while permitting bursts up to capacity.");
  log("   • GCRA makes the SAME admit/deny decisions as token bucket — GCRA ≡ token");
  log("     bucket — but with minimal state: one timestamp, no refill loop, no queue.");

  log("");
  log("TAKEAWAY: leaky bucket smooths OUTPUT by draining a queue at a fixed rate");
  log("(perfectly smooth, but it adds latency and hates bursts). GCRA is the elegant");
  log("form — a leaky bucket as a single timestamp (TAT), what Redis-Cell implements —");
  log("giving token-bucket behavior (bursts up to a tolerance, then the sustained rate)");
  log("with just ONE stored value and no background refill. Token bucket is still the");
  log("default; GCRA is its memory-minimal refinement.");
  process.exit(0);
}

main();
