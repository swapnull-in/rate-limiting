/**
 * Phase 2 — TOKEN BUCKET: smooth rate limiting with controlled bursts.
 * Run: npm run phase2
 *
 * The most-used limiter in practice (Stripe, AWS, nginx). The mental model:
 *
 *   A bucket holds up to CAPACITY tokens. Tokens drip in at a fixed REFILL RATE
 *   (e.g. 2/sec). Each request must take 1 token; if the bucket is empty, the
 *   request is throttled. You only store two numbers per key: token count and
 *   the last-refill timestamp — refill is computed lazily on each request.
 *
 * Why it's loved:
 *   • allows BURSTS up to `capacity` (a client that's been quiet can spend the
 *     accumulated tokens at once) — friendlier than a hard per-window cap,
 *   • but enforces a long-run AVERAGE of `refillRate` (you can't outrun the drip),
 *   • O(1) memory and time, no boundary-burst bug.
 *
 * (Leaky bucket is the mirror image: requests queue and drain at a fixed rate —
 * it smooths output; token bucket smooths input while permitting bursts.)
 */

import { log } from "../lib/log.ts";

class TokenBucket {
  private tokens: number;
  private lastRefill: number;
  private capacity: number;
  private refillPerMs: number;

  constructor(capacity: number, refillPerSec: number, now: number) {
    this.capacity = capacity;
    this.tokens = capacity; // start full
    this.refillPerMs = refillPerSec / 1000;
    this.lastRefill = now;
  }

  allow(now: number): boolean {
    // Lazily add the tokens that have dripped in since we last looked.
    const elapsed = now - this.lastRefill;
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.refillPerMs);
    this.lastRefill = now;

    if (this.tokens >= 1) { this.tokens -= 1; return true; }
    return false;
  }

  peek() { return this.tokens.toFixed(2); }
}

function main() {
  // capacity 5, refill 2 tokens/sec.
  const bucket = new TokenBucket(5, 2, 0);

  log("═══ Burst: 7 requests at t=0 (bucket starts with 5 tokens) ═══");
  for (let i = 1; i <= 7; i++) {
    const ok = bucket.allow(0);
    log(`   req ${i} @ 0ms → ${ok ? "✓ allowed" : "✗ throttled"}  (tokens left: ${bucket.peek()})`);
  }
  log("   → 5 allowed instantly (the burst), then throttled. Bucket is empty.");

  log("");
  log("═══ Recovery: tokens drip back at 2/sec ═══");
  for (const t of [500, 1000, 1500, 2000, 3000]) {
    const ok = bucket.allow(t);
    log(`   req @ ${t}ms → ${ok ? "✓ allowed" : "✗ throttled"}  (tokens: ${bucket.peek()})`);
  }
  log("   → roughly one request every 500ms — the 2/sec average, enforced smoothly.");

  log("");
  log("Two numbers per key, bursts allowed up to capacity, average pinned to the");
  log("refill rate, and no window-boundary bug. That's why token bucket is the default.");
  process.exit(0);
}

main();
