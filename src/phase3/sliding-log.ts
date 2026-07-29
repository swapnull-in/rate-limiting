/**
 * Phase 3 — SLIDING WINDOW LOG: exact rate limiting. Run: npm run phase3
 *
 * Fixed windows burst at the boundary (Phase 1). The sliding window LOG fixes it
 * exactly: keep the TIMESTAMP of every request in the last T seconds. On each
 * request, drop timestamps older than `now - T`, then allow if fewer than
 * `limit` remain. The window truly slides — "the last 1000ms from right now",
 * not "the current fixed bucket".
 *
 *   PRO: perfectly accurate. No boundary burst — ever.
 *   CON: memory is O(requests in the window) PER KEY. A hot key doing 10k rps
 *        stores 10k timestamps. That cost is why Phase 4 exists.
 *
 * This is the accuracy baseline the approximate methods are measured against.
 */

import { log } from "../lib/log.ts";

class SlidingWindowLog {
  private hits = new Map<string, number[]>(); // key → sorted timestamps
  private limit: number;
  private windowMs: number;

  constructor(limit: number, windowMs: number) {
    this.limit = limit;
    this.windowMs = windowMs;
  }

  allow(key: string, now: number): boolean {
    const cutoff = now - this.windowMs;
    const log_ = (this.hits.get(key) ?? []).filter((t) => t > cutoff); // evict old entries
    if (log_.length < this.limit) {
      log_.push(now);
      this.hits.set(key, log_);
      return true;
    }
    this.hits.set(key, log_);
    return false;
  }

  size(key: string) { return this.hits.get(key)?.length ?? 0; }
}

function main() {
  const limiter = new SlidingWindowLog(5, 1000); // 5 per rolling 1s

  log("═══ The boundary-burst attack that beat fixed windows ═══");
  let allowed = 0;
  for (let i = 0; i < 5; i++) if (limiter.allow("user:1", 950 + i)) allowed++;   // ~950ms
  for (let i = 0; i < 5; i++) if (limiter.allow("user:1", 1000 + i)) allowed++;  // ~1000ms
  log(`   same burst as Phase 1 → only ${allowed} allowed (the last 5 are within 1s of the first 5)`);
  log("   ✓ the sliding window counts the real last-1000ms, so no 2×limit leak.");

  log("");
  log("═══ As time passes, old timestamps expire and capacity returns ═══");
  log(`   at t=1000ms, stored timestamps for user:1: ${limiter.size("user:1")}`);
  const ok = limiter.allow("user:1", 2100); // 2100 - 1000 = 1100ms later; old ones expired
  log(`   request @ 2100ms → ${ok ? "✓ allowed" : "✗ throttled"} (earlier hits fell out of the window)`);

  log("");
  log("Exact, but it stores one timestamp per request per key. At scale that RAM");
  log("cost is real — which is exactly what the sliding window COUNTER approximates away.");
  process.exit(0);
}

main();
