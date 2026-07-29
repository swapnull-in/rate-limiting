/**
 * Phase 1 — FIXED WINDOW COUNTER: the simplest limiter, and its fatal flaw.
 * Run: npm run phase1
 *
 * Rate limiting is admission control: "at most N requests per T seconds per
 * key". The most obvious implementation:
 *
 *   window = floor(now / T)          // which T-second bucket are we in?
 *   count[key, window] += 1
 *   allow if count <= limit
 *
 * One integer per key per window. Cheap, easy. But it has a notorious bug: the
 * BOUNDARY BURST. The counter resets instantly at the window edge, so a client
 * can send `limit` requests at the very end of one window and `limit` more at
 * the very start of the next — 2×limit in a fraction of a second, all "legal".
 *
 * We use a virtual clock so the demo is exact and reproducible.
 */

import { log } from "../lib/log.ts";

class FixedWindowLimiter {
  private counts = new Map<string, { window: number; count: number }>();
  private limit: number;
  private windowMs: number;

  constructor(limit: number, windowMs: number) {
    this.limit = limit;
    this.windowMs = windowMs;
  }

  allow(key: string, now: number): boolean {
    const window = Math.floor(now / this.windowMs);
    const entry = this.counts.get(key);
    if (!entry || entry.window !== window) {
      this.counts.set(key, { window, count: 1 }); // new window → counter resets
      return true;
    }
    if (entry.count < this.limit) { entry.count++; return true; }
    return false;
  }
}

function main() {
  const limiter = new FixedWindowLimiter(5, 1000); // 5 requests per 1s

  log("═══ Normal use: 7 requests in one window, limit 5 ═══");
  for (let i = 1; i <= 7; i++) {
    const ok = limiter.allow("user:1", 100 * i); // all within window 0 (t=100..700ms)
    log(`   req ${i} @ ${100 * i}ms → ${ok ? "✓ allowed" : "✗ 429 throttled"}`);
  }

  log("");
  log("═══ The boundary burst: 5 at the end of window 0, 5 at the start of window 1 ═══");
  const l2 = new FixedWindowLimiter(5, 1000);
  let allowed = 0;
  for (let i = 0; i < 5; i++) if (l2.allow("user:2", 950 + i)) allowed++;   // t≈950–954ms (window 0)
  for (let i = 0; i < 5; i++) if (l2.allow("user:2", 1000 + i)) allowed++;  // t≈1000–1004ms (window 1)
  log(`   ${allowed} requests allowed in a ~50ms span across the 1s boundary`);
  log(`   → that's 2×limit (${allowed}) in well under one window. The counter reset betrayed us.`);

  log("");
  log("Fixed window is fine when a little bursting is OK. When it isn't, you need");
  log("a window that SLIDES (Phase 3/4) or a token bucket that meters smoothly (Phase 2).");
  process.exit(0);
}

main();
