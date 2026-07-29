/**
 * Phase 5 — DISTRIBUTED rate limiting on Redis, and why it must be ATOMIC.
 * Run: npm run phase5   (needs Redis)
 *
 * With 100 gateway nodes, an in-memory counter per node doesn't limit anything
 * globally — each node would allow the full quota. So the counter lives in a
 * shared store: Redis. But the naive approach hides a race:
 *
 *   count = GET key          ← node A reads 4, node B also reads 4
 *   if count < limit:        ← both think they're under the limit of 5
 *     INCR key               ← both increment → count 6, both ALLOWED
 *
 * Between the GET and the INCR, another request slipped in. Under load this
 * over-admits badly. The fix: make check-and-increment a SINGLE atomic operation.
 * Redis is single-threaded, so a Lua script runs to completion with nothing
 * interleaved — the check and the increment happen as one indivisible step.
 *
 * We reproduce the race with a non-atomic limiter, then fix it with Lua, then
 * show the 429 + Retry-After contract a real API returns.
 */

import Redis from "ioredis";
import { log } from "../lib/log.ts";

const redis = new Redis();
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const LIMIT = 5;

// ─── NAIVE: separate GET then INCR (race window in between) ─────────────────
async function naiveAllow(key: string): Promise<boolean> {
  const count = Number((await redis.get(key)) ?? 0);
  await sleep(5); // the race window: other requests read the same count here
  if (count < LIMIT) {
    await redis.incr(key);
    return true;
  }
  return false;
}

// ─── ATOMIC: one Lua script does check + incr + expire, indivisibly ─────────
const LUA = `
  local current = redis.call('INCR', KEYS[1])
  if current == 1 then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
  end
  if current > tonumber(ARGV[1]) then
    return 0
  end
  return 1
`;
async function atomicAllow(key: string, windowMs: number): Promise<boolean> {
  const ok = (await redis.eval(LUA, 1, key, String(LIMIT), String(windowMs))) as number;
  return ok === 1;
}

async function main() {
  // ─── Show the race ────────────────────────────────────────────────────────
  await redis.del("naive:user");
  log(`═══ NAIVE limiter, limit ${LIMIT}: fire 20 concurrent requests ═══`);
  const naiveResults = await Promise.all(Array.from({ length: 20 }, () => naiveAllow("naive:user")));
  const naiveAllowed = naiveResults.filter(Boolean).length;
  log(`   allowed ${naiveAllowed}/20  ❌ (should be ${LIMIT}; the GET→INCR race over-admitted)`);
  log(`   redis counter ended at ${await redis.get("naive:user")} — way past the limit`);

  // ─── Fix with atomic Lua ──────────────────────────────────────────────────
  await redis.del("atomic:user");
  log("");
  log(`═══ ATOMIC (Lua) limiter, limit ${LIMIT}: same 20 concurrent requests ═══`);
  const atomicResults = await Promise.all(Array.from({ length: 20 }, () => atomicAllow("atomic:user", 10_000)));
  const atomicAllowed = atomicResults.filter(Boolean).length;
  log(`   allowed ${atomicAllowed}/20  ✓ (exactly ${LIMIT}; check-and-incr was indivisible)`);

  // ─── The 429 + Retry-After contract ───────────────────────────────────────
  log("");
  log("═══ The client contract: 429 Too Many Requests + Retry-After ═══");
  const ttlMs = await redis.pttl("atomic:user");
  log("   a throttled response should carry headers so clients back off politely:");
  log(`      HTTP/1.1 429 Too Many Requests`);
  log(`      Retry-After: ${Math.ceil(ttlMs / 1000)}          (seconds until the window resets)`);
  log(`      X-RateLimit-Limit: ${LIMIT}`);
  log(`      X-RateLimit-Remaining: 0`);

  log("");
  log("Two laws worth remembering: (1) the check MUST be atomic — Lua on Redis, or");
  log("you over-admit under load; (2) when the limiter's Redis is down, fail-OPEN");
  log("for reads (don't take the site down) but fail-CLOSED for money/abuse endpoints.");

  redis.disconnect();
  process.exit(0);
}

main().catch((e) => { console.error("Phase 5 error:", e.message, "\nIs Redis running?"); process.exit(1); });
