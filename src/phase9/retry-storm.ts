/**
 * Phase 9 — THE RESPONSE CONTRACT & THE RETRY STORM: where a limiter AMPLIFIES
 * load instead of shedding it.
 * Run: node "src/phase9/retry-storm.ts"
 *
 * Every phase so far asked "should I admit this request?" and stopped there. But
 * a 429 is not the end of the story — it's a MESSAGE to the client, and what the
 * client does with it decides whether your limiter sheds load or drowns in it.
 *
 *   A rejection is part of the API CONTRACT. A well-behaved client that gets a
 *   429 backs OFF. A naive one retries IMMEDIATELY — and now every rejected
 *   request comes STRAIGHT BACK next tick, piling on top of the new traffic. The
 *   limiter keeps saying "no", the clients keep re-asking, and offered load
 *   CLIMBS while goodput stays pinned at the admit rate. The limiter you added to
 *   protect the service is now AMPLIFYING the load against it. That's a retry
 *   storm (a.k.a. a thundering herd), and rate limiting without a backoff
 *   contract manufactures it.
 *
 * The fix is a contract, not a smarter counter:
 *   • reject with 429 (client over quota) or 503 (server overloaded), and ALWAYS
 *     attach Retry-After + quota headers (X-RateLimit-Limit / -Remaining / -Reset)
 *     so a client can self-regulate instead of guessing,
 *   • clients honour it with EXPONENTIAL BACKOFF + JITTER — wait base·2^attempt
 *     ticks, times a random 0.5–1.5 so the herd doesn't re-synchronise and all
 *     retry on the same tick,
 *   • egregious offenders that ignore the contract get DROPPED (no response at
 *     all) rather than politely 429'd — a 429 still costs you a response,
 *   • and you SHED EARLY, SHED CHEAP: a rejection at the edge (WAF/CDN) costs
 *     almost nothing; the same rejection INSIDE the service already burned a
 *     connection, TLS, auth, and a thread. Under pressure, shed anonymous/read
 *     before authenticated/write.
 *
 * Everything runs on a VIRTUAL CLOCK of discrete integer ticks — no Date.now, no
 * wall-clock. A seeded mulberry32 supplies the backoff JITTER only, so the whole
 * simulation (storm and all) is byte-for-byte reproducible.
 */

import { log } from "../lib/log.ts";

// A tiny seeded PRNG (mulberry32). Deterministic: same seed → same stream. We use
// it ONLY to jitter backoff delays, never for admission decisions.
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return function () {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// The limiter: a token bucket metered in TICKS. It admits ~refillPerTick per tick
// and, on a rejection, can tell the client how long to wait (the Retry-After).
// ─────────────────────────────────────────────────────────────────────────────

class TokenBucket {
  private tokens: number;
  private lastRefill: number;
  private capacity: number;
  private refillPerTick: number;

  constructor(capacity: number, refillPerTick: number, now: number) {
    this.capacity = capacity;
    this.tokens = capacity; // start full
    this.refillPerTick = refillPerTick;
    this.lastRefill = now;
  }

  private refill(now: number): void {
    const elapsed = now - this.lastRefill;
    if (elapsed > 0) {
      this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.refillPerTick);
      this.lastRefill = now;
    }
  }

  allow(now: number): boolean {
    this.refill(now);
    if (this.tokens >= 1) { this.tokens -= 1; return true; }
    return false;
  }

  // The Retry-After the SERVER sends on a rejection: whole ticks until a token
  // is available. This is the floor a well-behaved client should never beat.
  retryAfter(now: number): number {
    this.refill(now);
    if (this.tokens >= 1) return 0;
    return Math.max(1, Math.ceil((1 - this.tokens) / this.refillPerTick));
  }

  remaining(now: number): number {
    this.refill(now);
    return Math.floor(this.tokens);
  }
}

// The shared workload for A vs B: a burst of NEW requests per tick, then quiet.
// Same offered demand feeds both policies so the ONLY variable is client behaviour.
const ARRIVALS = [6, 6, 6, 6, 6, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0]; // 36 real requests
const CAPACITY = 3;
const REFILL_PER_TICK = 3; // admit rate ≈ 3/tick — real demand (6/tick) outruns it
const MAX_TICKS = 60;      // safety bound for the drain loop
const MAX_ATTEMPTS = 12;   // a client that fails this many times gives up (dropped)

interface SimResult {
  offered: number[];       // offered load per tick (the storm signal)
  admitted: number;        // goodput
  rejectedWork: number;    // total rejected ATTEMPTS = wasted work
  dropped: number;         // clients that gave up
  peakOffered: number;
  ticksToDrain: number;
}

// ─────────────────────────────────────────────────────────────────────────────
// A) NAIVE IMMEDIATE RETRY — the storm. Every rejected request comes straight
//    back on the very next tick and stacks on top of the new arrivals.
// ─────────────────────────────────────────────────────────────────────────────

function simulateNaive(): SimResult {
  const bucket = new TokenBucket(CAPACITY, REFILL_PER_TICK, 0);
  const offered: number[] = [];
  let backlog = 0;         // rejected requests, ALL retrying next tick
  let admitted = 0, rejectedWork = 0, ticksToDrain = 0;

  for (let t = 0; t < MAX_TICKS; t++) {
    const arriving = ARRIVALS[t] ?? 0;
    const off = arriving + backlog; // new traffic + everyone we rejected last tick
    offered.push(off);
    if (off === 0 && t >= ARRIVALS.length) break;

    let admittedThisTick = 0;
    for (let i = 0; i < off; i++) if (bucket.allow(t)) admittedThisTick++;
    const rejected = off - admittedThisTick;

    admitted += admittedThisTick;
    rejectedWork += rejected;
    backlog = rejected;      // ← the bug: instant, undamped retry
    ticksToDrain = t;
  }

  return {
    offered, admitted, rejectedWork, dropped: 0,
    peakOffered: Math.max(...offered), ticksToDrain,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// B) EXPONENTIAL BACKOFF + JITTER — the fix. A rejected client waits
//    base·2^attempt·random(0.5,1.5) ticks (never less than the server's
//    Retry-After) before it dares ask again. Retries scatter across time instead
//    of landing all at once, so offered load stays near the real arrival rate.
// ─────────────────────────────────────────────────────────────────────────────

interface Client { attempts: number; nextRetry: number; }

function simulateBackoff(rng: () => number): SimResult {
  const bucket = new TokenBucket(CAPACITY, REFILL_PER_TICK, 0);
  const BASE = 1;
  const offered: number[] = [];
  const pending: Client[] = []; // clients waiting out a backoff, with their due tick
  let admitted = 0, rejectedWork = 0, dropped = 0, ticksToDrain = 0;
  let totalArrived = 0;
  const totalToArrive = ARRIVALS.reduce((a, b) => a + b, 0);

  for (let t = 0; t < MAX_TICKS; t++) {
    // Everyone due this tick: fresh arrivals + backed-off clients whose wait is up.
    const due: Client[] = [];
    const arriving = ARRIVALS[t] ?? 0;
    for (let i = 0; i < arriving; i++) due.push({ attempts: 0, nextRetry: t });
    totalArrived += arriving;
    for (let i = pending.length - 1; i >= 0; i--) {
      if (pending[i].nextRetry <= t) { due.push(pending[i]); pending.splice(i, 1); }
    }

    offered.push(due.length);
    // Done only when nothing is due, nothing is still backing off, and no more arrivals.
    if (due.length === 0 && pending.length === 0 && totalArrived >= totalToArrive) {
      ticksToDrain = t; break;
    }

    for (const c of due) {
      if (bucket.allow(t)) { admitted++; continue; }
      // Rejected. Server hands back a Retry-After; client backs off past it.
      rejectedWork++;
      c.attempts++;
      if (c.attempts >= MAX_ATTEMPTS) { dropped++; continue; } // gave up
      const jitter = 0.5 + rng();                        // random(0.5, 1.5)
      const backoff = Math.round(BASE * 2 ** c.attempts * jitter);
      const serverFloor = bucket.retryAfter(t);
      c.nextRetry = t + Math.max(1, backoff, serverFloor);
      pending.push(c);
    }
    ticksToDrain = t;
  }

  return {
    offered, admitted, rejectedWork, dropped,
    peakOffered: Math.max(...offered), ticksToDrain,
  };
}

// A compact per-tick bar so the SHAPE of offered load is visible at a glance.
function bar(n: number): string {
  return "█".repeat(n);
}

// ─────────────────────────────────────────────────────────────────────────────

function main() {
  log("═══ A) NAIVE IMMEDIATE RETRY — the storm (admit ≈ 3/tick, real demand 6/tick) ═══");
  const naive = simulateNaive();
  log("   tick │ offered load (new arrivals + every rejection retrying instantly)");
  naive.offered.forEach((o, t) => {
    if (o > 0 || t < ARRIVALS.length) log(`   ${String(t).padStart(4)} │ ${String(o).padStart(3)} ${bar(o)}`);
  });
  log(`   → real demand never exceeds 6/tick, yet offered load CLIMBS to ${naive.peakOffered}/tick.`);
  log(`     The limiter admitted ${naive.admitted} good requests but processed`);
  log(`     ${naive.rejectedWork} REJECTED attempts — wasted work it manufactured itself.`);
  log("     Every 429 bounced straight back and stacked on the next tick's arrivals.");

  log("");
  log("═══ B) EXPONENTIAL BACKOFF + JITTER — the fix (same arrivals, same bucket) ═══");
  const rng = mulberry32(0x9e3779b9); // fixed seed → deterministic jitter
  const backoff = simulateBackoff(rng);
  log("   tick │ offered load (new arrivals + only the FEW clients whose backoff expired)");
  backoff.offered.forEach((o, t) => {
    if (o > 0 || t < ARRIVALS.length) log(`   ${String(t).padStart(4)} │ ${String(o).padStart(3)} ${bar(o)}`);
  });
  log(`   → offered load stays bounded near the arrival rate (peak ${backoff.peakOffered}/tick, vs ${naive.peakOffered}`);
  log(`     under naive). Retries are SPREAD OUT by base·2^attempt·jitter, so the same`);
  log(`     ${backoff.admitted} good requests get through while wasted work drops to ${backoff.rejectedWork} rejected`);
  log(`     attempts (naive burned ${naive.rejectedWork}). The system DRAINS instead of amplifying.`);
  log("     A well-behaved client also never beats the server's Retry-After floor.");

  log("");
  log("═══ C) THE RESPONSE CONTRACT — what the server actually sends back ═══");
  const contractBucket = new TokenBucket(CAPACITY, REFILL_PER_TICK, 0);
  for (let i = 0; i < CAPACITY; i++) contractBucket.allow(2); // drain it at tick 2
  const ra = contractBucket.retryAfter(2);
  log("   A polite rejection is a full HTTP response with headers a client can obey:");
  log("      HTTP/1.1 429 Too Many Requests");
  log(`      Retry-After: ${ra}                 ← wait this long (server's floor)`);
  log(`      X-RateLimit-Limit: ${REFILL_PER_TICK}            ← your sustained quota`);
  log(`      X-RateLimit-Remaining: ${contractBucket.remaining(2)}        ← tokens left right now`);
  log(`      X-RateLimit-Reset: 3             ← tick a token frees up`);
  log("   • 429 = the CLIENT is over its quota.  503 + Retry-After = the SERVER is");
  log("     overloaded (shed load, come back later). Both hand back Retry-After.");
  log("   • An egregious offender that IGNORES the contract — hammering through every");
  log("     backoff — is not owed a polite 429. You DROP it (no response, connection");
  log("     reset / blackhole): answering it at all just spends resources on abuse.");

  log("");
  log("═══ D) SHED EARLY, SHED CHEAP — where the rejection happens is the cost ═══");
  const EDGE_COST = 1;     // WAF/CDN: match a rule, return 429. Almost free.
  const SERVICE_COST = 20; // in-process: connection + TLS + auth + a thread already spent
  const rejected = naive.rejectedWork;
  log("   A rejected request is not free — and its price depends on WHERE it dies:");
  log(`      at the EDGE  (WAF/CDN)     ≈ ${EDGE_COST.toString().padStart(2)} unit  per reject`);
  log(`      INSIDE the service        ≈ ${SERVICE_COST.toString().padStart(2)} units per reject  (TLS + auth + thread already burned)`);
  log(`   The storm above produced ${rejected} rejected attempts. Rejecting that SAME volume:`);
  log(`      at the edge   → ${(rejected * EDGE_COST).toString().padStart(5)} units`);
  log(`      in the service→ ${(rejected * SERVICE_COST).toString().padStart(5)} units   (${SERVICE_COST / EDGE_COST}× more, for identical 'no's)`);
  log(`   → shedding at the edge is ${SERVICE_COST / EDGE_COST}× cheaper. And backoff (B) shrinks the`);
  log(`     rejected volume itself, so the in-service bill falls from ${naive.rejectedWork * SERVICE_COST} to ${backoff.rejectedWork * SERVICE_COST} units.`);

  log("");
  log("   Degrade by PRIORITY — when you must shed, shed the cheap traffic first.");
  // A tick where 6 requests contend for 3 tokens: keep authed/write, drop anon/read.
  const contenders = [
    { name: "anon read   ", priority: 1 },
    { name: "anon read   ", priority: 1 },
    { name: "authed read ", priority: 2 },
    { name: "authed write", priority: 3 },
    { name: "anon read   ", priority: 1 },
    { name: "authed write", priority: 3 },
  ];
  const ranked = [...contenders].sort((a, b) => b.priority - a.priority); // high priority first
  ranked.forEach((c, i) => {
    const admitted = i < CAPACITY;
    log(`      ${c.name} (p${c.priority}) → ${admitted ? "✓ admitted" : "✗ shed (429)"}`);
  });
  log(`   → ${CAPACITY} tokens, 6 contenders: authed/write survive, anonymous/read is shed first.`);

  log("");
  log("TAKEAWAY: a 429 without client backoff makes the limiter AMPLIFY load — naive");
  log("immediate retries pile onto new traffic and offered load climbs into a storm");
  log(`(here ${naive.peakOffered}/tick against a real demand of 6). The contract is 429/503 + Retry-After`);
  log("+ quota headers (X-RateLimit-Limit/Remaining/Reset) so clients back off with");
  log("EXPONENTIAL BACKOFF + JITTER — and egregious offenders get DROPPED, not politely");
  log("rejected. Shed EARLY and CHEAP: a rejection at the edge costs ~nothing; inside the");
  log("service it already spent TLS, auth, and a thread. Under pressure, shed anonymous/");
  log("read before authenticated/write.");
  process.exit(0);
}

main();
