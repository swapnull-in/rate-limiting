/**
 * Phase 7 — DISTRIBUTED ENFORCEMENT: the three strategies for one global limit.
 * Run: node "src/phase7/distributed-strategies.ts"
 *
 * One node is trivial — a single in-memory bucket IS the limiter. The interview
 * question is how you hold a limit of L across a FLEET of N gateway nodes, where
 * a load balancer sprays requests across the fleet (never perfectly evenly). No
 * node sees the whole stream, so no node can enforce the global limit alone.
 *
 * Phase 5 already built one answer: a central Redis store where a Lua script runs
 * the check-and-decrement ATOMICALLY. That's exact — but it puts a synchronous
 * network hop on EVERY request. This phase puts the three strategies side by side
 * on the SAME request stream and measures the trade they make: accuracy vs cost,
 * where "cost" is a Redis trip — a node touching the central store.
 *
 *   A) CENTRAL STORE (exact, atomic) — every request runs the limiter math in the
 *      shared store. Exactly L allowed (correct), but one Redis trip PER REQUEST:
 *      Redis sits on the hot path, adding latency to every call.
 *
 *   B) LOCAL DIVIDED QUOTA — hand each node L/N and let it enforce locally, zero
 *      central trips. Free and instant — but the LB never splits evenly, so a HOT
 *      node burns through its L/N and starts FALSE-THROTTLING while COLD nodes sit
 *      on unused quota. Total admitted drops BELOW L: under-admission.
 *
 *   C) LOCAL + BATCH-BORROWING (the production answer) — a node borrows a BATCH of
 *      tokens from the central store in one trip, hands them out locally, and only
 *      goes back when the batch runs dry. ~1 trip per batch instead of per request.
 *      Near-correct, but the store hands out WHOLE batches, so the last batch can
 *      tip the total a few % over L: accuracy becomes EVENTUAL, not exact.
 *
 * We SIMULATE the whole thing in-process — no Redis, no network — so it's
 * deterministic: a virtual clock (times passed explicitly) and a seeded PRNG for
 * request→node assignment. Same stream, same skew, three limiters, three answers.
 *
 * TAKEAWAY: exact global limits (a central atomic store) cost a synchronous hop on
 * EVERY request. Local divided quota is free but false-throttles under LB skew.
 * Local + batch-borrowing is the production answer: ~1 store trip per batch,
 * near-correct with a few % overshoot. Rate limits protect CAPACITY — they aren't
 * billing invariants — so accept the overshoot. Unless it IS a quota/billing limit
 * (money on the line), and then you pay for the exact central store. Across
 * regions, don't limit globally: limit per region against a regional quota.
 */

import { log } from "../lib/log.ts";

// ─── Deterministic PRNG (mulberry32) — no Math.random on the hot path ───────
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ─── The scenario ───────────────────────────────────────────────────────────
const L = 100; // global limit: 100 requests admitted per window
const N = 5; // gateway nodes behind the load balancer
const BATCH = 15; // tokens a node borrows per central trip (option C)
const REQUESTS = 175; // stream length — well past L, so a good share should throttle

// LB skew: node 0 is "hot", node 4 is "cold". Weights sum to 1. Real load
// balancers hash on client IP / sticky sessions and never split evenly.
const WEIGHT = [0.4, 0.25, 0.15, 0.12, 0.08];

type Req = { id: number; node: number; t: number };

// Build ONE request stream (id, assigned node, virtual arrival time) and reuse
// it for all three strategies so the only variable is the limiter.
function buildStream(seed: number): Req[] {
  const rand = mulberry32(seed);
  const cum: number[] = [];
  let acc = 0;
  for (const w of WEIGHT) { acc += w; cum.push(acc); }

  const stream: Req[] = [];
  for (let i = 0; i < REQUESTS; i++) {
    const r = rand();
    let node = N - 1;
    for (let n = 0; n < cum.length; n++) { if (r < cum[n]) { node = n; break; } }
    // Virtual clock: spread the stream across a single 1000ms window.
    const t = Math.floor((i * 1000) / REQUESTS);
    stream.push({ id: i, node, t });
  }
  return stream;
}

function loadPerNode(stream: Req[]): number[] {
  const counts = new Array(N).fill(0);
  for (const req of stream) counts[req.node]++;
  return counts;
}

// ─── OPTION A — CENTRAL STORE (exact, atomic) ───────────────────────────────
// Every request touches the shared store. The store is a plain object standing
// in for Redis; the check-and-increment is one indivisible step (Phase 5's Lua).
function optionCentral(stream: Req[]): { allowed: number; trips: number } {
  const store = { count: 0 }; // the shared counter
  let allowed = 0;
  let trips = 0;
  for (const _req of stream) {
    trips++; // a synchronous hop to Redis on EVERY request
    if (store.count < L) { store.count++; allowed++; }
  }
  return { allowed, trips };
}

// ─── OPTION B — LOCAL DIVIDED QUOTA (free, but skew-blind) ───────────────────
// Each node gets L/N tokens and enforces alone. Zero central trips. But the LB
// skew means hot nodes exhaust L/N and throttle while cold nodes hoard quota.
function optionDivided(stream: Req[]): {
  allowed: number; trips: number; falseThrottles: number; wastedQuota: number;
} {
  const share = Math.floor(L / N); // L/N per node
  const nodes = Array.from({ length: N }, () => ({ used: 0 }));
  let allowed = 0;
  let falseThrottles = 0;
  for (const req of stream) {
    const node = nodes[req.node];
    if (node.used < share) { node.used++; allowed++; }
    else { falseThrottles++; } // throttled locally — even though the FLEET has room
  }
  // Quota that sat idle on cold nodes while hot nodes turned users away.
  const wastedQuota = nodes.reduce((sum, node) => sum + (share - node.used), 0);
  return { allowed, trips: 0, falseThrottles, wastedQuota };
}

// ─── OPTION C — LOCAL + BATCH-BORROWING (the production answer) ──────────────
// A shared pool of L tokens. A node borrows a whole BATCH in one trip, serves it
// locally, and only trips again when the batch is empty. The pool hands out WHOLE
// batches while any capacity remains — so the last batch can push the total a few
// tokens past L. That overshoot is the price of ~1 trip per batch.
function optionBorrow(stream: Req[]): { allowed: number; trips: number; overshoot: number } {
  const pool = { granted: 0, cap: L, exhausted: false }; // the shared store
  const nodes = Array.from({ length: N }, () => ({ local: 0 })); // borrowed tokens on hand

  // One central trip: hand out a full batch while any capacity is left (cheap —
  // no exact-remainder math on the hot path), else signal the pool is drained.
  function borrow(): number {
    if (pool.granted < pool.cap) { pool.granted += BATCH; return BATCH; }
    pool.exhausted = true;
    return 0;
  }

  let allowed = 0;
  let trips = 0;
  for (const req of stream) {
    const node = nodes[req.node];
    if (node.local === 0 && !pool.exhausted) {
      trips++; // go to the store — but only once per batch, not per request
      node.local = borrow();
    }
    if (node.local > 0) { node.local--; allowed++; }
    // else: local cache empty and pool known-drained → throttle with no trip.
  }
  const overshoot = Math.max(0, allowed - L); // how far actual admits ran past L
  return { allowed, trips, overshoot };
}

function pad(s: string | number, w: number): string {
  return String(s).padEnd(w);
}

function main() {
  const stream = buildStream(0xc0ffee);
  const load = loadPerNode(stream);

  log(`═══ Scenario: global limit L=${L}/window, N=${N} gateway nodes, ${REQUESTS} requests ═══`);
  log(`   LB skew (requests landing on each node): [${load.join(", ")}]`);
  log(`   node 0 is hot, node ${N - 1} is cold — the load balancer never splits evenly.`);

  // ─── A) CENTRAL STORE ──────────────────────────────────────────────────────
  log("");
  log("═══ A) CENTRAL STORE — every request runs the limiter atomically in Redis ═══");
  const a = optionCentral(stream);
  log(`   allowed ${a.allowed}/${L}  ✓ EXACT — the shared counter is the single source of truth`);
  log(`   redis trips: ${a.trips}  (one synchronous hop PER REQUEST — Redis on the hot path)`);

  // ─── B) LOCAL DIVIDED QUOTA ─────────────────────────────────────────────────
  log("");
  log(`═══ B) LOCAL DIVIDED QUOTA — each node enforces L/N = ${Math.floor(L / N)}, zero trips ═══`);
  const b = optionDivided(stream);
  log(`   allowed ${b.allowed}/${L}  ✗ UNDER-admits — ${L - b.allowed} short of the global limit`);
  log(`   redis trips: ${b.trips}  (nothing shared — free and instant)`);
  log(`   but ${b.falseThrottles} requests FALSE-THROTTLED on hot nodes while ${b.wastedQuota} quota sat`);
  log(`   idle on cold nodes — skew turns a global limit of ${L} into an effective ${b.allowed}.`);

  // ─── C) LOCAL + BATCH-BORROWING ─────────────────────────────────────────────
  log("");
  log(`═══ C) LOCAL + BATCH-BORROWING — borrow ${BATCH} tokens per trip, serve locally ═══`);
  const c = optionBorrow(stream);
  const overPct = ((c.overshoot / L) * 100).toFixed(0);
  log(`   allowed ${c.allowed}/${L}  ≈ near-correct — ${c.overshoot} over (${overPct}%), the last batch overshot`);
  log(`   redis trips: ${c.trips}  (~1 per batch — vs ${a.trips} for the central store: ${(a.trips / c.trips).toFixed(0)}× fewer)`);
  log(`   accuracy is now EVENTUAL: the pool hands out whole batches, so the tail spills over.`);

  // ─── D) SUMMARY TABLE ───────────────────────────────────────────────────────
  log("");
  log("═══ The trade: exact + slow  vs  fast + approximate ═══");
  log(`   ${pad("strategy", 24)}${pad("allowed / limit", 18)}${pad("redis trips", 14)}accuracy`);
  log(`   ${pad("─".repeat(22), 24)}${pad("─".repeat(16), 18)}${pad("─".repeat(12), 14)}${"─".repeat(20)}`);
  log(`   ${pad("A central store", 24)}${pad(`${a.allowed} / ${L}`, 18)}${pad(a.trips, 14)}exact (but a hop/req)`);
  log(`   ${pad("B divided quota", 24)}${pad(`${b.allowed} / ${L}`, 18)}${pad(b.trips, 14)}under-admits (skew)`);
  log(`   ${pad("C batch-borrowing", 24)}${pad(`${c.allowed} / ${L}`, 18)}${pad(c.trips, 14)}~${overPct}% over (eventual)`);

  log("");
  log("Rate limits protect CAPACITY — they aren't billing invariants — so a few % over");
  log("is fine: ship batch-borrowing. If it IS a quota/billing limit (money on the line),");
  log("pay for the exact central store. Across regions, limit PER region, not globally.");
  process.exit(0);
}

main();
