/**
 * Phase 11 — KEYING & LAYERED LIMITS: a request must pass a STACK of buckets.
 * Run: node "src/phase11/layered-limits.ts"
 *
 * A real request isn't checked against one limiter — it runs a GAUNTLET. Each
 * incoming request carries several identities: {ip, apiKey, tenant, endpoint}.
 * The gateway keeps a separate token bucket keyed by EACH of those, and the
 * request is admitted only if EVERY applicable bucket has a token to spare.
 * The layers are checked IN ORDER, tightest/cheapest first, and the FIRST layer
 * to deny wins — it returns 429 and names ITSELF so the response header and the
 * on-call engineer both know which limit bit:
 *
 *   1. per-IP        (edge / abuse)     — tightest; catches scrapers & cred-stuffing
 *   2. per-API-key   (gateway / tier)   — the product tier: free vs pro is just a rate
 *   3. per-tenant    (fairness)         — one tenant can't starve others (noisy neighbor)
 *   4. per-endpoint  (cost)             — expensive routes get tighter caps
 *   5. global        (capacity floor)   — the whole service's ceiling
 *
 * Two structural ideas this phase drives home:
 *
 *   FAIRNESS (§5). The per-tenant bucket is the noisy-neighbor guard. When
 *   tenant A floods the service, A is throttled at ITS tenant bucket — tenant B's
 *   bucket is untouched, so B's traffic still sails through. Without a per-tenant
 *   layer, A's burst would drain the shared global bucket and starve everyone.
 *
 *   THE HOT KEY / WHALE (§6). One API key that is ~50% of all traffic is a HOT
 *   PARTITION — every one of its requests hammers the SAME bucket (one shard,
 *   one lock, one Redis key). Fix: SPLIT the whale's bucket into N sub-buckets
 *   (route each request to sub-bucket = hash(reqId) % N; keep sum-of-caps ≤ limit)
 *   so the load spreads across N shards instead of pounding one — or hand the
 *   whale DEDICATED capacity so it can't affect anyone else.
 *
 * Note: these rules belong in HOT-RELOADABLE gateway/sidecar config — a table of
 * (key pattern → algorithm / rate / burst) owned by the platform team. Services
 * should NOT hand-roll their own limiters; tiers just stack as different rows
 * (free vs pro is the same per-API-key bucket with a different rate).
 *
 * Deterministic throughout: a virtual clock (`now` in ms) and a seeded hash —
 * no Date.now, no Math.random. (One subtlety we keep honest: a layer consumes
 * its token as it's checked, so a request denied at layer 4 has already spent a
 * token at layers 1–3. Real gateways accept this, or reserve-then-commit.)
 *
 * TAKEAWAY: limits STACK — a request must pass ALL applicable buckets in order
 * (per-IP → per-API-key/tier → per-tenant fairness → per-endpoint → global
 * floor), and the first to deny wins and names itself in the 429. Per-tenant
 * limits are what stop a noisy neighbor from starving others. A whale API key is
 * a hot partition — split its bucket into sub-buckets or isolate it to dedicated
 * capacity. Rules live in gateway-owned hot-reloadable config, not hand-rolled
 * per service.
 */

import { log } from "../lib/log.ts";

// ─── The token bucket (same lazy-refill model as phase 2) ───────────────────
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
    const elapsed = now - this.lastRefill;
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.refillPerMs);
    this.lastRefill = now;
    if (this.tokens >= 1) { this.tokens -= 1; return true; }
    return false;
  }
}

// ─── The chain: one keyed bucket-family per layer, checked in order ──────────
type Req = { ip: string; apiKey: string; tenant: string; endpoint: string };
type LayerSpec = { name: string; keyOf: (r: Req) => string; capacity: number; refillPerSec: number };
type Decision = { allowed: boolean; deniedBy: string | null };

class LimiterChain {
  private specs: LayerSpec[];
  private families: Map<string, TokenBucket>[]; // one bucket-per-key map per layer

  constructor(specs: LayerSpec[]) {
    this.specs = specs;
    this.families = specs.map(() => new Map());
  }

  // Reuse the bucket for a key across requests; the first empty layer denies.
  admit(r: Req, now: number): Decision {
    for (let i = 0; i < this.specs.length; i++) {
      const spec = this.specs[i];
      const key = spec.keyOf(r);
      const family = this.families[i];
      let bucket = family.get(key);
      if (!bucket) {
        bucket = new TokenBucket(spec.capacity, spec.refillPerSec, now);
        family.set(key, bucket);
      }
      if (!bucket.allow(now)) return { allowed: false, deniedBy: `${spec.name}[${key}]` };
    }
    return { allowed: true, deniedBy: null };
  }
}

// The gateway's hot-reloadable rule table (key pattern → cap/rate), factory'd
// so each demo gets a fresh, independent config profile.
function buildChain(overrides: Partial<Record<string, number>> = {}): LimiterChain {
  const cap = (name: string, dflt: number) => overrides[name] ?? dflt;
  return new LimiterChain([
    { name: "per-IP",       keyOf: (r) => r.ip,       capacity: cap("per-IP", 3),      refillPerSec: 0 },
    { name: "per-API-key",  keyOf: (r) => r.apiKey,   capacity: cap("per-API-key", 100), refillPerSec: 0 },
    { name: "per-tenant",   keyOf: (r) => r.tenant,   capacity: cap("per-tenant", 4),   refillPerSec: 0 },
    { name: "per-endpoint", keyOf: (r) => r.endpoint, capacity: cap("per-endpoint", 100), refillPerSec: 0 },
    { name: "global",       keyOf: () => "*",         capacity: cap("global", 8),       refillPerSec: 0 },
  ]);
}

// Deterministic 32-bit hash (FNV-1a) — used to shard the whale, no Math.random.
function hash32(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193); }
  return h >>> 0;
}

function fmt(d: Decision): string {
  return d.allowed ? "✓ 200 allowed" : `✗ 429 denied  → hit ${d.deniedBy}`;
}

function main() {
  // ─── A) LAYERED ADMISSION: different requests denied at different layers ────
  log("═══ A) Layered admission — one stack, the first empty layer wins ═══");
  const chain = buildChain(); // caps: IP=3, tenant=4, global=8
  const t = 0;

  log("   baseline — a normal request passes every layer:");
  log(`      ${fmt(chain.admit({ ip: "9.9.9.9", apiKey: "pro-1", tenant: "acme", endpoint: "/list" }, t))}`);

  log("   scraper — same IP hammering (per-IP cap is 3), unique keys/tenant so only IP bites:");
  for (let i = 1; i <= 4; i++) {
    const d = chain.admit({ ip: "6.6.6.6", apiKey: `k${i}`, tenant: "globex", endpoint: "/list" }, t);
    log(`      req ${i} from 6.6.6.6 → ${fmt(d)}`);
  }

  log("   noisy tenant — 'acme' fills its fairness cap (4) from many IPs; only tenant bites:");
  for (let i = 1; i <= 4; i++) {
    const d = chain.admit({ ip: `10.0.0.${i}`, apiKey: `acme-${i}`, tenant: "acme", endpoint: "/list" }, t);
    log(`      acme req ${i} → ${fmt(d)}`);
  }

  log("   global floor — fully fresh keys each time, so only the global ceiling (8) can bite:");
  for (let i = 1; i <= 2; i++) {
    const d = chain.admit({ ip: `172.16.0.${i}`, apiKey: `fresh-${i}`, tenant: `t-${i}`, endpoint: `/e${i}` }, t);
    log(`      fresh req ${i} → ${fmt(d)}`);
  }
  log("   → three requests, three DIFFERENT layers named the 429. That's the debugging win.");

  // ─── B) FAIRNESS: a noisy neighbor can't starve the quiet one ───────────────
  log("");
  log("═══ B) Fairness — tenant A's burst does NOT starve tenant B ═══");
  // Tenant cap 4; IP/global set high so ONLY the per-tenant layer is the constraint.
  const fair = buildChain({ "per-IP": 100, "per-tenant": 4, global: 1000 });
  log("   tenant A bursts 8 requests (per-tenant cap = 4):");
  let aOk = 0;
  for (let i = 1; i <= 8; i++) {
    const d = fair.admit({ ip: `a.${i}`, apiKey: `A-${i}`, tenant: "A", endpoint: "/list" }, t);
    if (d.allowed) aOk++;
    if (i <= 5) log(`      A req ${i} → ${fmt(d)}`);
  }
  log(`      … A got ${aOk}/8 through — throttled at its OWN tenant bucket.`);
  log("   meanwhile tenant B sends 3 requests — B's bucket is untouched:");
  for (let i = 1; i <= 3; i++) {
    const d = fair.admit({ ip: `b.${i}`, apiKey: `B-${i}`, tenant: "B", endpoint: "/list" }, t);
    log(`      B req ${i} → ${fmt(d)}`);
  }
  log("   → B sails through. Per-tenant limits contain the blast radius to the noisy tenant.");

  // ─── C) THE WHALE / HOT KEY: split one hot bucket into sub-buckets ──────────
  log("");
  log("═══ C) The whale — one API key is ~50% of traffic, a HOT partition ═══");
  const WHALE_REQS = 40;
  const LIMIT = 12; // the whale's allowance for this window

  // Naive: a SINGLE bucket. Every request pounds the same shard/lock/Redis key.
  const single = new TokenBucket(LIMIT, 0, 0);
  let singleOk = 0;
  for (let i = 0; i < WHALE_REQS; i++) if (single.allow(t)) singleOk++;
  log(`   NAIVE single bucket (cap ${LIMIT}): ${singleOk}/${WHALE_REQS} admitted,`);
  log(`      but all ${WHALE_REQS} ops slammed ONE bucket — a hot shard, a single lock.`);

  // Mitigation: split into N sub-buckets, sum of caps ≤ LIMIT, route by hash.
  const N = 4;
  const sub = Array.from({ length: N }, () => new TokenBucket(LIMIT / N, 0, 0)); // cap 3 each → sum 12
  const ops = new Array(N).fill(0);
  const admitted = new Array(N).fill(0);
  for (let i = 0; i < WHALE_REQS; i++) {
    const shard = hash32(`whale-req-${i}`) % N;
    ops[shard]++;
    if (sub[shard].allow(t)) admitted[shard]++;
  }
  const splitOk = admitted.reduce((a, b) => a + b, 0);
  log(`   SPLIT into ${N} sub-buckets (cap ${LIMIT / N} each, sum ${LIMIT}), routed by hash(reqId):`);
  for (let s = 0; s < N; s++) {
    log(`      sub-bucket #${s}: ${ops[s]} ops → ${admitted[s]} admitted`);
  }
  log(`      total admitted ${splitOk}/${WHALE_REQS} (same budget) — but load spread across ${N} shards,`);
  log(`      ~${Math.round(WHALE_REQS / N)} ops each instead of ${WHALE_REQS} on one. The hot partition is gone.`);
  log("   (Alternative: give the whale a DEDICATED bucket/shard so it can't touch anyone else.)");

  log("");
  log("Limits STACK: a request runs the gauntlet per-IP → per-API-key → per-tenant →");
  log("per-endpoint → global, and the first empty bucket returns the 429 naming itself.");
  log("Per-tenant is the noisy-neighbor guard; a whale key is a hot partition you split");
  log("or isolate. All of it lives in gateway-owned, hot-reloadable config — not per service.");
  process.exit(0);
}

main();
