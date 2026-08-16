/**
 * Phase 8 — FAIL-OPEN vs FAIL-CLOSED: "Redis is down, now what?"
 * Run: node "src/phase8/fail-open-closed.ts"
 *
 * This is the #1 failure probe in a rate-limiting interview, and the answer that
 * separates Staff from everyone else. A distributed limiter keeps its counters in
 * a central store (Redis) so 100 gateway nodes share one global budget. Fine — until
 * that store blinks. Now every limiter call throws ECONNREFUSED. What does each node
 * do with the request it's holding?
 *
 *   The junior answer: one global switch — "fail open" (allow everything) or
 *   "fail closed" (deny everything). Both are wrong as a blanket policy:
 *     • Global fail-OPEN → an attacker credential-stuffs your /login freely the
 *       moment Redis hiccups. Your limiter outage became a security hole.
 *     • Global fail-CLOSED → Redis hiccups and your ENTIRE user-facing API returns
 *       503. Your limiter — a guardrail — just became a single point of failure
 *       for the whole product. A dependency of the limiter took down the site.
 *
 *   The Staff answer: fail-open vs fail-closed is a PER-ENDPOINT POLICY, chosen by
 *   asking one question about each endpoint — "is enforcement here about
 *   AVAILABILITY or about SECURITY?"
 *     • user-facing READS (GET /feed) → FAIL-OPEN. Availability beats enforcement.
 *       Serving a few extra reads during a Redis blip is harmless; blanking the
 *       feed is not. Don't let the limiter's outage become an API outage.
 *     • AUTH / LOGIN / PAYMENT → FAIL-CLOSED. Here enforcement IS the security.
 *       You must not let an attacker brute-force logins or replay charges just
 *       because the counter store is unreachable. Reject (503) and make them wait.
 *
 *   And covering the gap BOTH ways: a conservative LOCAL fallback cap that each
 *   node enforces from its own memory while the store is down — so fail-open reads
 *   aren't truly unlimited (each node still caps itself low), and you have a knob
 *   to allow a trickle on fail-closed endpoints instead of a hard zero if you want.
 *
 * We simulate the central store in-process (no Redis needed) with an UP/DOWN toggle,
 * register several endpoints each tagged with its policy, and watch the same store
 * outage produce different — correct — behaviour per endpoint.
 *
 * TAKEAWAY: when the limiter's store is down, fail-open vs fail-closed is a
 * PER-ENDPOINT policy — fail OPEN for user-facing reads (availability beats
 * enforcement; don't let a limiter outage become an API outage) and fail CLOSED
 * for auth/login/payment (enforcement IS the security), with a conservative LOCAL
 * fallback cap covering the gap either way. The limiter must never be a single
 * point of failure for the whole API.
 */

import { log } from "../lib/log.ts";

type Policy = "FAIL_OPEN" | "FAIL_CLOSED";

/** What the limiter hands back to the request handler: an HTTP-ish verdict. */
interface Verdict {
  status: 200 | 429 | 503;
  note: string;
}

/**
 * The shared counter store (stand-in for Redis). Single atomic INCR-and-check.
 * Flip `up` to false to simulate the store being unreachable — every call throws,
 * exactly as an ioredis call would when the connection is refused.
 */
class CentralStore {
  private up: boolean;
  private counts: Map<string, number>;

  constructor() {
    this.up = true;
    this.counts = new Map();
  }

  setUp(up: boolean): void {
    this.up = up;
  }

  /** Atomic increment-and-return for `key`. Throws when the store is down. */
  incr(key: string): number {
    if (!this.up) throw new Error("ECONNREFUSED: central store unreachable");
    const n = (this.counts.get(key) ?? 0) + 1;
    this.counts.set(key, n);
    return n;
  }
}

/**
 * One endpoint's limiter. Enforces `centralLimit` against the shared store when it
 * can reach it. When the store throws, it applies this endpoint's `policy` and falls
 * back to a conservative per-node `localCap` counted from local memory.
 */
class EndpointLimiter {
  private name: string;
  private policy: Policy;
  private centralLimit: number;
  private localCap: number;
  private store: CentralStore;
  private localCount: number;

  constructor(name: string, policy: Policy, centralLimit: number, localCap: number, store: CentralStore) {
    this.name = name;
    this.policy = policy;
    this.centralLimit = centralLimit;
    this.localCap = localCap;
    this.store = store;
    this.localCount = 0; // per-node counter used only while the store is down
  }

  allow(): Verdict {
    try {
      // Normal path: one atomic check against the shared store — exact global limit.
      const n = this.store.incr(this.name);
      if (n > this.centralLimit) {
        return { status: 429, note: `central limit ${this.centralLimit} hit (count ${n})` };
      }
      return { status: 200, note: `central ${n}/${this.centralLimit}` };
    } catch {
      // Store unreachable. This is the whole lesson — decide by POLICY, not by panic.
      this.localCount += 1;

      // Conservative local fallback cap: enforced from node memory either way, so
      // fail-open reads still aren't unlimited during the outage.
      if (this.localCount > this.localCap) {
        return { status: 429, note: `LOCAL fallback cap ${this.localCap} hit — throttled on this node` };
      }

      if (this.policy === "FAIL_OPEN") {
        // Availability > enforcement. Serve the read; don't take the site down.
        return { status: 200, note: `store DOWN → FAIL-OPEN, served locally ${this.localCount}/${this.localCap}` };
      }
      // FAIL_CLOSED: enforcement IS the security. Reject rather than let abuse run free.
      return { status: 503, note: "store DOWN → FAIL-CLOSED, rejected (enforcement is the security here)" };
    }
  }
}

/** Fire `count` requests at a limiter and log each verdict on a virtual timeline. */
function fire(clock: { now: number }, limiter: EndpointLimiter, label: string, count: number): void {
  for (let i = 1; i <= count; i++) {
    clock.now += 100; // deterministic virtual clock; no Date.now, no Math.random
    const v = limiter.allow();
    const mark = v.status === 200 ? "✓ 200 allowed " : v.status === 429 ? "✗ 429 throttled" : "✗ 503 rejected ";
    log(`   ${label} req ${i} @ ${clock.now}ms → ${mark}  (${v.note})`);
  }
}

function main() {
  const clock = { now: 0 };
  const store = new CentralStore();

  // Register endpoints. Each carries its OWN policy — this is the per-endpoint switch.
  //   name             policy         centralLimit  localCap
  const feed    = new EndpointLimiter("GET /feed",    "FAIL_OPEN",   5, 2, store);
  const login   = new EndpointLimiter("POST /login",  "FAIL_CLOSED", 3, 1, store);
  const payment = new EndpointLimiter("POST /pay",    "FAIL_CLOSED", 2, 1, store);

  // ─── A) STORE UP — everyone enforces exactly against the shared store ───────
  log("═══ A) STORE UP: every endpoint enforces its exact global limit ═══");
  fire(clock, feed,    "feed ", 7); // limit 5 → 5 allowed, 2 throttled
  fire(clock, login,   "login", 4); // limit 3 → 3 allowed, 1 throttled
  fire(clock, payment, "pay  ", 3); // limit 2 → 2 allowed, 1 throttled
  log("   → normal 429s from the central counter. The store is doing its job.");

  // ─── the store goes down ────────────────────────────────────────────────────
  log("");
  log("═══ ✱ CENTRAL STORE GOES DOWN (ECONNREFUSED on every limiter call) ✱ ═══");
  store.setUp(false);
  log("   the naive move is a GLOBAL switch. Watch what per-endpoint policy does instead.");

  // ─── B) STORE DOWN + FAIL-OPEN (reads) — keep serving the user ──────────────
  log("");
  log("═══ B) STORE DOWN, GET /feed is FAIL-OPEN: reads stay served ═══");
  fire(clock, feed, "feed ", 2); // fail-open, under local cap → allowed
  log("   → the limiter's dependency is down, but the read path is NOT. Availability wins.");

  // ─── C) STORE DOWN + FAIL-CLOSED (auth/payment) — reject, don't leak ────────
  log("");
  log("═══ C) STORE DOWN, POST /login & POST /pay are FAIL-CLOSED: rejected 503 ═══");
  fire(clock, login,   "login", 2); // fail-closed → 503 (then 429 once the tiny local cap is spent)
  fire(clock, payment, "pay  ", 2); // fail-closed → 503
  log("   → no free credential-stuffing / charge-replay just because Redis blinked.");

  // ─── D) LOCAL FALLBACK CAP — reads aren't unlimited during the outage ───────
  log("");
  log("═══ D) LOCAL fallback cap: even FAIL-OPEN reads are capped per node ═══");
  log("   feed already spent 2/2 of its local cap in (B); more reads now throttle LOCALLY:");
  fire(clock, feed, "feed ", 3); // local cap 2 already exhausted → 429 from node memory
  log("   → fail-open ≠ unlimited. Each node still enforces a conservative local budget.");

  // ─── E) the principle ───────────────────────────────────────────────────────
  log("");
  log("═══ E) The principle ═══");
  log("   Never let the limiter be a single point of failure for the WHOLE API.");
  log("   Choose open/closed PER ENDPOINT by asking: is enforcement here about");
  log("   AVAILABILITY (→ fail open) or about SECURITY (→ fail closed)? Back both");
  log("   with a conservative LOCAL cap so the gap is covered either way.");

  log("");
  log("Fail OPEN for user-facing reads (a limiter outage must not become an API outage),");
  log("fail CLOSED for auth/login/payment (there, enforcement IS the security), and let a");
  log("small LOCAL per-node cap cover the gap both ways. Per-endpoint policy, never a switch.");
  process.exit(0);
}

main();
