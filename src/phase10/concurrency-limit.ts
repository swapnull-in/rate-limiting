/**
 * Phase 10 — RATE LIMIT ≠ CONCURRENCY LIMIT: protecting slow downstreams.
 * Run: node "src/phase10/concurrency-limit.ts"
 *
 * A Staff-level distinction most candidates miss. Two different things get
 * protected by two different mechanisms:
 *
 *   • A RATE limiter caps the ARRIVAL rate — requests admitted per window
 *     (e.g. 5 req/sec). It counts how often requests SHOW UP.
 *   • A CONCURRENCY limiter caps the IN-FLIGHT count — requests being
 *     PROCESSED at once (e.g. a connection/thread pool of C=10 slots). It
 *     counts how many are ACTIVE right now.
 *
 * The trap: a LOW rate of SLOW requests can saturate a downstream that a rate
 * limiter happily admits. Rate limiting sees arrivals; it is blind to how long
 * each request HOLDS a resource. A pool of 10 connections dies not from too many
 * arrivals, but from arrivals that linger.
 *
 * LITTLE'S LAW ties them together:  L = λ × W
 *   in-flight = arrival_rate × service_time
 * The concurrency you actually need (L) depends on BOTH how fast requests arrive
 * (λ) AND how long each one takes (W). A rate limiter set on λ alone ignores W
 * entirely — so it cannot know L. That is precisely the number a concurrency
 * limiter caps.
 *
 * This file uses a VIRTUAL CLOCK: requests arrive at fixed tick times, and each
 * occupies a downstream slot for `serviceTime` ticks. No Date.now, no randomness
 * — the same run every time.
 */

import { log } from "../lib/log.ts";

/**
 * A downstream service with a fixed pool of C concurrent slots (connections /
 * threads). A request acquires a slot on start and releases it `serviceTime`
 * ticks later. `cap` is the concurrency limit: Infinity means "no limiter — the
 * pool absorbs whatever the rate limiter admits" (Demo A); a finite cap means a
 * semaphore rejects arrivals once all slots are busy (Demo B).
 */
class Downstream {
  private active: number[]; // completion tick of each in-flight request
  private cap: number;
  private serviceTime: number;
  admitted: number;
  rejected: number;
  peakInFlight: number;

  constructor(cap: number, serviceTime: number) {
    this.active = [];
    this.cap = cap;
    this.serviceTime = serviceTime;
    this.admitted = 0;
    this.rejected = 0;
    this.peakInFlight = 0;
  }

  // Free every slot whose request has completed by `now`.
  private release(now: number): void {
    this.active = this.active.filter((completesAt) => completesAt > now);
  }

  // Try to admit a request arriving at `now`. Returns the in-flight count after.
  arrive(now: number): { admitted: boolean; inFlight: number } {
    this.release(now);
    let ok: boolean;
    if (this.active.length < this.cap) {
      this.active.push(now + this.serviceTime); // hold a slot for serviceTime
      this.admitted += 1;
      ok = true;
    } else {
      this.rejected += 1; // pool exhausted → shed load (or bounded-queue)
      ok = false;
    }
    this.peakInFlight = Math.max(this.peakInFlight, this.active.length);
    return { admitted: ok, inFlight: this.active.length };
  }
}

function main() {
  // ── Demo parameters ───────────────────────────────────────────────────────
  // Traffic: 5 requests/sec — one every 200ms. Well under any sane arrival cap.
  const arrivalPerSec = 5;
  const intervalMs = 1000 / arrivalPerSec; // 200ms between arrivals
  const serviceMs = 3000;                  // each request is SLOW: holds a slot 3s
  const poolSize = 10;                     // downstream has C=10 concurrent slots

  // Arrivals from t=0 for 4 seconds (21 requests, every 200ms).
  const arrivals: number[] = [];
  for (let t = 0; t <= 4000; t += intervalMs) arrivals.push(t);

  log(`Traffic: ${arrivalPerSec} req/sec (one every ${intervalMs}ms), each holds a slot for ${serviceMs}ms.`);
  log(`Downstream pool: C=${poolSize} concurrent slots. A rate limiter of ${arrivalPerSec} req/sec admits ALL of it.`);
  log("");

  // ── A) RATE LIMIT ALONE — the pool has no concurrency cap ──────────────────
  log("═══ A) RATE LIMIT ALONE (5 req/sec admits everything; NO concurrency cap) ═══");
  log("   The rate limiter says 'sure, 5/sec is fine'. Watch in-flight climb anyway:");
  const rateOnly = new Downstream(Infinity, serviceMs);
  for (const t of arrivals) {
    const { inFlight } = rateOnly.arrive(t);
    const over = inFlight > poolSize ? `  ⚠ OVER C=${poolSize} — pool exhausted` : "";
    log(`   req @ ${String(t).padStart(4)}ms → in-flight: ${String(inFlight).padStart(2)}${over}`);
  }
  log(`   → peak in-flight = ${rateOnly.peakInFlight}, but the pool only has ${poolSize} slots.`);
  log(`   A LOW rate of SLOW requests saturated the downstream. Rate limiting never saw it coming.`);
  log("");

  // ── B) CONCURRENCY LIMITER — a semaphore caps in-flight at C ────────────────
  log(`═══ B) CONCURRENCY LIMITER (semaphore: in-flight capped at C=${poolSize}) ═══`);
  log("   Same slow traffic. Acquire a slot on start, release on completion; full → reject:");
  const limited = new Downstream(poolSize, serviceMs);
  for (const t of arrivals) {
    const { admitted, inFlight } = limited.arrive(t);
    const mark = admitted ? "✓ admitted" : "✗ rejected (pool full)";
    log(`   req @ ${String(t).padStart(4)}ms → ${mark.padEnd(22)} in-flight: ${String(inFlight).padStart(2)}`);
  }
  log(`   → peak in-flight = ${limited.peakInFlight} (never exceeds C=${poolSize}). ${limited.rejected} requests shed to protect the pool.`);
  log(`   The downstream stays healthy. The rate-only run would have overwhelmed it.`);
  log("");

  // ── C) LITTLE'S LAW — why the numbers came out this way ─────────────────────
  // L = λ × W : the in-flight count a stable system settles at equals the
  // arrival rate times the service time. This is the concurrency you must
  // provision — and the number a concurrency limiter caps.
  const lambda = arrivalPerSec;      // λ = 5 req/sec
  const W = serviceMs / 1000;        // W = 3.0 sec of service per request
  const L = lambda * W;              // L = required in-flight
  log("═══ C) LITTLE'S LAW:  L = λ × W  (in-flight = arrival_rate × service_time) ═══");
  log(`   λ = ${lambda} req/sec   W = ${W.toFixed(1)} sec   →   L = λ × W = ${L} concurrent in-flight required.`);
  log(`   The rate limiter was set on λ=${lambda} alone and IGNORED W entirely — so it could not`);
  log(`   know L=${L}. With a pool of only C=${poolSize}, L=${L} means guaranteed saturation.`);
  log(`   The concurrency limiter caps exactly this L at C=${poolSize}, whatever W turns out to be.`);
  log("");

  // ── D) Mature systems use BOTH ──────────────────────────────────────────────
  log("═══ D) USE BOTH ═══");
  log("   • Rate limits at the EDGE — arrival/abuse control, per-client fairness, DDoS.");
  log("   • Concurrency limits for slow DOWNSTREAMS — DB pools, upstream APIs, thread pools.");
  log("   • Adaptive concurrency (AIMD, like TCP congestion control; Netflix concurrency-limits)");
  log("     tunes C automatically from observed latency, so you don't hand-pick it.");
  log("");
  log("TAKEAWAY: rate ≠ concurrency. A rate limiter caps ARRIVALS per window, but a low rate");
  log("of SLOW requests can still exhaust a downstream's in-flight capacity (Little's Law:");
  log("in-flight = arrival_rate × service_time). Protect slow downstreams with a CONCURRENCY");
  log("limiter (a semaphore / in-flight cap), and run BOTH: rate limits for arrival/abuse at the");
  log("edge, concurrency limits for the expensive downstreams. Saying 'rate ≠ concurrency'");
  log("unprompted is a Staff signal.");
  process.exit(0);
}

main();
