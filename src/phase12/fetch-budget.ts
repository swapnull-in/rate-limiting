/**
 * Phase 12 — SPENDING A BUDGET: when YOU are the rate-limited client.
 * Run: npm run phase12
 *
 * Every phase so far sat on the server side of the limit. A price tracker
 * (camelcamelcamel, Keepa) lives on the other side: the retailer's API allows
 * a fixed number of lookups per second, there are far more products than
 * that, and users want an alert the moment a price drops. Servers are cheap;
 * THE FETCH BUDGET IS THE CONSTRAINT. The same shape appears in crawlers,
 * feed pollers, and any system refreshing more things than its quota allows.
 *
 * So the scheduler is an ALLOCATOR: given B fetches per tick, which products
 * get them? The honest metric is not "fetches per second" — that's fixed —
 * but how long the people who care wait:
 *
 *   alert delay = (tick we noticed the price change) − (tick it happened)
 *   weighted by how many users are watching that product.
 *
 * THREE POLICIES:
 *
 *   • ROUND-ROBIN — every product on the same interval. Fair to products,
 *     unfair to users: a product with 5,000 watchers waits as long as one
 *     with a single watcher.
 *
 *   • PRIORITY — each tick, fetch the products with the highest
 *         score = staleness × √(watchers × volatility)
 *     Staleness makes every product eventually rise; watchers and volatility
 *     (how often its price has been changing) decide how FAST it rises. The
 *     square root is deliberate: delay falls off as 1/frequency, so spending
 *     in proportion to the raw weight over-serves the head and starves the
 *     rest — the same reasoning as weighted fair queuing.
 *
 *   • PRIORITY + CEILING — the same, plus a hard maximum interval: any product
 *     unchecked for longer jumps the queue. Without it, an unwatched, stable
 *     product may wait indefinitely and its price chart goes blank.
 *
 * And the operational case: the retailer throttles us to HALF the budget
 * (a 429 storm on a sale day — phase9 from the client's chair). Every
 * policy gets slower, but a good allocator keeps the most-watched products
 * inside their target even then.
 *
 * We demo: (A) the three policies on the same stream of price changes;
 * (B) the same with half the budget; (C) where the fetches actually went.
 *
 * TAKEAWAY: when the quota is the constraint, the scheduler is an allocator.
 * Prioritize by staleness × √(watchers × volatility), put a ceiling on the
 * interval so nothing starves, and measure late alerts per tier — not QPS.
 */

import { log } from "../lib/log.ts";

// ── Deterministic PRNG (seeded mulberry32) ───────────────────────────────────
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ── The catalogue ────────────────────────────────────────────────────────────

interface Product {
  watchers: number;      // users with an alert on this product
  volatility: number;    // probability its price changes on any given tick
}

const PRODUCTS = 2_000;
const TICKS = 1_200;
const BUDGET = 40;           // fetches per tick → a uniform interval of 50 ticks
const SLO_TICKS = 10;        // "alerted promptly" = noticed within 10 ticks
const CEILING = 200;         // max ticks any product may go unchecked

function catalogue(): Product[] {
  const rand = mulberry32(42);
  // Heavy-tailed interest: a few products have thousands of watchers, most have a handful.
  return Array.from({ length: PRODUCTS }, (_, i) => ({
    watchers: Math.floor(5_000 / (i + 1)) + 1,
    volatility: 0.002 + rand() * 0.03,
  }));
}

type Policy = "round-robin" | "priority" | "priority+ceiling";

interface Result {
  meanDelay: number;         // watcher-weighted mean alert delay, in ticks
  withinSlo: number;         // share of watcher-alerts noticed within SLO_TICKS
  top100Delay: number;       // same mean, for the 100 most-watched products only
  worstStaleness: number;    // longest any product went unchecked
  fetchesTop100: number;     // share of all fetches spent on the top 100
}

function simulate(policy: Policy, budget: number): Result {
  const products = catalogue();
  const changes = mulberry32(7);              // the same price-change stream for every policy
  const lastChecked = new Array<number>(PRODUCTS).fill(0);
  const pendingSince = new Array<number>(PRODUCTS).fill(-1);  // tick of the oldest unnoticed change
  const weight = products.map((p) => Math.sqrt(p.watchers * p.volatility));

  let delaySum = 0, alerts = 0, onTime = 0;
  let topDelaySum = 0, topAlerts = 0;
  let worstStaleness = 0, fetchesTop100 = 0, fetches = 0;
  let cursor = 0;                             // round-robin position

  const notice = (i: number, now: number) => {
    const since = pendingSince[i]!;
    if (since < 0) return;
    const delay = now - since, w = products[i]!.watchers;
    delaySum += delay * w; alerts += w;
    if (delay <= SLO_TICKS) onTime += w;
    if (i < 100) { topDelaySum += delay * w; topAlerts += w; }
    pendingSince[i] = -1;
  };

  for (let now = 1; now <= TICKS; now++) {
    // Prices move whether or not we are looking.
    for (let i = 0; i < PRODUCTS; i++) {
      if (changes() < products[i]!.volatility && pendingSince[i]! < 0) pendingSince[i] = now;
    }

    // Choose this tick's fetches.
    let chosen: number[];
    if (policy === "round-robin") {
      chosen = Array.from({ length: budget }, () => cursor++ % PRODUCTS);
    } else {
      const score = (i: number) => {
        const stale = now - lastChecked[i]!;
        if (policy === "priority+ceiling" && stale >= CEILING) return Infinity;
        return stale * weight[i]!;
      };
      chosen = Array.from({ length: PRODUCTS }, (_, i) => i)
        .sort((a, b) => score(b) - score(a))
        .slice(0, budget);
    }

    for (const i of chosen) {
      worstStaleness = Math.max(worstStaleness, now - lastChecked[i]!);
      lastChecked[i] = now;
      fetches++;
      if (i < 100) fetchesTop100++;
      notice(i, now);
    }
  }

  // Changes we never got round to noticing still count, as delay until the end.
  for (let i = 0; i < PRODUCTS; i++) {
    notice(i, TICKS);
    worstStaleness = Math.max(worstStaleness, TICKS - lastChecked[i]!);
  }

  return {
    meanDelay: delaySum / alerts,
    withinSlo: onTime / alerts,
    top100Delay: topDelaySum / topAlerts,
    worstStaleness,
    fetchesTop100: fetchesTop100 / fetches,
  };
}

const POLICIES: Policy[] = ["round-robin", "priority", "priority+ceiling"];
const pct = (x: number) => `${(100 * x).toFixed(0)}%`.padStart(4);
const row = (name: string, r: Result) =>
  `   ${name.padEnd(18)} mean delay ${r.meanDelay.toFixed(1).padStart(5)} ticks · within ${SLO_TICKS} ticks ${pct(r.withinSlo)} · top-100 delay ${r.top100Delay.toFixed(1).padStart(5)} · longest unchecked ${String(r.worstStaleness).padStart(4)}`;

log(`(A) FULL BUDGET — ${BUDGET} fetches/tick for ${PRODUCTS} products (a uniform interval would be ${PRODUCTS / BUDGET} ticks)`);
const full = new Map<Policy, Result>();
for (const policy of POLICIES) {
  full.set(policy, simulate(policy, BUDGET));
  log(row(policy, full.get(policy)!));
}
{
  const rr = full.get("round-robin")!, pr = full.get("priority")!, pc = full.get("priority+ceiling")!;
  log(`   priority cuts the delay users feel by ${(rr.meanDelay / pr.meanDelay).toFixed(1)}× with the SAME number of fetches`);
  log(`   but its least interesting product went ${pr.worstStaleness} ticks unchecked; the ceiling caps that at ~${pc.worstStaleness} for almost no cost`);
}

log("");
log(`(B) THROTTLED — the retailer cuts us to ${BUDGET / 2} fetches/tick`);
const half = new Map<Policy, Result>();
for (const policy of POLICIES) {
  half.set(policy, simulate(policy, BUDGET / 2));
  log(row(policy, half.get(policy)!));
}
{
  const rrF = full.get("round-robin")!, rrH = half.get("round-robin")!;
  const pcF = full.get("priority+ceiling")!, pcH = half.get("priority+ceiling")!;
  log(`   round-robin: top-100 delay ${rrF.top100Delay.toFixed(1)} → ${rrH.top100Delay.toFixed(1)} ticks (everyone degrades equally)`);
  log(`   priority+ceiling: top-100 delay ${pcF.top100Delay.toFixed(1)} → ${pcH.top100Delay.toFixed(1)} ticks (slower too, but still inside the ${SLO_TICKS}-tick target)`);
}

log("");
log("(C) WHERE THE BUDGET WENT — share of all fetches spent on the 100 most-watched products (5% of the catalogue)");
for (const policy of POLICIES) log(`   ${policy.padEnd(18)} ${pct(full.get(policy)!.fetchesTop100)}`);

log("");
log("TAKEAWAY: the quota is fixed, so schedule by staleness × √(watchers × volatility), cap the interval, and measure late alerts.");
