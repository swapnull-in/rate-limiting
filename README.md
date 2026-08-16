# Learn Rate Limiting in TypeScript

A hands-on, runnable project for understanding rate limiting and admission
control at a Staff/EM level — every major algorithm from scratch, plus the
distributed, atomic version on Redis.

Every phase is a small script you can run and read. No build step: modern Node
runs the TypeScript directly.

> Built to match a Staff-level study path. The through-line: admission control is
> the front door's job, and two laws govern it — **fail-open for reads,
> fail-closed for money**, and **exact-and-slow (central Redis) vs
> fast-and-approximate (local + borrowing), chosen per endpoint**.

## Prerequisites

- **Node.js 22+** (uses native `.ts` execution)
- **Redis** on `localhost:6379` for Phase 5 (`redis-cli ping` → `PONG`)

## Setup

```bash
npm install
```

## The lessons

Phases 1–4 use a virtual clock so the output is exact and reproducible.

| Command | Algorithm | The point |
|---|---|---|
| `npm run phase1` | **Fixed window counter** | simplest; the boundary-burst bug (2×limit) |
| `npm run phase2` | **Token bucket** | smooth average + controlled bursts (the default) |
| `npm run phase3` | **Sliding window log** | exact, but O(requests) memory per key |
| `npm run phase4` | **Sliding window counter** | ~exact with two integers — the production sweet spot |
| `npm run phase5` | **Distributed on Redis** | the atomicity race + the Lua fix + 429/Retry-After |
| `npm run phase6` | **Leaky bucket & GCRA** | smooth output; GCRA = a leaky bucket as one timestamp (Redis-Cell) |
| `npm run phase7` | **Distributed strategies** | central (exact/slow) vs local ÷N (skew) vs batch-borrowing |
| `npm run phase8` | **Fail-open / fail-closed** | Redis down → per-endpoint policy + a local fallback cap |
| `npm run phase9` | **The retry storm** | naive 429-retry amplifies load; backoff + jitter drains it |
| `npm run phase10` | **Concurrency limiting** | rate ≠ concurrency; Little's Law; a slow downstream saturates |
| `npm run phase11` | **Layered limits** | per-IP → key → tenant → endpoint → global; fairness; the whale |

> **Phases 6–11 fold in the Staff-level depth** — the missing algorithms (leaky/GCRA),
> the three distributed strategies, the failure-mode probe (fail-open/closed), the
> retry storm, `rate ≠ concurrency`, and layered keying. All dependency-free and
> deterministic (a virtual clock), so unlike phase 5 they need **no Redis**.

## What each phase proves (the money quotes)

- **Phase 1** — a client sneaks **10** requests (2×limit) through a limit-of-5
  fixed window by straddling the window boundary.
- **Phase 2** — a full bucket allows a burst of 5 instantly, then meters to
  exactly the 2/sec refill rate afterward.
- **Phase 5** — 20 concurrent requests against a limit of 5: the naive
  GET-then-INCR limiter allows **all 20**; the atomic Lua limiter allows
  **exactly 5**. That's the race, and the fix.
- **Phase 7** — across 5 skewed nodes: the central store admits exactly the limit
  (100 store trips), local ÷N false-throttles to **~75/100** under skew, and
  batch-borrowing lands at **~104/100** with only ~5 trips.
- **Phase 8** — with the store down, `GET /feed` **fails open** (served) while
  `POST /login` and `/pay` **fail closed** (503) — a per-endpoint policy, not a switch.
- **Phase 10** — 3 arrivals/tick (well within any rate limit) but a 6-tick service
  time → in-flight = **3×6 = 18** climbs past a pool of 10; a concurrency limiter
  holds it at 10. Rate ≠ concurrency.

## Interactive Rate Lab

Every phase is also a live instrument in the browser — straddle a window boundary
for the 2× burst, drain a token bucket, watch the atomicity race let 20 through a
limit of 5, cross the exact-vs-approximate trade, deny requests at different layers
of the stack, flip a store down to see fail-open vs fail-closed, and feed a retry
storm.

```bash
npm run web        # serves web/index.html at http://localhost:8080 (no deps)
```

One self-contained static page (self-hosted fonts), grouped by tier and
deep-linkable. To host it on **Cloudflare Pages**: connect this repo in the
dashboard with build output `web` (auto-deploys on push), or run
`npx wrangler login` then `npm run deploy`.

## The decision guide (which limiter when)

| Need | Use |
|---|---|
| Smooth average, allow bursts, O(1) state | **Token bucket** |
| Hard "no more than N per rolling T", exact | **Sliding window log** |
| Same, but memory-cheap and distributed | **Sliding window counter** (on Redis) |
| Simple and bursts are acceptable | **Fixed window** |

Plus the two operational laws: the check **must be atomic** (Redis Lua) or you
over-admit under load; and when the limiter store is down, **fail-open** for
read endpoints but **fail-closed** for money/abuse endpoints.

## Project layout

```
src/
  lib/log.ts               shared timestamped logger
  phase1/  fixed window counter
  phase2/  token bucket
  phase3/  sliding window log
  phase4/  sliding window counter
  phase5/  distributed + atomic Lua on Redis
  phase6/  leaky bucket & GCRA
  phase7/  distributed strategies (central / local / borrowing)
  phase8/  fail-open vs fail-closed
  phase9/  the retry storm + response contract
  phase10/ concurrency limiting (rate ≠ concurrency)
  phase11/ layered limits + the hot-key whale
web/
  index.html  ·  serve.mjs   (the interactive Rate Lab — npm run web)
```

## License

MIT — use it, fork it, learn from it.
