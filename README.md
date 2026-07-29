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

## What each phase proves (the money quotes)

- **Phase 1** — a client sneaks **10** requests (2×limit) through a limit-of-5
  fixed window by straddling the window boundary.
- **Phase 2** — a full bucket allows a burst of 5 instantly, then meters to
  exactly the 2/sec refill rate afterward.
- **Phase 5** — 20 concurrent requests against a limit of 5: the naive
  GET-then-INCR limiter allows **all 20**; the atomic Lua limiter allows
  **exactly 5**. That's the race, and the fix.

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
```

## License

MIT — use it, fork it, learn from it.
