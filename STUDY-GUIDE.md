# Study Guide — Rate Limiting & Admission Control

This repo is the runnable half of **Deep Dive 22 — Rate Limiting & Admission Control**. Study in pairs: read a section of the deep dive, then run the matching phase (`npm run phaseN`) and predict the output before you look — every phase uses a virtual clock, so the numbers are exact and your prediction is checkable. Finish each sitting with the Drill panel in the web Rate Lab (`npm run web`) for active recall.

## Phase → deep-dive mapping

| Phase | What it builds | Deep-dive section | The staff insight |
|---|---|---|---|
| 1 | Fixed window counter and its boundary bug | §2 Algorithms | "2× burst at boundary (100 at :59 + 100 at :01)" |
| 2 | Token bucket with lazy refill | §2 Algorithms | "allows bursts up to B, enforces sustained rate r — matches real traffic; O(1) memory" |
| 3 | Sliding window log (the accuracy baseline) | §2 Algorithms | "exact — memory = O(requests) — dies at scale" |
| 4 | Sliding window counter (two integers) | §2 Algorithms | "near-exact, 2 counters — approximation (assumes even spread)" |
| 5 | Shared Redis counter + the atomic Lua fix | §3 Option A | "read-compute-write from the app races: two gateways read tokens=1 simultaneously, both allow" |
| 6 | Leaky bucket queue and GCRA | §2 Algorithms | "GCRA: leaky bucket as a timestamp — one stored value, no background refill" |
| 7 | Central vs local ÷N vs batch-borrowing, side by side | §3 Options A–C | "local-with-borrowing and accept ~5% overshoot — rate limits protect capacity, they're not billing invariants" |
| 8 | Per-endpoint fail-open / fail-closed policy | §6 Failure modes | "fail open for user-facing reads, fail closed for auth/login/payment — enforcement IS the security" |
| 9 | The 429 contract and the retry storm | §4 Response contract | "naive clients retry immediately on 429 → the limiter amplifies load instead of shedding it" |
| 10 | Concurrency limiter vs rate limiter | §1 What it's for | "Rate = requests per window. Concurrency = in-flight requests — saying this unprompted is a Staff signal" |
| 11 | Layered keys + the hot-key whale | §5 Keying · §6 | "Limits stack — a request passes all applicable buckets"; "split, cache, or isolate the whale" |

## Go deeper

- **Deep Dives/09-api-gateway.md** — where the limiter lives: the gateway's middleware menu, and why per-node counters silently multiply your limit by N.
- **Deep Dives/01-redis.md** — the store itself: atomic INCR/Lua, sharding by key, and the rate-limiter recipe in §5.4.
- **Core Course/11-reliability.md** — the surrounding toolkit: timeouts, circuit breakers, and where admission control fits among them.
- **Core Course/28-designing-for-failure.md** — degrading by priority under global pressure: shedding anonymous reads before authenticated writes.
