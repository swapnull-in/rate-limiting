/** Drill data — mined from Deep Dives/22-rate-limiting.md (+09-api-gateway.md). Loaded by index.html's Drill panel. */
window.DRILL = {
  module: "Deep Dive 22 — Rate Limiting & Admission Control",
  source: "Deep Dives/22-rate-limiting.md + 09-api-gateway.md",
  cheats: [
    "Four jobs: <b>capacity, fairness, abuse, tiers</b> — name yours; and <em>rate limit ≠ concurrency limit</em>.",
    "<b>Token bucket by default</b> (burst B, sustained r, 2 numbers, lazy refill); sliding-window counter for hard quotas; GCRA is the elegant form.",
    "Token bucket in one line: <code>tokens = min(B, tokens + elapsed * r)</code>; take one or deny with <code>retry_after = (1 - tokens) / r</code>.",
    "Distributed: <b>atomic in Redis via Lua</b> for exact; <b>local + batch-borrowing</b> for fast — and state the ±% you accept.",
    "<b>429 + Retry-After + quota headers</b>; clients back off with <em>exponential backoff + jitter</em> or the limiter creates the storm it should prevent.",
    "Shed early, shed cheap: reject at WAF/CDN/gateway, not inside the service — an edge rejection costs ~nothing.",
    "Redis down → <b>fail-open for reads, fail-closed for auth/money</b>, local fallback caps either way. Per-endpoint policy, never a global switch.",
    "Hot API key = hot partition — <b>split, cache, or isolate the whale</b>.",
    "Multi-region: rate-limit <b>per region</b> against regional quotas — a globally synchronous limiter imports cross-region latency nobody needs.",
    "Gateway one-liner (09): single entry point — routing, authn, <b>rate limiting</b>, TLS — then requests fan out to services.",
    "Per-node in-memory counters multiply your limit by N nodes (09) — counters live in <b>Redis, atomic INCR/Lua</b>, shared by the fleet.",
    "The gateway stays thin — route, verify, limit (09); the moment it orchestrates, it's a monolith with root access."
  ],
  cards: [
    {
      topic: "Store down",
      q: "Redis is down — now what does the limiter do?",
      a: "Fail-open vs fail-closed is a <b>per-endpoint policy</b>, not a global switch. Fail open for user-facing reads — availability beats enforcement. Fail closed for auth, login, and payment endpoints — there, enforcement IS the security. Local fallback limits (each node enforces a conservative cap) cover the gap, and the limiter must never be a single point of failure for the whole API."
    },
    {
      topic: "Atomicity race",
      q: "Two gateways race on the same key — how do you stop over-admission?",
      a: "Put atomicity <b>in the store</b>, not in the app: a Redis Lua script (or CAS) runs the whole check-and-decrement with nothing interleaved. The naive read-compute-write races — two gateways read tokens=1 simultaneously and both allow. Alternatively, accept approximation via batch-borrowing."
    },
    {
      topic: "Hot key",
      q: "One API key is 50% of all traffic — what breaks and what do you do?",
      a: "That key's bucket lives on one Redis shard — a classic hot partition. Split the key's bucket into sub-buckets and sum on check, cache a local allowance on each gateway, or move the whale to dedicated capacity. Same playbook as any hot-partition problem."
    },
    {
      topic: "Clock skew",
      q: "Gateway nodes have skewed clocks — does the limiter drift?",
      a: "Not if you never trust node clocks: use the <b>store's</b> clock — Redis <code>TIME</code> called inside the Lua script — so every refill computation shares one authoritative clock. Per-node wall clocks are the bug, not an implementation detail."
    },
    {
      topic: "Reset herd",
      q: "Thundering herd every time the window resets — why, and the fix?",
      a: "That is fixed-window's boundary burst: the counter resets at an instant, so clients pile onto it. Token bucket and GCRA have <b>no reset instant</b> — refill is continuous — so prefer them. If you are stuck with windows, jitter the reset per key so keys do not reset in unison."
    },
    {
      topic: "Quota visibility",
      q: "How do users know their quota and when to retry?",
      a: "Quota headers (<code>X-RateLimit-Limit/Remaining/Reset</code>), a quota endpoint, and docs. A 429 with <code>Retry-After</code> is part of the API contract, not an error to hide — well-behaved clients self-regulate off it."
    },
    {
      topic: "Algorithm choice",
      q: "Which rate-limiting algorithm, and why is it the default?",
      a: "<b>Token bucket</b> is the industry default (Stripe, AWS): real clients are bursty, and it forgives bursts up to B while enforcing sustained rate r, with O(1) state — two numbers, lazy refill. Use sliding-window counter when the contract is a hard cap per window (billing/quotas). Fixed window has the 2x boundary burst; sliding log is exact but O(requests) memory; GCRA is leaky bucket as a single timestamp (Redis-Cell)."
    },
    {
      topic: "Distributed options",
      q: "Enforce one global limit across a fleet of gateways — what are the three options?",
      a: "<b>A: global store</b> — every request runs atomic Lua in Redis; exact, but +0.5-1 ms per request and Redis is on the hot path. <b>B: local buckets, limit/N each</b> — free and dependency-less, but LB skew makes it false-throttle and N changes every deploy. <b>C: local + batch-borrowing</b> (the production answer) — each node borrows ~100 tokens from Redis at a time; one store trip per 100 requests, accuracy becomes eventual with brief over-admission."
    },
    {
      topic: "The trade",
      q: "Exact or approximate — which do you pick, and what do you say unprompted?",
      a: "Say: exact global limits cost a synchronous hop on every request; local enforcement is free but approximate. Go local-with-borrowing and accept ~5% overshoot — rate limits protect capacity, they are not billing invariants. If it IS billing or quota, go exact (Option A). Multi-region: enforce per-region quotas."
    },
    {
      topic: "Response contract",
      q: "A request is rejected — what exactly goes back, and what is the trap?",
      a: "429 (or 503 + Retry-After for server-overload semantics) with <code>Retry-After</code> and quota headers, Stripe/GitHub style. The trap: naive clients retry immediately, so the limiter <em>amplifies</em> load instead of shedding it — clients and your own SDKs must back off exponentially with jitter, and egregious offenders get dropped, not politely 429'd. Shed at the outermost layer, and shed anonymous reads before authenticated writes under global pressure."
    },
    {
      topic: "Design skeleton",
      q: "Design a distributed rate limiter — the 5-minute skeleton.",
      a: "1) Requirements: rules per key/tier, scale (1M rps, 50 gateways), latency budget under 1 ms, accuracy tolerance (protective ±5% OK; quota exact), multi-region. 2) Placement: library/sidecar in the gateway — not a separate synchronous service. 3) Algorithm: token bucket, lazy refill, 2 numbers per key. 4) State: Redis sharded by key, Lua for atomicity, local borrowing on the hot path. 5) Anchor numbers: Redis op sub-ms, ~100k ops/sec/node, ~40 bytes/key so 100M keys is ~4 GB — memory is not the problem, the synchronous hop is."
    },
    {
      topic: "Rate vs concurrency",
      q: "Why is a rate limiter not enough to protect a slow downstream?",
      a: "Rate caps the <em>arrival</em> rate per window; concurrency caps <em>in-flight</em> requests. A low rate of expensive, slow queries passes any rate limit yet saturates the pool — rate limiting is blind to service time. Mature systems run both, and saying so unprompted is a Staff signal."
    },
    {
      topic: "Layered keying",
      q: "What keys does a real request get limited on?",
      a: "Limits stack — a request must pass <b>all</b> applicable buckets: per-IP (edge, abuse) → per-API-key/user (gateway, tiers) → per-tenant (fairness) → per-endpoint (expensive routes tighter) → global (capacity floor). Rules live in hot-reloadable config owned by the gateway/sidecar — services should not hand-roll their own."
    }
  ]
};
