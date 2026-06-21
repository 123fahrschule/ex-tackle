# 1. Send messages through a long-lived, pooled publisher (instead of one connection per publish)

- Status: accepted
- Date: 2026-06-21
- Deciders: Andreas Riemer (+ team)
- Related: `ex-tackle-Review-und-UI-Konzept.md` (Finding 4 "new connection per publish/retry" and Finding 10 "nack without publisher confirms")

## Context and Problem Statement

When publishing, ex-tackle opens a **new AMQP connection per message** and closes it again afterwards:

- `Tackle.publish/2` runs through `Tackle.execute/3`. The `:default` path opens a fresh connection and closes it in the `after` block (`lib/tackle.ex:40-50`). On top of that, the target exchange is **re-declared on every publish** (`lib/tackle.ex:29`).
- The retry/dead path (`Tackle.DelayedRetry.publish/4`) **always** uses `:default` (`lib/tackle/delayed_retry.ex:11`) — i.e. a new connection for every retried message, exactly during an error storm.
- The `publisher_connection_name` option exists (`lib/tackle.ex:26`) and lets the **connection** be reused (`lib/tackle.ex:53-64`), but it is (a) **undocumented** in the README, (b) only partially effective (a channel is still opened per call, and the exchange is still declared per call), and (c) **ineffective for the retry path**.

Consequences: a TCP/AMQP/(for `amqps`) TLS handshake per message. This is expensive under load — and especially during error storms — and can exhaust broker-side connections/file descriptors. The library is used fleet-wide (9+ services, hundreds of consumers), which multiplies the effect. Observed in `absence`: the publisher sets `publisher_connection_name: "Absence Publisher"` (so the workaround is known), yet the retry path still goes through `:default`.

Two coupled questions need to be decided:

1. **Connection strategy when publishing** — connection/channel per call vs. long-lived/pooled; and does this also apply to the retry/dead path?
2. **Delivery guarantee** — should we publish with publisher confirms (this addresses Finding 10)?

## Decision Drivers

- Hot-path performance and resource usage: no connection/channel churn per message.
- Robustness during error storms: the retry/dead path must not be more expensive than the normal path.
- Delivery guarantee without silent loss (publisher confirms) — directly linked to Finding 10.
- AMQP best practice: one (a few) long-lived connection(s) per app; channels are cheap but must **not** be shared across processes.
- Backwards compatibility for the fleet: three ex-tackle revisions run in parallel, all pinned via `branch: main` (see Finding 15) → default changes propagate uncontrollably.
- Minimal changes to the existing consumer/publish API.

## Considered Options

- **O1 — Documentation only.** Document `publisher_connection_name` in the README and set it fleet-wide. No change to the hot path.
- **O2 — Named connection as the default.** Publishing no longer uses `:default` but a cached named connection; channel and exchange declaration still happen per call.
- **O3 — Long-lived, supervised publisher with a channel pool.** A `Tackle.Publisher` (per connection name) holds a connection + channel(s) via a pool (e.g. `nimble_pool`), declares exchanges once per `{connection, exchange, type}` (and again on reconnect), publishes optionally with confirms; the retry/dead path uses the same mechanism, or the consumer's already-open connection, instead of `:default`.
- **O4 — External publisher library** (e.g. Broadway / a custom AMQP abstraction). Adds a third-party dependency and covers more than needed.

| Dimension | O1 Docs | O2 Named default | O3 Pool + confirms | O4 External lib |
|---|---|---|---|---|
| Complexity / effort | very low | low | medium | medium–high |
| Connection churn fixed | no | partially (channel/declare remain) | **yes** | yes |
| Retry/dead path fixed | no | no | **yes** | yes |
| Delivery guarantee (confirms) | no | no | **yes (configurable)** | yes |
| Performance under load | unchanged | better | **best** | good |
| Backwards compatibility | full | high | high (additive) | low (third-party dep) |

## Decision Outcome

Chosen: **O3 as the target, O1 as the immediate transitional measure.**

Concretely: document `publisher_connection_name` right away and set it fleet-wide (O1), to make today's partially effective workaround official and consistent. In parallel, introduce a **supervised `Tackle.Publisher`** per connection name that holds a long-lived connection and a small channel pool, declares exchanges only once (and again on reconnect), and publishes **optionally with publisher confirms** (O3). `Tackle.publish/2` will route through this publisher; the retry/dead path in the `Executor` will use the same publisher, or the already-open consumer connection, instead of a new `:default` connection.

This is rolled out **additively**: the existing `:default` behaviour stays available, the pooled publisher is opt-in first and then recommended as the new default. The change ships via a **tagged release** instead of `branch: main` (Finding 15), to migrate the fleet in a controlled way.

O2 is rejected because it solves neither the channel/declare overhead nor the retry path. O4 is rejected because a third-party dependency is disproportionate for this narrowly scoped purpose and conflicts with the library's lean character.

### Consequences

**Positive**

- No connection/channel churn per message; significantly cheaper under load and during error storms.
- Consistent publish path: publish, retry, and republish use the same mechanism.
- Publisher confirms close the silent loss/duplication window from Finding 10.
- Idiomatic AMQP: channel ownership per process, connection reuse.
- `publisher_connection_name` is documented; no more "hidden knowledge".

**Negative / risks**

- More moving parts (publisher process, pool, confirm handling, reconnect/declare invalidation).
- Publisher confirms add some latency → keep them configurable.
- The channel pool must be sized (too small → serialization, too large → resource use).
- Declare caching must be invalidated on reconnect, otherwise a missing (re-)declaration after a broker restart.
- Rollout risk due to the fleet's `branch: main` pinning → tie to a tagged release.

**Follow-up decisions / action items**

- [x] Define the pool size (default) and the confirm default (on/off, timeout). — Pool size defaults to `4`; confirms default to **off**, with a `5_000 ms` timeout. All configurable via `config :tackle` (`publisher_pool_size`, `publisher_confirms`, `publisher_confirm_timeout`).
- [x] Decide: retry/dead publish via the existing consumer connection **or** via the central `Tackle.Publisher`. — Chosen: via the central shared `Tackle.Publisher` (`:tackle_publisher`), so an error storm reuses one connection.
- [x] Document `publisher_connection_name` in the README (analogous to `connection_id`) — immediate measure.
- [x] Describe the migration path and deprecation of the per-call `:default` publishing. — See the README "Migrating to the pooled publisher". The per-call path stays available behind `publisher_strategy: :per_call` / `publisher_connection_name: :default`.
- [ ] Move ex-tackle to tagged releases and align the fleet (Finding 15). — Process/ops follow-up, out of scope for this change.
- [x] Pull exchange declaration out of the publish hot path (once / on reconnect). — Exchanges are declared once per `{connection, exchange, type}`, cached, and re-declared on reconnect.

### Implementation notes

- `Tackle.Publisher` (one `GenServer` per connection name, under `Tackle.Publisher.Supervisor` with a `Tackle.Publisher.Registry`) owns a persistent connection and a `nimble_pool` of channels (`Tackle.Publisher.ChannelPool`). Publishing runs in the caller process via a pool checkout; the declared-exchange cache is an ETS table read on the hot path and cleared on reconnect.
- The `Republisher` was intentionally left on its existing per-batch connection: it holds a single channel across `get`/`publish`/`ack`, which does not fit the per-publish pool-checkout model, it is a manual/low-frequency operation (not the error-storm hot path), and its property-loss behaviour is tracked by a separate ADR.
