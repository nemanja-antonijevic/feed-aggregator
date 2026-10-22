# Task: per-feed failure isolation and logging in the polling cycle (M3)

## Scope

`FeedPollingCoordinator.runCycle()`: what happens to the other feeds of a cycle when one feed's poll
fails, and what is logged about that failure. The retry policy, per-feed time budgets, `pollStatus`
persistence and the application lifecycle are out of scope (see Known limitations).

## Contract decisions

1. Within one cycle every feed returned by the catalog is attempted exactly once, at a concurrency of
   `FEED_CONCURRENCY` (4).
2. A failing feed does not cancel the remaining feeds and does not end the cycle. The cycle completes
   normally and the tick is still emitted by `cycles()`.
3. "Failing" covers an error signal (`Mono.error`) and a synchronous throw from
   `FeedPollOperation.poll(feed)`. The coordinator does not rely on implementations never throwing:
   the call is wrapped in `Mono.defer`, so a synchronous throw becomes an error signal of that one
   feed. A `null` instead of a `Mono` is turned into an error signal by `Mono.defer` as well, but
   that path has no test.
4. The isolation boundary is inside the `flatMap` mapper, around the single-feed `Mono`. Placed
   outside the mapper, `flatMap` would already have propagated the error and cancelled the inner
   subscriptions of the other feeds.
5. A failed feed produces exactly one WARN event:
   `event=poll_feed_failed feedId=<id> errorType=<exception simple class name>`.
   `WARN` because the failure is contained; `ERROR` is reserved for a failure of the whole cycle.
6. The event never contains the feed URL, the exception message or the stack trace. The logger
   receives no `Throwable`: messages and cause chains of arbitrary exceptions can carry the URL with
   user info or query tokens, and the policy must hold by construction, not by checking today's
   exception types.
7. The log call is a `doOnError` placed before `onErrorResume`. If the logging callback itself throws,
   the resulting error is still an error signal of that feed and is still isolated.

## Tests (`FeedPollingCoordinatorTest`)

| Test | Protects |
|---|---|
| `feedFailureDoesNotPreventOtherFeedsFromBeingPolled` | `Mono.error` of one feed: both feeds attempted once, cycle emits `0L` |
| `synchronousFeedFailureDoesNotPreventLaterFeedsFromBeingPolled` | synchronous throw of the middle of three feeds: all three attempted once (fails without `Mono.defer`) |
| `feedFailureLogsSanitizedWarningAndCycleContinues` | WARN level, exact message, no URL/`secret`/`token=` in it, `ThrowableProxy == null`, cycle continues (fails if the `Throwable` is passed to the logger or `doOnError` is removed) |

The two earlier tests (skipped ticks, catalog timeout) are unchanged. Full suite: 52 tests, 0 failures
(Testcontainers tests need Docker).

## Known limitations

- A failure of the whole cycle (catalog read error or the 10 s catalog timeout) is still swallowed by
  the outer `onErrorResume` without a log line. `poll_cycle_failed` is the next separate change.
- `errorType` does not say why a feed failed: `FeedSourceRequestException` covers an HTTP error status,
  a connection failure and a body over 1 MiB alike. `getSimpleName()` is empty for an anonymous class.
- There is no per-feed timeout in the coordinator. A hanging poll holds one of the 4 concurrency slots
  until the cycle ends.
- A permanently dead feed logs one WARN per cycle.
- `FeedPollOperation` has no production implementation yet and the coordinator is not wired into the
  application lifecycle, so none of this runs in the application yet.
- No test covers a `null` returned by `FeedPollOperation.poll`.

## Commits

- `e7f6166` isolate per-feed poll failures in the cycle
- `e58cfc3` log per-feed poll failures without leaking details
