# Task: retry transient per-feed poll failures with backoff (M3)

## Scope

`FeedPollingCoordinator.runCycle()`: retry only transient source failures while preserving the
existing per-feed isolation, sanitized logging and skipped-tick behavior. Poll status persistence,
the production `FeedPollOperation` and application lifecycle remain out of scope.

## Contract decisions

1. The retry policy is an allowlist. It retries `FeedSourceTimeoutException`, HTTP 5xx represented
   by `FeedSourceHttpException`, and `FeedSourceConnectionException`. HTTP 4xx, an oversized body,
   invalid content or URL, parser/persistence failures and every unknown error are not retried.
2. `FeedSourceRequestException` remains the common API-facing base type so the manual M2 endpoint
   keeps its existing 502 responses and messages. Subtypes carry the structured classification used
   by the polling policy; message text is never inspected by the retry filter.
3. There are at most two retries after the first attempt. Backoff starts at 500 ms, uses multiplier
   2, is capped at 750 ms and has jitter explicitly disabled. The injected coordinator scheduler is
   used by both backoff and virtual-time tests. Attempts therefore start at 0, 500 and 1,250 ms.
4. The existing 3 second `FeedSourceClient` timeout applies to each source attempt. A separate
   12 second timeout surrounds the complete per-feed poll, including retry, parsing and persistence.
   The worst-case source portion is 10.25 seconds, leaving 1.75 seconds for parse and persistence.
   The coordinator's 10 second `CATALOG_TIMEOUT` only bounds loading the feed catalog; it is not a
   whole-cycle deadline.
5. `retryWhen` is inside the `flatMap` mapper and before the 12 second timeout and failure log. An
   exhausted retry rethrows the original failure. Consequently `poll_feed_failed` occurs exactly
   once with the original `errorType`, and the existing `onErrorResume` still isolates the feed.
6. The outer timeout may cancel persistence after some items were saved. This is accepted: the
   `(feedId, guid)` unique index makes the next cycle idempotent, and already saved items become
   duplicates. This timeout is logged as `TimeoutException`, not `FeedSourceTimeoutException`.
7. `UnknownHostException` is intentionally covered by the connection classification and retried.
   All 4xx statuses, including 408 and 429, wait for the next polling tick instead of retrying in the
   same cycle. `WebClientRequestException` also covers some permanent failures, such as a TLS
   handshake failure or unsupported scheme; these are intentionally retried with the same bounded
   1.25 second backoff cost because the coordinator uses a narrow type-based allowlist.
8. The production `FeedPollOperation` must propagate `FeedSourceTimeoutException`,
   `FeedSourceHttpException` and `FeedSourceConnectionException` unchanged. Wrapping them in an API
   or generic exception prevents the coordinator's retry filter from recognizing them. This
   precondition must be pinned by tests when the production operation is implemented.

## Tests

- `FeedSourceClientTest` pins structured HTTP, connection and oversized-body classification without
  changing the existing endpoint messages.
- Parameterized virtual-time tests independently cover timeout, HTTP 5xx and connection retries.
- Negative parameterized cases independently cover HTTP 4xx, oversized body, parser failure,
  invalid URL and an unknown error.
- Backoff assertions distinguish multiplier 2 from 1 and the 750 ms cap from an uncapped sequence.
- Exhaustion asserts three attempts, one sanitized feed warning with the original error type, one
  successful sibling poll and a normally completed cycle.
- A hanging poll proves the 12 second outer budget, `TimeoutException` logging and sibling isolation.
- A 5xx first attempt followed by a hanging retry proves that the 12 second timeout covers backoff
  and all attempts together instead of restarting for each attempt.

## Known limitations

- One dead feed now occupies one of four `flatMap` slots for up to 10.25 seconds instead of roughly
  3 seconds. Four dead feeds can occupy every slot during that period and delay healthy feeds. With
  the planned five-minute polling interval this is accepted for the training project; there is no
  prioritization, per-host limit or separate retry pool.
- If a complete cycle eventually exceeds the polling interval, the existing coordinator drops the
  overlapping tick rather than queueing another cycle.
- A 429 response does not honor `Retry-After`; the next regular tick is the next attempt.
- `FeedPollOperation` still has no production implementation and the coordinator is not connected to
  application lifecycle, so the retry policy is currently exercised through its coordinator seam.
