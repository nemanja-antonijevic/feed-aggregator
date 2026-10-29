# Task: connect feed polling to the application lifecycle (M3)

## Scope

Provide the production `FeedPollOperation`, create the polling coordinator from validated
configuration and own its subscription through the Spring application lifecycle. This task does
not change retry, per-feed status, skipped-tick or catalog-timeout behavior already implemented in
the coordinator.

## Contract decisions

### Production poll operation

1. Production polling is a thin adapter over `FetchFeedService`; it does not introduce a second
   download, parse or item-persistence path.
2. The adapter deliberately calls the existing public `FetchFeedService.fetch(String feedId)` with
   the ID from the catalog snapshot. The second `findById` is retained as a revalidation step. A
   feed deleted after the snapshot but before its poll is therefore not fetched from a stale URL.
   No new public `fetch(Feed)` overload is added.
3. `FeedNotFoundException` caused by that revalidation is treated by the adapter as a skipped poll:
   it completes empty, emits no log and does not become `poll_feed_failed`. The coordinator's
   following success status update matches no document and is therefore a no-op. The adapter
   catches the exception by type. `FeedService.findById` is its only current source in the fetch
   chain; adding another source of the same exception requires revisiting this rule.
4. A delete racing after the revalidation can still leave items whose feed was deleted. Eliminating
   that narrower race requires a transaction or a different data model and remains outside M3.
5. The adapter discards `FetchFeedResponse` with `.then()`. Item counts remain part of the manual
   endpoint response; scheduled polling does not persist or log them. Polling metrics are a later
   concern.
6. The adapter changes exactly one error type: `FeedNotFoundException` becomes empty completion.
   Every other error passes through as the same instance. This includes all `FeedSource*Exception`
   types so the coordinator retry allowlist can classify them, and the existing
   `ItemPersistenceException`, which intentionally remains non-retryable.
7. `retryWhen` resubscribes to the complete adapter operation. If a transient source failure happens
   after an earlier attempt saved any items, the next attempt may try to save them again. The
   `(feedId, guid)` unique index is a required idempotency mechanism and converts those writes into
   duplicates.

### Lifecycle ownership

8. A dedicated `SmartLifecycle` owns exactly one subscription to
   `FeedPollingCoordinator.cycles()`. `ApplicationRunner` is not used because it does not own a
   symmetric shutdown hook.
9. `start()` is idempotent while a live subscription exists. `stop()` disposes that subscription and
   makes `isRunning()` false. Calling `start()` after a normal `stop()` is supported and creates a
   new subscription; there is no automatic restart. The subscription reference is atomic because
   lifecycle calls and reactive terminal callbacks can run on different threads. A terminal
   callback clears the reference only with an identity-based compare-and-set against the
   subscription that installed that callback, so a late signal from subscription A cannot erase a
   restarted subscription B.
10. The lifecycle phase is explicitly 100 below Boot's
    `WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE`, and remains above the web server's
    start/stop phase. On shutdown, graceful HTTP shutdown begins first, polling is then cancelled,
    and the web server is finally stopped while lower-phase persistence infrastructure is still
    available. Startup is the reverse: the web server starts before polling subscribes. The
    integration test selects and compares both Boot lifecycle beans explicitly.
11. Feed indexes are created by `SmartInitializingSingleton` before Spring starts lifecycle beans.
    The lifecycle does not implement custom index ordering. An integration test must prove that the
    required indexes exist before the first production poll is invoked.
12. Shutdown is immediate rather than graceful. Both `stop()` and `stop(Runnable)` dispose the
    subscription; the callback form invokes its callback after disposal and does not wait for the
    current cycle.
13. Immediate disposal may cancel source work, item writes or a poll-status write. An individual
    Mongo update is atomic, so a status document is either updated or left at its earlier value, but
    the last attempt can remain unrecorded. Partial item writes remain safe to retry on the next
    start because of the `(feedId, guid)` unique index. Graceful draining is a non-goal for M3.
14. Subscription uses an explicit terminal error consumer. An error escaping `cycles()` logs
    `event=polling_stream_terminated errorType=<simple class name>` without a Throwable, URL or
    message, conditionally clears only its own subscription and leaves `isRunning()` false when no
    replacement is active. It is not restarted automatically; an operator or a later explicit
    `start()` is required.
15. The lifecycle overrides only `stop()`. It relies on `SmartLifecycle.stop(Runnable)` default
    behavior, which calls `stop()` and then always invokes the callback. Tests cover the callback
    when polling is running, was never started and was already cleared by a terminal error.

### Configuration and scheduling

16. `feeds.polling.enabled` defaults to `false`. When absent or false, production polling
    infrastructure is not created and no subscription occurs. Existing full-context tests therefore
    remain quiet without adding test-specific overrides.
17. `feeds.polling.interval` defaults to five minutes. It is bound as a `Duration` and must be at
    least one second. The properties bean is registered only when polling is enabled: an invalid
    interval is ignored while disabled, but zero, negative, null and sub-second values fail
    configuration validation during enabled context startup, before `Flux.interval` is assembled.
18. When enabled, the first tick is immediate because the application should catch up after startup
    or downtime. Later ticks use the configured interval; overlapping ticks retain the existing drop
    behavior.
19. Production uses a dedicated single-thread daemon scheduler named `feed-polling` for ticks,
    retry backoff and coordinator timeouts. HTTP and Mongo work remain on their own reactive
    threads. Work scheduled on `feed-polling` must be non-blocking; Rome parsing already moves to
    `boundedElastic`. BlockHound enforcement remains planned for M6. The Spring context owns and
    disposes the scheduler after the lifecycle has stopped; a normal lifecycle stop does not
    dispose it so restart remains possible.
20. The existing ten-second `CATALOG_TIMEOUT` continues to bound only loading and materializing the
    feed snapshot. It is not configurable in this task and is already covered by
    `catalogTimeoutDoesNotTerminatePollingStream`.
21. `poll_status_write_failed` continues to log the actual terminal status-write error type. A write
    budget expiry is therefore `TimeoutException`; no exception message, URL or Throwable is logged.
    This is already covered by
    `FeedPollingStatusCoordinatorTest.hangingStatusWriteTimesOutAndCycleCompletes`.

## Tests

- Adapter tests prove that it calls `fetch(String)` with the snapshot ID, discards a successful
  `FetchFeedResponse`, maps `FeedNotFoundException` to empty completion without logging and
  propagates every other error as the same instance.
- A coordinator-level test proves that `ItemPersistenceException` produces one attempt and is not
  retried. Existing retry tests continue to protect the source-exception allowlist.
- Lifecycle tests prove one subscription for repeated `start()`, immediate disposal on `stop()`,
  `isRunning()` transitions, a new subscription after stop/start and callback invocation from
  `stop(Runnable)` in running, never-started and terminal-error states.
- A concurrency test proves that a delayed terminal callback from stopped subscription A cannot
  clear restarted subscription B. A mutation that replaces identity compare-and-set with an
  unconditional clear must fail.
- A restart test uses a real scheduler and coordinator and observes an actual tick before and after
  stop/start. It must fail if `stop()` disposes the scheduler.
- A terminal-stream-error test proves the sanitized `polling_stream_terminated` event, no Throwable,
  `isRunning() == false` and no automatic resubscription.
- Configuration context tests prove that absent/false `feeds.polling.enabled` creates no production
  polling adapter, coordinator, scheduler or lifecycle; true creates all four; an invalid interval
  is ignored while disabled and fails before lifecycle startup while enabled.
- A full Testcontainers context with default properties proves that no scheduled poll occurs.
- An enabled Testcontainers context proves that URL and item unique indexes exist before the first
  production poll.
- The existing virtual-time coordinator test
  `slowCycleDropsIntermediateTickAndStartsNextCycleOnRegularTick` proves that the first tick is at
  time zero and that later ticks remain aligned to the configured interval. No real-time context
  test is added for this coordinator-owned behavior.
- A real-context phase test places polling strictly between Boot's graceful-shutdown and web-server
  start/stop lifecycle beans instead of depending on registration order.
- A scheduler test proves that the production scheduler thread is named `feed-polling-*`, is daemon
  and still produces a tick after lifecycle stop/start.
- A deletion test snapshots a feed, deletes it before the adapter runs and proves that no source
  request and no item write occur.

## Mutations

- Remove the second `findById` by calling a path that accepts the stale `Feed`: the deletion test
  must fail.
- Stop swallowing `FeedNotFoundException`: the adapter and lifecycle isolation tests must fail.
- Wrap a `FeedSourceHttpException` in a generic exception: the retry classification test must fail.
- Return `FetchFeedResponse` instead of completing the adapter with `Mono<Void>`: compilation or the
  adapter contract test must fail.
- Remove the idempotent-start guard: two-subscription lifecycle test must fail.
- Replace identity compare-and-set in a terminal callback with unconditional clearing: stale-signal
  concurrency test must fail.
- Remove the terminal error consumer: sanitized log and running-state tests must fail.
- Remove disposal of the active subscription from `stop()`: the immediate-cancellation test must
  fail. The scheduler is context-owned and intentionally unavailable to the lifecycle, so
  lifecycle `stop()` cannot dispose it.
- Change the lifecycle phase: the two-boundary web lifecycle test must fail.
- Change `enabled` default to true, remove conditional creation or accept a zero interval: context
  tests must fail.
- Delay the first tick by one interval: the virtual-time skipped-tick test must fail.

## Definition of done

- `README.md` documents scheduled polling, `feeds.polling.enabled`,
  `feeds.polling.interval`, their defaults, the immediate first cycle and the single-instance
  limitation; the milestone table marks M3 done.
- `curls.md` shows how to start the application with polling enabled and how to read `pollStatus`
  through the existing feed GET endpoint. No new HTTP endpoint is introduced.
- The full suite and lifecycle mutations are green/red as specified before cold review and commit.

## Known limitations and non-goals

- The application is single-instance for M3. Multiple instances can poll the same feed concurrently
  and can double-count status failures. Distributed leasing or leader election is not implemented.
- Graceful cycle draining on shutdown is not implemented.
- A delete racing after adapter revalidation can still leave orphaned items; transactions and
  cascading item deletion remain separate work.
- Scheduled fetch counts are not persisted or exposed as metrics.
