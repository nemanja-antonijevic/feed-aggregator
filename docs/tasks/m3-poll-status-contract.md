# Task: expose and persist per-feed poll status (M3)

## Scope

Persist the terminal outcome of each scheduled feed poll and expose that state through the feed API.
Manual `POST /feeds/{id}/fetch` calls do not change `pollStatus`.

## Contract decisions

1. `pollStatus` is embedded in the `Feed` document and contains:
   - `lastAttemptAt`: when the coordinator started the latest scheduled poll, before retries;
   - `lastSuccessAt`: when the latest successful poll completed, nullable;
   - `outcome`: `SUCCESS` or `FAILURE`;
   - `errorType`: terminal exception simple class name, nullable;
   - `consecutiveFailures`: non-negative integer.
   The coordinator captures `lastAttemptAt` from an injected `Clock` before invoking the poll and
   carries that value through retry and persistence. All stored instants are truncated to
   milliseconds, matching MongoDB precision and the existing `createdAt` policy.
2. A feed that has never been polled has no `pollStatus` field in MongoDB. `FeedResponse` represents
   that state as `"pollStatus": null`; old documents therefore remain readable without migration.
3. After success, `outcome` becomes `SUCCESS`, `lastAttemptAt` and `lastSuccessAt` are updated,
   `consecutiveFailures` becomes `0`, and the previous `errorType` is removed. After failure,
   `outcome` becomes `FAILURE`, `lastAttemptAt` and `errorType` are updated,
   `consecutiveFailures` is incremented atomically, and `lastSuccessAt` is preserved. The
   `errorType` is the class of the original terminal failure returned by the retry policy, never a
   Reactor retry-exhaustion wrapper. No additional causal unwrapping is performed: a classified
   client timeout remains `FeedSourceTimeoutException`, while the outer 12 second budget is
   `TimeoutException`. A poll is successful when its `Mono<Void>` completes normally; it does not
   need to emit an `onNext` value.
4. `FeedResponse` exposes the five fields above. It never exposes an exception message, cause,
   stack trace or any additional source URL. `errorType` is the same sanitized class name used by
   `poll_feed_failed` logging. Inside a non-null `pollStatus`, an unavailable `lastSuccessAt` is
   serialized as an explicit JSON `null`, not omitted.
5. The coordinator owns the terminal outcome and delegates persistence to a dedicated status store;
   `FeedPollOperation` only performs the feed work. The store updates `pollStatus.*` with atomic
   dotted-path `$set`, `$unset` and `$inc` operations, never by saving the whole `Feed` document.
   Every success or failure uses exactly one MongoDB update containing all operators for that
   outcome, so readers cannot observe a partially updated status. On success, `errorType` is removed
   with `$unset` in that same update.
   Future feed edits must likewise update only their own fields so status and feed data cannot
   overwrite one another.
6. Status updates never use upsert. A zero-match update means the feed was deleted during polling
   and is treated as a no-op, preventing resurrection or orphan status. A database error while
   writing status is logged once as sanitized `poll_status_write_failed` with `feedId` and
   `errorType`, but does not retry the poll, fail the cycle or cancel other feeds; the previous
   status may remain stale. The status-write branch is outside the feed-poll error boundary: its
   failure never emits `poll_feed_failed` and never changes a successful poll into a feed failure.
   Each status-store operation has a separate two-second timeout on the injected scheduler. Its
   `TimeoutException` follows the same `poll_status_write_failed` path, releases the per-feed slot
   and allows the cycle to complete.

## Required tests

- Never-polled old and new feeds return `pollStatus: null`. A real-Mongo test must inspect the raw
  document created through `FeedService.create` and prove that the `pollStatus` key is absent, not
  stored with an explicit `null` value.
- A first failure on a real document without `pollStatus` proves that one atomic update creates the
  embedded document, `$inc` produces `consecutiveFailures == 1`, and absent `lastSuccessAt` maps to
  an explicit JSON `null`.
- Two parallel failures against the same feed in real MongoDB produce
  `consecutiveFailures == 2`, proving atomic increment behavior. The feed's `url`, `title` and
  `createdAt` must remain unchanged after status updates.
- A later success clears an earlier error, resets `consecutiveFailures` to zero and preserves the
  rest of the feed document.
- A success followed by a failure preserves the exact earlier `lastSuccessAt`, changes `outcome` to
  `FAILURE`, stores the terminal `errorType` and increments `consecutiveFailures`.
- With virtual time, a poll that fails once and succeeds after retry stores `lastAttemptAt` from
  before the first attempt and a later `lastSuccessAt` from final completion. This test must fail if
  `lastAttemptAt` is captured after retry or immediately before status persistence.
- A poll whose first attempt fails and whose second attempt hangs records `FAILURE` exactly at the
  outer 12 second budget with `errorType == "TimeoutException"`. This test must fail if status
  persistence is placed before the timeout or if the timeout restarts per attempt.
- Response and status-write logs contain no exception message, cause, stack trace or extra URL.
- When a successful poll's status write fails, the cycle and sibling feeds continue,
  `poll_status_write_failed` is emitted once, and no `poll_feed_failed` event is emitted.
- When a failed poll's status write also fails, exactly one `poll_feed_failed` and exactly one
  `poll_status_write_failed` are emitted, and the cycle still completes.
- A status store that returns `Mono.never()` times out after two seconds, logs
  `poll_status_write_failed` with `errorType == "TimeoutException"`, releases its concurrency slot
  and does not stop later cycles.
- A zero-match update does not stop the cycle or create a document.

## Known limitation

Status persistence is best-effort. If its write fails, the feed items may be correct while the
visible `pollStatus` remains stale until a later scheduled poll updates it successfully.

True subscription cancellation is outside the status contract. The outer 12 second timeout is an
error (`TimeoutException`) and records `FAILURE`, and the 10 second catalog timeout happens before
any per-feed poll starts. Application shutdown or explicit cancellation of an active cycle can stop
a poll without a final status write, leaving the previous status unchanged until the next run.
