# Task: debug run of one real polling cycle (retry, then success)

## Scope

Observe the M3 polling path in a running application with a debugger attached: a source that
fails once with 503 and then serves a valid feed. This task changes no production code. Its output
is a debug-log entry and, if the run contradicts the M3 contracts, a list of findings.

## Setup

1. MongoDB is up (`docker compose up -d`; Docker was not running at the time of writing).
2. A local feed source that returns `503` on the first request and a valid RSS/Atom document with
   `200` on every later request. The tool is the author's choice; it must count requests so the
   switch happens exactly once.
3. One feed registered through `POST /feeds` pointing at that source.
4. The application started in the IDE under the debugger with
   `--feeds.polling.enabled=true --feeds.polling.interval=1s`.

## Breakpoints

| Location | Question to answer |
|---|---|
| `FeedPollingCoordinator.isRetryable` | Which exception type arrives for a 503, and does the filter accept it? |
| `FeedPollingCoordinator.recordSuccess` (private) | On which attempt does it fire, and with which timestamps? |
| `MongoFeedPollStatusStore.recordSuccess` | What exactly is written to the feed document? |

## Expected observations

1. The first attempt fails with a source exception; `isRetryable` returns `true`; no
   `poll_feed_failed` log appears while retries remain.
2. A later attempt succeeds; `recordSuccess` is reached exactly once for that cycle.
3. `GET /feeds/{id}` shows a non-null `pollStatus` reflecting the success, with no raw exception
   text in it (sanitized result only).
4. Items from the fetch exist once each; a repeated cycle does not create duplicates
   (`(feedId, guid)` unique index).

## Done when

- Each breakpoint was hit and its question is answered in one line.
- Any observation that differs from the list above is recorded as a finding, not fixed in this
  task.
- A `## Bug N` entry is appended to `logs/debug-log.md` in the 100-day-plan repo using the debug
  template (the Reproduce / Isolate / Hypothesize / Verify structure applies even when the run is
  an observation rather than a failure).

## Out of scope

`streak-history` Debug run, M2 open items (`IllegalReferenceCountException`, final `onErrorMap`,
SSRF), and any change to retry or status behavior.
