# Task: live item stream over SSE backed by a MongoDB change stream (M5)

## Scope

Add `GET /feeds/{id}/items/stream`, a Server-Sent Events endpoint that pushes every item newly
inserted for one feed, sourced from a MongoDB change stream on the `items` collection. The task
covers resume after reconnect, client disconnect (cancellation), slow-consumer backpressure and
idle-connection heartbeats. It does not change fetching, polling, parsing, indexes or the item
validator.

## Why this task exists

The compose file and `TestcontainersConfiguration` already run MongoDB as a single-node replica set
because change streams require one. M5 is the first feature that keeps a subscription open for hours
instead of returning a value, so the interesting failures move from "wrong answer" to "resource that
is never released" and "event that is silently lost".

## Contract decisions

### Endpoint and framing

1. `GET /feeds/{id}/items/stream`, produces `text/event-stream`. One feed per connection; there is no
   all-feeds stream.
2. The feed is looked up with `FeedService.findById` before the stream opens. Unknown or malformed id
   returns the existing JSON `ApiError` (404 / 400) with a normal status, not an SSE body.
3. Event name `item`; `id:` is the change stream resume token serialized as JSON; `data:` is a new
   `ItemResponse` DTO (`id`, `feedId`, `guid`, `title`, `link`, `publishedAt`, `fetchedAt`,
   `extras`). The entity is not serialized directly.
4. Only `insert` operations produce events. Updates and deletes are filtered out in the change stream
   pipeline, not in Java. A re-fetch that hits the `(feedId, guid)` unique index inserts nothing and
   therefore emits nothing.
5. The filter on `feedId` is part of the server-side pipeline (`$match` on
   `fullDocument.feedId`). `feedId` is stored as an `ObjectId` (`FieldType.OBJECT_ID` on `Item`), so
   the match value must be an `ObjectId`, not the string from the path.

### Start point and resume

6. A connection without `Last-Event-ID` receives only items inserted after the stream opened. There
   is no replay and no `startAtOperationTime`. An item inserted between the 404 check and the
   cursor opening can be missed; this is a documented gap, not a bug to fix here.
7. A connection with `Last-Event-ID: <token>` resumes after that token (`resumeAfter`) and receives
   every later insert for the feed exactly once, including those inserted while the client was away.
8. An unparsable token returns 400 `ApiError` before the stream opens. A parsable token whose oplog
   position no longer exists is a server-side `ChangeStreamHistoryLost`. Whether this can still be
   turned into a status code (response not yet committed) is an open prediction (Step 1, item 6);
   the contract is "client gets a clear terminal error, not a hung connection".

### Idle connections, cancellation, backpressure

9. A heartbeat SSE comment (`:keepalive`) is sent every `feeds.stream.heartbeat-interval` (default
   15s, configurable so tests can use milliseconds). The heartbeat is merged with the item flux and
   must end when the item flux ends (`takeUntilOther` or equivalent); a heartbeat that outlives its
   stream is a leak. Reason: a disconnected client on an idle stream is only noticed on the next
   write.
10. When the client disconnects, the subscription is cancelled and the server-side change stream
    cursor is closed. This is verified by the server's own open-cursor count, not by the absence of
    errors in the log.
11. The item flux between the change stream and the SSE writer is bounded
    (`onBackpressureBuffer` with an explicit capacity, default 256, configurable). Overflow
    terminates the stream with an error; it never drops items silently (`onBackpressureDrop` is
    rejected) and never buffers without limit. A client that was cut off recovers through
    `Last-Event-ID`.
12. The server does not resubscribe on its own after a Mongo error. Any upstream error ends the
    stream; reconnection is the client's job.

### Not covered (stated, not forgotten)

- A feed deleted while a stream is open: the stream stays open and idle, heartbeats continue.
- Limit on concurrent streams (each one is an open server cursor).
- Authentication and per-client quotas.

## Step 1: predictions, written before any code

Go into the debug log entry before the first run:

1. On which thread does the `map` that builds the SSE event run: Netty `reactor-http-nio-N`, a
   Mongo driver thread (`multiThreadIoEventLoopGroup-*`), or `boundedElastic`? And is it the same
   thread for every event?
2. If the filter value is the path string instead of an `ObjectId`, what does the client see: an
   error, or silence?
3. Does the change stream publisher honour downstream demand, or does it fetch ahead? How would a
   test tell the difference?
4. After `curl` is interrupted, how long until the cursor disappears from the server, and what is
   the first signal that arrives in the pipeline (`doOnCancel`, an error, or nothing until the next
   heartbeat)?
5. Is `Last-Event-ID` inclusive or exclusive of the item the token belongs to?
6. For an expired resume token, is the response already committed when the error arrives?

## Step 2: tests

All tests run against the Testcontainers MongoDB replica set, with a real HTTP exchange through
`WebTestClient` bound to a server (not mocked controller), so cancellation is a real disconnect.

- Insert after subscribe: one `item` event with every DTO field; `id` is a non-empty token.
- Insert for another feed: no event inside a short bounded window; then an insert for the right
  feed arrives, proving the stream was alive and not just silent.
- Duplicate fetch (same `guid`): no second event.
- Resume: take the token of event 1, disconnect, insert items 2 and 3, reconnect with
  `Last-Event-ID`: receives 2 and 3, not 1, in order.
- Invalid token: 400 `ApiError`. Unknown feed: 404 `ApiError`.
- Heartbeat: with a millisecond interval, a comment frame arrives on an idle stream.
- Cancellation: baseline `serverStatus.metrics.cursor.open.total`, open a stream, assert it rose,
  cancel the client, assert it returns to baseline within a bounded polling window (no fixed sleep).
- Backpressure: a subscriber that requests nothing while more than the buffer capacity of items is
  inserted terminates with the overflow error, and delivered items are a prefix with no gap.

## Step 3: mutations

Each must make at least one test fail; a surviving mutation is a missing test, recorded as a finding.

1. Remove the `feedId` clause from the `$match`.
2. Compare `feedId` as a string instead of an `ObjectId`.
3. Remove `resumeAfter`.
4. Remove the heartbeat termination (`takeUntilOther`).
5. Replace the bounded buffer with an unbounded one.
6. Replace the bounded buffer with `onBackpressureDrop`.
7. Drop the `insert` operation filter (so a delete or update produces an event).

## Step 4: Debug run

1. Docker up (`docker compose up -d`), application in the IDE under the debugger on a free port.
2. Create a feed; open `curl -N http://localhost:<port>/feeds/<id>/items/stream`.
3. Breakpoints: the lambda that builds the `ServerSentEvent`, and the `doOnCancel` (or equivalent)
   on the stream. Suspend policy `Thread`, not `All`: a suspended event-loop thread otherwise
   starves timers and can trigger timeouts that are artefacts of the breakpoint (Day 113 lesson).
4. In a second terminal `POST /feeds/<id>/fetch` against a local RSS source. Record thread name,
   top of stack, and the event payload at the breakpoint.
5. Interrupt `curl`. Record whether and where `doOnCancel` fires, then check in `mongosh` that the
   cursor is gone (`db.serverStatus().metrics.cursor.open`).
6. Write a Bug entry in `debug-log.md`: predictions from Step 1 first, then observations, then
   which prediction was wrong.

## Definition of done

Contract decisions 1-12 each map to a test or to an explicit "not covered" line; seven mutations
killed or recorded; docs updated (`README.md`, `curls.md`: endpoint, resume with `Last-Event-ID`,
heartbeat and buffer properties, known gaps); Debug run recorded; cold review of the diff after the
tests are green.
