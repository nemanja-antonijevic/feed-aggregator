# Task: catch and fix a blocking call in reactive code (M6)

## Scope

Install BlockHound for the tests, run the existing suite under it, plant one blocking call on an
event-loop thread, watch BlockHound catch it, then fix it with `Schedulers.boundedElastic()`.
No new production feature. Nothing beyond this.

## Contract decisions

1. BlockHound is a test-scope dependency with a pinned version (the Spring Boot BOM does not manage
   it). On JDK 21 the surefire `argLine` in `pom.xml` needs
   `-XX:+AllowRedefinitionToAddDeleteMethods`.
2. `BlockHound.install` is called once per JVM, from one place, before the first Spring context.
3. A canary test proves it is active: `Thread.sleep` inside a `Mono` subscribed on
   `Schedulers.parallel()` fails with `BlockingOperationError`. "No detections" in the suite counts
   only next to a green canary.
4. If the existing suite reports something, it is recorded verbatim in the debug log, then fixed
   (our code) or allowlisted with a one-line reason (framework or class loading). No blanket
   allowances.
5. The planted blocking call is `Thread.sleep(100)` inside the `flatMap` of
   `FetchFeedService.fetch`. With BlockHound it must fail with `BlockingOperationError`; after
   wrapping it in `Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())` the same test
   is green. The plant is reverted afterwards; only BlockHound and the canary stay in the repo.

## Not covered

JDWP attach and Docker (Debug runs dropped on 2026-11-16), event-loop starvation measurement,
blocking on Mongo driver threads, BlockHound as a CI gate.

## Predictions, written before the first run

1. Does the existing suite report anything with the default configuration?
2. `FeedIndexConfig` calls `.block()` at startup: reported or not, and why?
3. In decision 5, which error arrives first: Reactor's own `IllegalStateException` (if `.block()` is
   used) or BlockHound's `BlockingOperationError` (for `Thread.sleep`)?

## Definition of done

Canary green, baseline result recorded, plant caught and fixed, `clean verify` green and rerun
independently, `README.md` gets a short "BlockHound" note, plan §4 row M6 checked with its
`Debugger` cell corrected.
