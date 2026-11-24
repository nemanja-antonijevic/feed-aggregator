# Task: close the three open M2 items in the feed source client

## Goal

M2 (`POST /feeds/{id}/fetch`, `FeedSourceClient`) was shipped with three items written down in
`README.md` under Known limitations. All three live in the path between the stored feed URL and the
bytes handed to the parser. This task closes them in three independent parts, in this order, one commit each.

## Current behavior (read from the code on 2026-11-24)

- `FeedSourceClient.fetch` maps `DataBufferLimitException`, `TimeoutException`, a Netty
  `ReadTimeoutException` in the cause chain, `WebClientRequestException` and `PrematureCloseException`.
  Nothing else is mapped. Any other exception from the client reaches `ApiExceptionHandler` unmapped and
  becomes Spring's default `500` shape. `FeedPollingCoordinator.isRetryable` retries only
  `FeedSourceTimeoutException`, `FeedSourceConnectionException` and 5xx `FeedSourceHttpException`.
- `FeedSourceClient` calls `.uri(url)` with a `String`, which WebClient treats as a URI *template*.
  Whether a stored URL containing `{` can make this throw is **unverified**; part A must find out by
  test, not assume.
- For a body over 1 MiB the API correctly returns `502`, but Reactor Netty logs a post-termination
  `IllegalReferenceCountException`. The cause is unknown (README, Known limitations).
- The URL is only checked as an absolute `http`/`https` URL (`AbsoluteHttpUrlValidator`). The server will
  connect to loopback, private, link-local and metadata addresses. Redirects are not followed.
- The integration tests host feed sources on `MockWebServer` (loopback), so any address block breaks them
  unless it is switchable.

## Decisions (proposed 2026-11-24; **A1, C1, C2 and C3 need the owner's confirmation before implementation**)

### Part A: terminal error mapping

- **A1.** Any exception leaving `FeedSourceClient.fetch` that is not already one of the feed-source
  exceptions (`FeedSourceRequestException` and its subclasses, `FeedSourceTimeoutException`,
  `InvalidFeedSourceException`) is mapped by one last `onErrorMap` to
  `FeedSourceRequestException("feed source request failed", cause)` → `502`. It is **not** retryable
  (an unknown failure is not assumed transient) and a `WARN` log carries the cause's class name,
  because the mapping would otherwise hide it. Fatal errors are not touched (Reactor rethrows them
  before `onErrorMap`).
- **A2.** The terminal map must be the last operator and must not re-wrap an already mapped exception.
- **A3.** If a synchronous failure from `.uri(url)` (the URI-template question) is proven, it is
  fixed at the failing hop (build the URI without template expansion), not hidden by the terminal map.
  If it is not reproducible, say so in the README and the test names.

### Part B: `IllegalReferenceCountException` on an oversized body

- **B1.** Reproduce first, through the real client against a `MockWebServer` answering > 1 MiB (not
  through the stubbed `ExchangeFunction`), with `-Dio.netty.leakDetection.level=paranoid`, and record
  the Reactor Netty and Netty versions.
- **B2.** Decision rule, by evidence:
  1. a `LEAK:` message appears → a real leak; fix it at the source of the read (candidate: read the body
     as `DataBuffer`s and join them with `DataBufferUtils.join(flux, maxBytes)`, which owns the release);
  2. no `LEAK:`, the exception is dropped noise from a known upstream issue → pin the evidence in the
     README (issue link or version), keep the behavior test, and say plainly that it is *not fixed*;
  3. no `LEAK:` and the exception disappears with the candidate read → adopt it.
  Lowering a logger level to hide the message is **not** an accepted outcome.
- **B3.** The regression test captures the log output of the real-client oversized-body case and
  asserts no `IllegalReferenceCountException` is logged (outcomes 1 and 3), or asserts the documented
  behavior (outcome 2).

### Part C: SSRF on the fetch path

- **C1.** Enforced at connection time, in a custom Reactor Netty `AddressResolverGroup` wrapped around
  the default one, so the check is applied to the exact addresses the client will connect to (a host name
  that resolves to a private address, and DNS rebinding between a check and the connect, are both covered).
  Not done at `POST /feeds`: creation behavior does not change.
- **C2.** Blocked destinations: loopback, any-local (`0.0.0.0`, `::`), link-local (including
  `169.254.169.254`), site-local (`10/8`, `172.16/12`, `192.168/16`), IPv6 unique-local `fc00::/7`,
  carrier-grade NAT `100.64/10`, and multicast. An IPv4-mapped IPv6 address is judged as its IPv4 address.
  Every address a host resolves to must be allowed, otherwise the request is refused.
- **C3.** A blocked destination fails as `InvalidFeedSourceException("feed source address is not
  allowed")` → `422`, not retryable. The switch is `feeds.source.allow-private-addresses`, default
  `false`; the existing `MockWebServer` integration tests set it to `true` in their test properties,
  and README documents that local demos against `localhost` need it.
- **C4.** Out of scope: port restrictions, authentication, allow-lists, following redirects.

## Tests

- **A:** unit tests on `FeedSourceClient` with a stubbed `ExchangeFunction`: an unmapped exception type
  becomes `FeedSourceRequestException` with the cause kept; an already mapped one is not re-wrapped
  (the same instance); the polling coordinator does not retry it. HTTP `502` through the controller.
- **B:** the real-client oversized-body test from B3, plus the existing stubbed one unchanged.
- **C:** unit tests of the resolver group with a stubbed delegate (no DNS): each blocked range at its
  first and last address, the neighbouring public address allowed, IPv4-mapped IPv6, a host with one
  public and one private answer is refused, `allow-private-addresses=true` lets loopback through.
  Integration: with the switch off, fetching a `MockWebServer` feed returns `422` and the polling
  status records a non-retryable failure; with it on, the existing tests stay green.
- **Mutations the suite must kill:** A: drop the "not already mapped" guard; make the new exception
  retryable. C: drop each blocked range one at a time (or the multicast / IPv4-mapped rule); check only
  the first resolved address; invert the switch default; treat `169.254.0.0/16` as allowed.

## Docs

README Known limitations: remove or rewrite the three entries (`IllegalReferenceCountException`,
"Source errors are mapped by type", SSRF), document the new property and the `422`; add a short note to
this file's verification section with the measured test totals and the mutation table.

## Out of scope

Redirect following, cascade delete of items, the 3 s overall budget, URL normalization, leader election
for polling, and any Docker/JDWP work.

## Verification

- Baseline before Part A (`./mvnw -q clean verify`, 2026-11-24): 154 tests, 0 failures, 0 errors;
  BlockHound canary green.
- Part A discovery: before the fix, the new unknown-error test received the original
  `IllegalStateException`. `.uri(String)` on `https://example.com/{feed}` threw synchronously with
  `IllegalArgumentException: Not enough variable values available to expand 'feed'`; the stubbed
  `ExchangeFunction` was never called. The final hop encodes braces to `%7B`/`%7D`, passes a `URI`
  (no template expansion), and preserves the valid `%20` escape.

- Part A test-first: `mapsUnmappedExchangeExceptionToRequestFailureAndKeepsCause` first failed with the original `IllegalStateException`; the URI regression test first failed because `.uri(String)` threw the synchronous template-expansion `IllegalArgumentException`.
- Part A mutation copy: removing the already-mapped guard was killed by `returnsAlreadyMappedRequestExceptionAsTheSameInstance`; making `FeedSourceRequestException` retryable was killed by `doesNotRetryFailureOutsideAllowlist`. Each mutant was restored before the next.
- Part A (`./mvnw clean verify`): 159 tests, 0 failures, 0 errors; BlockHound canary green.
- Part A mutations in isolated copy: removing the already-mapped guard was killed by
  `returnsAlreadyMappedRequestExceptionAsTheSameInstance`; making `FeedSourceRequestException`
  retryable was killed by `doesNotRetryFailureOutsideAllowlist`. Both mutants were restored between
  runs and the temporary copy was removed.
- Part A final (`./mvnw clean verify`, 2026-11-24): 159 tests, 0 failures, 0 errors; BlockHound canary green.
- Part B B1/B3 (`-Dio.netty.leakDetection.level=paranoid`): the real WebClient received an 8 MiB body, returned `FeedSourceBodyTooLargeException`, and the captured root log showed no `LEAK:` and no `IllegalReferenceCountException` on Reactor Netty 1.3.7 / Netty 4.2.17.Final. Upstream evidence: Reactor Netty #1374 (closed; downstream/DataBufferLimit-related post-termination symptom) and Spring Framework #26060. B2 outcome 2: no application-level read fix is claimed; the regression test asserts the size error and no leak report while recording any ILE if it reappears.
- Part B B1/B3 (`-Dio.netty.leakDetection.level=paranoid`): a real WebClient/MockWebServer 8 MiB body returns `FeedSourceBodyTooLargeException`. Captured root logs showed neither `LEAK:` nor `IllegalReferenceCountException`; Reactor Netty 1.3.7 / Netty 4.2.17.Final. B2 outcome 2 documented with Reactor Netty #1374 and Spring Framework #26060; no app-level reader rewrite.
- Part B final (`./mvnw -q clean verify`, 2026-11-24): 160 tests, 0 failures, 0 errors; BlockHound canary green.
- Part A final (`./mvnw clean verify`, 2026-11-24): 159 tests, 0 failures, 0 errors; BlockHound canary green.

## Implementation verification (2026-11-24)

- Decisions A1, C1, C2 and C3 were implemented as proposed; no explicit confirmation of them was recorded.
- **A:** an unmapped exception becomes `FeedSourceRequestException("feed source request failed")` (`502`,
  not retryable, `WARN` with the cause class). A stored URL containing `{` is escaped before `.uri(URI)`;
  note that `AbsoluteHttpUrlValidator` (`new URI`) already rejects `{` at creation, so the template
  failure is only reachable for a document written to Mongo directly.
- **B:** outcome 2. On Reactor Netty 1.3.7 / Netty 4.2.17.Final an 8 MiB body with paranoid leak detection
  logs neither `LEAK:` nor `IllegalReferenceCountException`; the real-client test now asserts both.
  Not fixed, because it was not reproducible; the upstream references are in the README.
- **C:** the resolver refuses a host if any resolved address is blocked. Review found that WebClient wraps
  the refusal in `WebClientRequestException`, which `FeedSourceClient` then mapped to the retryable
  `FeedSourceConnectionException` (`502`) instead of the `422` of C3. Measured with a throwaway test in a
  copy, then fixed with a test-first change: the wrapped `InvalidFeedSourceException` is returned as is.
  Three tests failed first (`502` instead of `422`), and pass now: a unit test, a real-client test with the
  switch off (source never contacted), and an API test with the switch off (`422`).
- `./mvnw -q clean verify`: **207 tests**, zero failures, errors or skips (baseline before the task: 154).
- Mutations, one at a time on a separate copy, with a no-change control that stayed green. All 19 were
  detected: drop the "not already mapped" guard; unmapped exception retryable; no unwrap of the blocked
  address; each of the ranges any-local, loopback, `169.254/16`, `10/8`, `172.16/12`, `192.168/16`,
  `100.64/10`, IPv4 multicast, IPv6 any-local, IPv6 loopback, IPv6 link-local, IPv6 multicast, IPv6
  unique-local; the IPv4-mapped rule; checking only the first resolved address; switch default `true`.
- Two harness results were discarded and redone: one mutation did not compile, and the IPv6 loopback
  mutation **survived** the first run. `::1` had no test case, so the test was added and the mutation
  is now killed.
- Not covered, documented in the README: the deprecated `fec0::/10` range and NAT64 (`64:ff9b::/96`).
