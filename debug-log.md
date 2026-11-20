# Debug log

## Bug 1: M5 live item stream

### Step 1 — predictions (before the first run)

1. **SSE event mapping thread:** I predict the `map` that builds each `ServerSentEvent` will run on a MongoDB driver event-loop thread (`multiThreadIoEventLoopGroup-*`), because the change-stream publisher emits there and there is no scheduler boundary in the pipeline. I do not expect one fixed thread across every event; the driver may deliver later batches on another event-loop thread. I will verify this by recording the thread name at the mapping breakpoint for several inserts.
2. **String `feedId` in the server-side match:** I predict the client will see silence, not an error. MongoDB will compare a BSON string with the stored BSON `ObjectId`, so the match will simply return no events. I will verify by subscribing, inserting for that feed, and observing the stream over a bounded window.
3. **Change-stream demand and prefetch:** I predict the reactive publisher honors Reactive Streams demand, while Reactor or the Mongo driver may still fetch a bounded batch ahead internally. With the required backpressure buffer, I expect the operator to request upstream independently of the downstream's zero demand, so inserting more than the configured capacity should overflow. A zero-demand subscriber plus a request probe at the publisher boundary (and the overflow test) will distinguish downstream delivery from upstream fetching.
4. **Cancellation and cursor cleanup:** I predict interrupting `curl` will trigger `doOnCancel` immediately and the server cursor will return to baseline within a short bounded polling window (under a few seconds), without waiting for the next heartbeat. I will record the cancellation signal and poll `serverStatus.metrics.cursor.open.total` until it returns to baseline.
5. **Resume-token boundary:** I predict `resumeAfter` is exclusive: reconnecting with event 1's token will not emit event 1 again, and will emit later inserts in order. I will verify with the resume integration test.
6. **Resume token error response commitment:** I predict a resume error will arrive before the first heartbeat or item is written, leaving the response uncommitted and allowing a normal terminal error response. **Outcome:** a well-formed token pointing at an absent position returned JSON `ApiError` with HTTP 410 before the SSE response committed. MongoDB 8.0 returned code 280 (`ChangeStreamFatalError`, `NonResumableChangeStreamError`, “resume token was not found”), not 286. This proves the unknown-token path, not an actual oplog rollover; code 286 classification is now separately unit-tested.

### Observations

Pending first run.

## Zadatak 2 — predikcije pre M0 i izmene helpera

1. **Pet testova sa starim helperom pod M0:** predviđam da `sendsInsertedItemWithResumeTokenAndCompleteDto` i `resumesExclusivelyAfterLastEventId` padnu timeout-om, jer prvi insert stiže pre nego što se odloženi change stream pretplati. Predviđam i da `duplicateFetchDoesNotEmitASecondInsert` može da padne istim putem ako prvi fetch završi u okviru 300 ms. `ignoresOtherFeedAndThenDeliversRequestedFeed` i `ignoresUpdatesAndStillDeliversLaterInserts` očekujem da prođu: prvi ciljani insert se šalje posle prozora od 180 ms, a u drugom testu se novi insert šalje posle update-a i dodatnog prozora od 180 ms, pa se oba dešavaju nakon odloženog otvaranja kursora.
2. **`ignoresOtherFeedAndThenDeliversRequestedFeed` pod M0:** predviđam da prolazi i uz uklonjeni `feedId` uslov. Tuđi feed se ubacuje pre otvaranja kursora, pa se taj insert ne replay-uje; traženi feed se ubacuje kasnije i zadovoljava proveru. `expectNoEvent` sam po sebi ne potvrđuje da je kursor već otvoren.

Izabrani signal: **A**, `serverStatus.metrics.cursor.open.total`, jer je to merilo koje ugovor već koristi za proveru stvarno otvorenog server-side kursora. Stabilizovaću baseline pre testa da smanjim mogućnost preplitanja zatvaranja prethodnog kursora sa otvaranjem novog.


### M0 — stari helper i popravljen helper

Sa M0 (`changeEvents.delaySubscription(Duration.ofMillis(300))`) i starim helperom pali su 4 od 8 testova:

- `sendsInsertedItemWithResumeTokenAndCompleteDto` — `nextItem` je istekao posle 5 s.
- `resumesExclusivelyAfterLastEventId` — prvi `nextItem` je istekao posle 5 s.
- `duplicateFetchDoesNotEmitASecondInsert` — StepVerifier je istekao čekajući prvi događaj.
- `ignoresUpdatesAndStillDeliversLaterInserts` — StepVerifier je istekao čekajući novi insert.

`ignoresOtherFeedAndThenDeliversRequestedFeed` je prošao: insert drugog feeda nastao je dok je cursor pretplata bila odložena, pa nije stigao kao događaj. Zato je i `feedId` mutacija mogla da preživi ovaj redosled. Predikcija da će `ignoresUpdatesAndStillDeliversLaterInserts` proći bila je pogrešna; prozor i rad baze nisu garantovali da se kursor otvori pre narednog inserta.

Sa novim helperom i istom M0 mutacijom svih 8 testova je prošlo. Helper uzima `serverStatus.metrics.cursor.open.total` pre zahteva i čeka porast do 5 s. `@BeforeEach` sačeka povratak na idle baseline zabeležen pre prvog testa, pa stabilizuje brojač kroz uzastopne uzorke postojeće 25 ms petlje. To sprečava da zatvaranje starog kursora sakrije otvaranje novog. Nije dodat fiksni sleep.

**Reflection (Radio): A** — broj otvorenih kursora preko `serverStatus.metrics.cursor.open.total`; to je ugovoreno merilo server-side kursora. Idle baseline se stabilizuje pre svakog testa.

## Zadatak 3 — predikcije pre M-cancel i izmene testa

1. **Kursor pod M-cancel:** predviđam da će kursor ostati otvoren posle klijentovog `dispose`, jer `publish().autoConnect()` ne propagira otkazivanje poslednjeg downstream pretplatnika Mongo change stream-u. Idle heartbeat treba da drži SSE tok živim.
2. **Stari test pod M-cancel:** predviđam da neće proći. `openStream` već čeka da count poraste i vraća aktivnu vezu; zatim test ponovo uzima baseline i čeka još jedan porast. Očekujem timeout na tom suvišnom `awaitCursorCount(count -> count > baseline, ...)` pre `dispose`, pa stari test ne bi stigao da proveri curenje posle otkazivanja.


### M-cancel — ishod starog i novog testa

Sa `changeEvents.publish().autoConnect()` stari test je pao na liniji 307, pre klijentovog `dispose`: čekao je dodatni rast brojača preko baseline-a koji je već sadržao otvoren kursor. Zato stari test pod ovom mutacijom nije stigao da proveri da li otkazivanje zatvara kursor.

Novi test je pod istom mutacijom dokazao curenje: pre `dispose` stigao je SSE heartbeat frejm, bez prethodnog `onError` ili `onComplete`; posle `dispose` strogo čekanje je isteklo sa očekivanim idle baseline-om `0`, a viđeni broj je ostao `1`. Pad je na proveri zatvaranja, ne u `openStream`.

**Not covered:** sa heartbeat intervalom od 40 ms ovaj test ne razlikuje zatvaranje zbog prekida konekcije od zatvaranja posle neuspelog sledećeg heartbeat upisa. Ugovor kaže da prekid idle konekcije postaje vidljiv pri sledećem upisu; razliku treba izmeriti u Debug run-u kada se nastavi Step 4.5.

Jedina provera u ovom zadatku koja nije mutaciono dokazana jeste zastavica terminalnog signala pre `dispose` (`onComplete` i `onError`). Test je čita neposredno pre otkazivanja; kratkotrajni self-complete nije pouzdano simuliran jer bi se trkao sa helperom.

**Reflection (Radio):** stvarna rupa u starom testu bila je to što nije posmatrao `onComplete`; stream koji se sam završi mogao je da zatvori kursor i učini test zelenim bez klijentovog otkazivanja.


## Poligon M5 — neupotrebljiv resume token i overflow preko HTTP

- `_data` resume tokena počinje bajtom verzije `82`; narednih 8 bajtova su BSON Timestamp: 4 bajta sekundi i 4 bajta inkrementa. Test čuva ostatak stvarnog tokena, menja timestamp na `Timestamp(0, 1)` i proverava da je izmenjeni token prosleđen.
- Ovo je nepoznat token sa odsutnom pozicijom, ne token čiji je oplog entry dokazano prepisan. MongoDB 8.0/Testcontainers vraća kod 280 `ChangeStreamFatalError`, label `NonResumableChangeStreamError`, poruku `resume token was not found` — ne kod 286. Pet timestamp varijanti dalo je isti 280. Mapper klasifikuje 286 i samo ovu usku 280 kombinaciju kao `ChangeStreamHistoryLost`; ostali 280 error-i ostaju server greške.
- HTTP zahtev sa tim tokenom vraća JSON `ApiError`, status 410 pre SSE commitment-a. To dokazuje unknown-token → 410; stvarni oplog rollover nije simuliran.
- Overflow ostaje pokriven unit testom; **not covered via HTTP** jer se overflow javlja nakon SSE frejmova koji commit-uju response, a TCP/Netty bufferi čine slow-reader HTTP test transport-dependent. Uklonjen je `handleItemStreamBackpressure` HTTP handler; `ItemStreamBackpressureException` i dalje označava terminalni stream overflow.

### Step 3 — sedam ugovornih mutacija

| Mutacija | Ishod | Test i opažanje |
|---|---|---|
| 1. Uklonjen feedId iz `$match` | **Oborena** | `ignoresOtherFeedAndThenDeliversRequestedFeed` je primio item drugog feeda tokom `expectNoEvent`. |
| 2. feedId poređen kao string | **Oborena** | `sendsInsertedItemWithResumeTokenAndCompleteDto` je istekao čekajući insert. |
| 3. Uklonjen `resumeAfter` | **Oborena** | `resumesExclusivelyAfterLastEventId` nije dobio iteme 2 i 3 i istekao je. |
| 4. Uklonjen `takeUntilOther` za heartbeat | **Oborena** | `heartbeatFluxCompletesWhenItemFluxCompletes` dobio je dodatni keepalive umesto completion-a. |
| 5. Bounded buffer zamenjen neograničenim | **Oborena** | `boundedItemBufferErrorsWithoutDroppingItsQueuedPrefix` istekao je jer overflow greška nije stigla. Ovo je unit pokriće, ne HTTP. |
| 6. Bounded buffer zamenjen `onBackpressureDrop` | **Oborena** | Isti unit test istekao je bez terminalne overflow greške; drop varijanta nije prijavila gubitak. |
| 7. Uklonjen insert operation filter | **Oborena** | `ignoresUpdatesAndStillDeliversLaterInserts` je primio update tokom `expectNoEvent`. |

Mutacije su pokretane pojedinačno; produkcioni fajl je vraćan iz sačuvane kopije nakon svakog pokušaja.

### Cold review follow-up — response negotiation and classifier guards

- `malformedIdsAndResumeTokensReturnJsonApiErrorsBeforeStreaming` now sends `Accept: text/event-stream` for unparsable resume token, malformed feed id and missing feed. Each still returns its expected `ApiError` status and `application/json`; the 410 handler also explicitly selects JSON.
- Unit coverage now exercises code 286 mapping, Mongo 8 code 280 with the full code-name/label/message fingerprint, and negative 280 cases with each fingerprint condition changed or missing. Only the exact non-resumable “resume token was not found” command error maps to history-lost; other 280 errors pass through unchanged.
- The integration test is named for an **unknown/unavailable** token. It does not claim that the oplog entry was actually rolled off. The production 410 policy covers both an unknown token and an expired token because clients recover the same way: reconnect without `Last-Event-ID`. Actual oplog rollover remains not covered.

## M6 BlockHound — predikcije pre instalacije

1. **Baseline sa podrazumevanom konfiguracijom:** predviđam da postojeća testna klasa ne prijavi blokirajući poziv u kodu aplikacije. `FetchFeedService.parse` je na `boundedElastic`; `FeedIndexConfig` poziva `.block()` pri podizanju Spring konteksta na glavnoj startup niti; M5 SSE `map` pravi DTO i resume id bez blokirajućeg I/O. Blokirajući `.block()` pozivi u testovima su na testnim nitima, ne na Reactor non-blocking workerima.
2. **Startup `.block()`:** predviđam da `FeedIndexConfig` neće prijaviti grešku. `SmartInitializingSingleton` se poziva tokom context refresh-a na glavnoj niti, koju BlockHound ne tretira kao non-blocking.
3. **Parsiranje bez `subscribeOn`:** predviđam da će CPU parsiranje samo po sebi ostati tiho; `XmlReader` čita iz `ByteArrayInputStream`. Prvi poziv može ipak prijaviti blokiranje ako Rome/JAXP uradi instrumentisan class/resource lookup. Ako se to desi samo pri prvom korišćenju, klasifikovaću ga odvojeno od CPU rada.
4. **`Thread.sleep` u M5 `map`-u:** predviđam da će podrazumevani BlockHound ostati tih na `multiThreadIoEventLoopGroup-*`, jer Mongo driver nit verovatno ne implementira Reactor `NonBlocking` marker. Canary za `parallel()` treba da potvrdi da je instalacija aktivna i da ovaj tihi rezultat nije neispravna instalacija.
5. **`.block()` na Netty niti:** predviđam da će Reactor-ova provera u `Mono.block()` prvo baciti `IllegalStateException` pre nego što BlockHound stigne do park/wait operacije.
6. **Starvation merenje:** predviđam da će nezavisan `GET /feeds`, poslat 50 ms nakon početka 500 ms CPU spin-a na jedinom event-loop workeru, odgovoriti posle približno još 450 ms. Isti GET bi na `boundedElastic` trebalo da ostane brz; CPU spin sam po sebi BlockHound neće prijaviti.


### M6 installation checkpoint

- BlockHound `1.0.11.RELEASE` was resolved before adding it to `pom.xml`; `mvn dependency:get` downloaded its POM and JAR directly through `https://nexus.tmaws.io/nexus/content/groups/com.ticketmaster.marketplace/`.

### M6 baseline — BlockHound default, pre-allowlist

Predikcija 1 je bila pogrešna ako se broje sve detekcije: suite je prijavio šest istih BlockHound detekcija i završio sa 154 testa, 2 neuspeha i 68 grešaka. Nijedna detekcija nema frame u `com.nantonijevic.feeds`; Spring/Mongo konteksti koji su delili ovu Mongo grešku pali su pri startu.

Šest detekcija, doslovno iz passive callback-a:

```text
BLOCKHOUND_DETECTION blocking=java.io.FileInputStream#readBytes thread=multiThreadIoEventLoopGroup-2-3 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=java.io.FileInputStream#readBytes thread=multiThreadIoEventLoopGroup-3-3 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=java.io.FileInputStream#readBytes thread=multiThreadIoEventLoopGroup-5-3 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=java.io.FileInputStream#readBytes thread=multiThreadIoEventLoopGroup-6-3 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=java.io.FileInputStream#readBytes thread=multiThreadIoEventLoopGroup-8-3 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=java.io.FileInputStream#readBytes thread=multiThreadIoEventLoopGroup-9-3 firstAppFrame=<none>
```

Reprezentativni stack iz baseline-a:

```text
reactor.blockhound.BlockingOperationError: Blocking call! java.io.FileInputStream#readBytes
  at java.base/java.io.FileInputStream.readBytes(FileInputStream.java)
  at java.base/java.io.FileInputStream.read(FileInputStream.java:287)
  at java.base/java.io.FilterInputStream.read(FilterInputStream.java:119)
  at java.base/sun.security.provider.NativePRNG$RandomIO.readFully(NativePRNG.java:432)
  at java.base/sun.security.provider.NativePRNG$RandomIO.ensureBufferValid(NativePRNG.java:535)
  at java.base/sun.security.provider.NativePRNG$RandomIO.implNextBytes(NativePRNG.java:554)
  at java.base/sun.security.provider.NativePRNG.engineNextBytes(NativePRNG.java:224)
  at java.base/java.security.SecureRandom.nextBytes(SecureRandom.java:768)
  at java.base/java.util.UUID.randomUUID(UUID.java:153)
  at com.mongodb.internal.session.ServerSessionPool.createNewServerSessionIdentifier(ServerSessionPool.java:201)
  at com.mongodb.internal.session.ServerSessionPool$ServerSessionImpl.<init>(ServerSessionPool.java:214)
  at com.mongodb.internal.session.ServerSessionPool.get(ServerSessionPool.java:94)
  at com.mongodb.internal.session.BaseClientSessionImpl.getServerSession(BaseClientSessionImpl.java:143)
  at com.mongodb.internal.connection.ClientSessionContext.getSessionId(ClientSessionContext.java:48)
  at com.mongodb.internal.connection.CommandMessage.getExtraElements(CommandMessage.java:344)
  at com.mongodb.internal.connection.CommandMessage.writeOpMsg(CommandMessage.java:245)
  at com.mongodb.internal.connection.CommandMessage.encodeMessageBody(CommandMessage.java:235)
  at io.netty.channel.SingleThreadIoEventLoop.runIo(SingleThreadIoEventLoop.java:225)
```

Classification: Mongo driver internals seed JDK `SecureRandom` while generating a server-session UUID on an I/O event loop. This is not an application frame; decision 5 permits one narrow allowance on `ServerSessionPool.createNewServerSessionIdentifier`, with the trace and reason documented. The observed detection also disproves prediction 4's “silent” outcome: BlockHound treats these Mongo I/O threads as non-blocking by default.

The canary emitted its expected detection separately:

```text
BLOCKHOUND_DETECTION blocking=java.lang.Thread.sleep0 thread=parallel-4 firstAppFrame=com.nantonijevic.feeds.testing.BlockHoundCanaryTest.lambda$detectsThreadSleepOnParallelScheduler$0(BlockHoundCanaryTest.java:16)
```


### Rerun posle ServerSessionPool allowance-a

Preostale su 13 `Unsafe.park` detekcija. Sirovi callback zapisi:

```text
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-8-4 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-8-3 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-3 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-4 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-5 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-6 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-6 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-6 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-3 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-6 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-6 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-8 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-7 firstAppFrame=<none>
```

Sirovi stack-ovi grupišu se u pet poziva kroz `ClusterClock.advance`, četiri kroz `DefaultConnectionPool$OpenConcurrencyLimiter.tryHandOverOrRelease` i tri kroz njegovu ugnježdenu `ConcurrentPool$StateAndPermits.releasePermit`. Sve su Mongo driver interne putanje, bez aplikacionog frame-a. Suite je imao 154 testa i 13 neuspeha.


### Rerun posle ClusterClock i OpenConcurrencyLimiter allowances

Još je ostalo 11 `Unsafe.park` callback događaja; sirovi zapisi:

```text
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-8-3 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-8-4 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-3 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-5 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-6 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-7 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-8 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-9 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-10 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-4 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-12 firstAppFrame=<none>
```

Deset stack-ova koje je Surefire priložio vode kroz `DefaultConnectionPool$OpenConcurrencyLimiter.tryHandOverOrRelease` i njegov `ConcurrentPool.StateAndPermits.releasePermit`; jedan callback događaj nije imao odgovarajući test stack. Ovaj run je imao 154 testa i 11 neuspeha, sa pool čekanjima, pa nije clean baseline. Dodavanje uskog `ConcurrentPool.StateAndPermits.releasePermit` allowlist-a uklonilo je pool detekciju u daljem ciljanom concurrency testu.


### Ciljani FeedApi concurrency baseline pre Jackson allowance-a

`FeedApiIntegrationTest.concurrentPostsWithSameUrlCreateExactlyOneFeed` je proizveo osam `Unsafe.park` događaja u Jackson-ovom lazy deserializer cache-u; prvi app frame nije pronađen. Sirovi zapisi:

```text
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=parallel-3 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=parallel-9 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=parallel-7 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=parallel-1 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=parallel-5 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=parallel-6 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=parallel-8 firstAppFrame=<none>
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=parallel-2 firstAppFrame=<none>
```

Reprezentativni stack je `Unsafe.park -> ReentrantLock.lock -> tools.jackson.databind.deser.DeserializerCache._createAndCacheValueDeserializer -> findValueDeserializer -> ObjectReader` pri konkurentnom dekodiranju `CreateFeedRequest`. Test je pao jer je BlockHound prekinuo osam POST zahteva pre očekivanih 409 odgovora. Kao framework first-use cache contention, a bez app frame-a, dodata je jedna method-level allowance za `DeserializerCache._createAndCacheValueDeserializer`. Ciljani test je zatim prošao.

**Allowlist rationale:** entries targetuju samo Mongo/Jackson metode iz gornjih stack-ova; nijedan ne pokriva `com.nantonijevic.feeds`. Nema package/wildcard allowance ni dodatnog thread predicate-a. Puni baseline sa kompletnim setom i canary-jem još treba da potvrdi clean rezultat.


### M6 baseline follow-up — Mongo connection `releasePermit`

A later baseline callback found one more driver-only path, after the earlier Mongo/Jackson entries were allowed:

```text
BLOCKHOUND_DETECTION blocking=jdk.internal.misc.Unsafe#park thread=multiThreadIoEventLoopGroup-9-5 firstAppFrame=<none> trace=java.base/jdk.internal.misc.Unsafe.park(Unsafe.java) <- java.base/java.util.concurrent.locks.LockSupport.park(LockSupport.java:221) <- java.base/java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:754) <- java.base/java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:990) <- java.base/java.util.concurrent.locks.ReentrantLock$Sync.lock(ReentrantLock.java:153) <- java.base/java.util.concurrent.locks.ReentrantLock.lock(ReentrantLock.java:322) <- com.mongodb.internal.Locks.checkedWithLock(Locks.java:59) <- com.mongodb.internal.Locks.withLock(Locks.java:55) <- com.mongodb.internal.Locks.withLock(Locks.java:33) <- com.mongodb.internal.connection.DefaultConnectionPool$OpenConcurrencyLimiter.releasePermit(DefaultConnectionPool.java:1105) <- com.mongodb.internal.connection.DefaultConnectionPool$OpenConcurrencyLimiter.lambda$openWithConcurrencyLimitAsync$0(DefaultConnectionPool.java:1033) <- com.mongodb.internal.connection.DefaultConnectionPool$PooledConnection.lambda$openAsync$0(DefaultConnectionPool.java:659) <- com.mongodb.internal.connection.UsageTrackingInternalConnection.lambda$openAsync$0(UsageTrackingInternalConnection.java:72) <- com.mongodb.internal.connection.InternalStreamConnection.lambda$finishHandshakeAsync$0(InternalStreamConnection.java:300) <- com.mongodb.internal.connection.InternalStreamConnectionInitializer.finishHandshakeAsync(InternalStreamConnectionInitializer.java:124) <- com.mongodb.internal.connection.InternalStreamConnection.finishHandshakeAsync(InternalStreamConnection.java:292) <- com.mongodb.internal.connection.InternalStreamConnection.access$200(InternalStreamConnection.java:105) <- com.mongodb.internal.connection.InternalStreamConnection$1.lambda$completed$0(InternalStreamConnection.java:269) <- com.mongodb.internal.connection.InternalStreamConnectionInitializer.lambda$startHandshakeAsync$0(InternalStreamConnectionInitializer.java:111) <- com.mongodb.internal.connection.CommandHelper.lambda$executeCommandAsync$0(CommandHelper.java:79)
```

Stack: `java.base/jdk.internal.misc.Unsafe.park(Unsafe.java) <- java.base/java.util.concurrent.locks.LockSupport.park(LockSupport.java:221) <- java.base/java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:754) <- java.base/java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:990) <- java.base/java.util.concurrent.locks.ReentrantLock$Sync.lock(ReentrantLock.java:153) <- java.base/java.util.concurrent.locks.ReentrantLock.lock(ReentrantLock.java:322) <- com.mongodb.internal.Locks.checkedWithLock(Locks.java:59) <- com.mongodb.internal.Locks.withLock(Locks.java:55) <- com.mongodb.internal.Locks.withLock(Locks.java:33) <- com.mongodb.internal.connection.DefaultConnectionPool$OpenConcurrencyLimiter.releasePermit(DefaultConnectionPool.java:1105) <- com.mongodb.internal.connection.DefaultConnectionPool$OpenConcurrencyLimiter.lambda$openWithConcurrencyLimitAsync$0(DefaultConnectionPool.java:1033) <- com.mongodb.internal.connection.DefaultConnectionPool$PooledConnection.lambda$openAsync$0(DefaultConnectionPool.java:659) <- com.mongodb.internal.connection.UsageTrackingInternalConnection.lambda$openAsync$0(UsageTrackingInternalConnection.java:72) <- com.mongodb.internal.connection.InternalStreamConnection.lambda$finishHandshakeAsync$0(InternalStreamConnection.java:300) <- com.mongodb.internal.connection.InternalStreamConnectionInitializer.finishHandshakeAsync(InternalStreamConnectionInitializer.java:124) <- com.mongodb.internal.connection.InternalStreamConnection.finishHandshakeAsync(InternalStreamConnection.java:292) <- com.mongodb.internal.connection.InternalStreamConnection.access$200(InternalStreamConnection.java:105) <- com.mongodb.internal.connection.InternalStreamConnection$1.lambda$completed$0(InternalStreamConnection.java:269) <- com.mongodb.internal.connection.InternalStreamConnectionInitializer.lambda$startHandshakeAsync$0(InternalStreamConnectionInitializer.java:111) <- com.mongodb.internal.connection.CommandHelper.lambda$executeCommandAsync$0(CommandHelper.java:79)`. This is connection-pool handshake release on the Mongo I/O loop, with no application frame; the listener now has a separate exact allowance for `DefaultConnectionPool$OpenConcurrencyLimiter.releasePermit`.

### M6 baseline — rezultat posle uskih framework/driver allowances

Puni `clean verify` sa podrazumevanim thread predicate-om, četiri precizna Mongo/Jackson allowance-a i zelenim canary testom završio je sa **154 testa, 0 neuspeha, 0 grešaka**. Callback je zabeležio samo očekivani canary:

```text
BLOCKHOUND_DETECTION blocking=java.lang.Thread.sleep0 thread=parallel-5 firstAppFrame=com.nantonijevic.feeds.testing.BlockHoundCanaryTest.lambda$detectsThreadSleepOnParallelScheduler$0(BlockHoundCanaryTest.java:16) trace=java.base/java.lang.Thread.sleep0(Thread.java) <- java.base/java.lang.Thread.sleep(Thread.java:509) <- com.nantonijevic.feeds.testing.BlockHoundCanaryTest.lambda$detectsThreadSleepOnParallelScheduler$0(BlockHoundCanaryTest.java:16) <- reactor.core.publisher.MonoCallable.call(MonoCallable.java:69) <- reactor.core.publisher.FluxSubscribeOnCallable$CallableSubscribeOnSubscription.run(FluxSubscribeOnCallable.java:228) <- reactor.core.scheduler.SchedulerTask.call(SchedulerTask.java:72) <- reactor.core.scheduler.SchedulerTask.call(SchedulerTask.java:28) <- java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317) <- java.base/java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:304) <- java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144) <- java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642) <- java.base/java.lang.Thread.run(Thread.java:1583)
```

Predikcija 1 („bez detekcija u app kodu“) bila je tačna za naše pakete, ali Mongo/Jackson interne detekcije su zahtevale šest uskih method-level allowances. Predikcija 2 je potvrđena: startup `.block()` na `main` niti nije prijavljen. Predikcija o Mongo event-loop niti iz prvobitnog dužeg ugovora bila je pogrešna — BlockHound ih tretira kao non-blocking; zato nije dodat thread predicate. U skraćenom M6 scope-u parse mutation, `.block()` varijanta i starvation test nisu pokrenuti. Jedini plant (`Thread.sleep`) jeste uhvaćen kao `BlockingOperationError`; alternativna `.block()` grana ostaje neproverena.

### M6 short-scope plant — rezultat

Plant je bio `Thread.sleep(100)` u `FetchFeedService.fetch` posle HTTP odgovora feed-a, unutar `flatMap`. Test `FeedApiIntegrationTest.fetchesAndStoresItemsFromRssFeed` pao je na očekivanom mehanizmu:

```text
BLOCKHOUND_DETECTION blocking=java.lang.Thread.sleep0 thread=reactor-http-nio-2 firstAppFrame=com.nantonijevic.feeds.service.FetchFeedService.lambda$fetch$0(FetchFeedService.java:52) trace=java.base/java.lang.Thread.sleep0(Thread.java) <- java.base/java.lang.Thread.sleep(Thread.java:509) <- com.nantonijevic.feeds.service.FetchFeedService.lambda$fetch$0(FetchFeedService.java:52) <- reactor.core.publisher.MonoFlatMap$FlatMapMain.onNext(MonoFlatMap.java:132)
```

Isti test je prošao `1/1` kada je isti sleep stavljen u `Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())`, bez BlockHound detekcije. Plant/fix je vraćen; `FetchFeedService.java` odgovara sačuvanoj kopiji bajt-po-bajt.

Završni M6 `clean verify` i nezavisni `verify` prošli su po `154/0/0`, oba sa zelenim canary-jem. README objašnjava test wiring i `BlockingOperationError`. Plan §4 red M6 je označen done; debugger/Docker attach je „not covered“.
