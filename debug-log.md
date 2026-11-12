# Debug log

## Bug 1: M5 live item stream

### Step 1 — predictions (before the first run)

1. **SSE event mapping thread:** I predict the `map` that builds each `ServerSentEvent` will run on a MongoDB driver event-loop thread (`multiThreadIoEventLoopGroup-*`), because the change-stream publisher emits there and there is no scheduler boundary in the pipeline. I do not expect one fixed thread across every event; the driver may deliver later batches on another event-loop thread. I will verify this by recording the thread name at the mapping breakpoint for several inserts.
2. **String `feedId` in the server-side match:** I predict the client will see silence, not an error. MongoDB will compare a BSON string with the stored BSON `ObjectId`, so the match will simply return no events. I will verify by subscribing, inserting for that feed, and observing the stream over a bounded window.
3. **Change-stream demand and prefetch:** I predict the reactive publisher honors Reactive Streams demand, while Reactor or the Mongo driver may still fetch a bounded batch ahead internally. With the required backpressure buffer, I expect the operator to request upstream independently of the downstream's zero demand, so inserting more than the configured capacity should overflow. A zero-demand subscriber plus a request probe at the publisher boundary (and the overflow test) will distinguish downstream delivery from upstream fetching.
4. **Cancellation and cursor cleanup:** I predict interrupting `curl` will trigger `doOnCancel` immediately and the server cursor will return to baseline within a short bounded polling window (under a few seconds), without waiting for the next heartbeat. I will record the cancellation signal and poll `serverStatus.metrics.cursor.open.total` until it returns to baseline.
5. **Resume-token boundary:** I predict `resumeAfter` is exclusive: reconnecting with event 1's token will not emit event 1 again, and will emit later inserts in order. I will verify with the resume integration test.
6. **Expired resume token response commitment:** I predict `ChangeStreamHistoryLost` will arrive before the first heartbeat or item is written, leaving the response uncommitted; the client should therefore receive a normal terminal error response rather than an SSE frame. I will inspect the actual HTTP status/body in the expired-token scenario and record whether headers had already been committed.

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
