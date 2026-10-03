# Changelog

## 0.1.0 (2026-10-03)


### ⚠ BREAKING CHANGES

* the import path is now github.com/giraffesyo/portunus and the package identifier is portunus. Any existing import of github.com/giraffesyo/mux must be updated.

### refactor

* rename module to portunus ([2057218](https://github.com/giraffesyo/portunus/commit/2057218dfa1e16168227bcdad1e4e62fc3a65a33))


### Features

* add abuse limits, panic containment, and Stats() ([cbf3eb4](https://github.com/giraffesyo/portunus/commit/cbf3eb4bf4218f257c988074059da59c5fa372ff))
* add BDP-autotuned flow control and keepalive ([a8c854f](https://github.com/giraffesyo/portunus/commit/a8c854f6dc49434db0802ab7c3b2927e41cc8f60))
* add QUIC adapter and shared conformance suite ([eeb0c41](https://github.com/giraffesyo/portunus/commit/eeb0c4167ba621b38530ad98612f8ef4284518fa))
* add wire format and session core ([c5b32ae](https://github.com/giraffesyo/portunus/commit/c5b32aeeab5411147bf7d7d0c2ae93b6921167e2))
* Batched carrier wrapper, so a batch over TLS is one socket write ([55acf8d](https://github.com/giraffesyo/portunus/commit/55acf8d7c0d0cf4ab2ee079fd1c686d99166fd1c))
* select TCP congestion control per socket (Config.CongestionControl) ([b583079](https://github.com/giraffesyo/portunus/commit/b5830790cb202aeaaf3197f20c1133564ccfb109))


### Bug Fixes

* correct defects found by code review ([fd0ea6d](https://github.com/giraffesyo/portunus/commit/fd0ea6dfef323eb191ffd2404adf6610c52b4055))
* flush queued frames before a graceful shutdown closes ([8b49fd0](https://github.com/giraffesyo/portunus/commit/8b49fd041c123cc1163e580c0de51f314bcf617a))
* half-close and drain before closing the carrier on Shutdown ([486d299](https://github.com/giraffesyo/portunus/commit/486d299e3065c0c0e838a1a9898712e27eb92a0d))
* make BDP autotune converge reliably ([2e63619](https://github.com/giraffesyo/portunus/commit/2e63619232e342704f128aa616e0360f5016db24))
* reject MaxBatchBytes past the batch's 32-bit staging offsets ([d9683e6](https://github.com/giraffesyo/portunus/commit/d9683e6d3f790b92ce4066214b390f4cd06fb47a))
* stop control frames overtaking the streams they belong to ([b067326](https://github.com/giraffesyo/portunus/commit/b067326625c080db61349b2b8f40dfe474ad73ed))


### Performance

* add experimental small-frame prioritization, and bound its claim ([ddf6fbe](https://github.com/giraffesyo/portunus/commit/ddf6fbe439e636a7a46e6ee8dca637cb1c006bd6))
* batch frames across streams into one writev ([8e8e993](https://github.com/giraffesyo/portunus/commit/8e8e993cad5a362692fdbb5857c91fd856751517))
* batch writes with themselves, shard small frames, trim the per-frame path ([df79622](https://github.com/giraffesyo/portunus/commit/df79622d08f199e5180a6bef123b774259856917))
* default the read buffer to 256KB, measured on a real NIC ([4dd151c](https://github.com/giraffesyo/portunus/commit/4dd151c6b8f1b19759d5eb72ccc89e9842047e73))
* eliminate per-release and per-flush allocations ([bc1564d](https://github.com/giraffesyo/portunus/commit/bc1564dc7635cd9058b010121f2b2fad38deaee8))
* hold bulk to one frame per flush while interactive traffic flows ([9ba5b22](https://github.com/giraffesyo/portunus/commit/9ba5b22c77f158697010dac4e4bcd3e03fb5d036))
* pool receive segments and add zero-copy relay ([c21a2fc](https://github.com/giraffesyo/portunus/commit/c21a2fcfdf1569361e25fa3b98038bd06b38e133))
* reach zero allocations per frame on the data paths ([5945239](https://github.com/giraffesyo/portunus/commit/5945239e9beeb96f52a2d1fe89039cd3eca81803))
* refresh flow-control credit at a quarter window, not a half ([336f3fb](https://github.com/giraffesyo/portunus/commit/336f3fb5827507a872d1a3921f0a8460f41985fb))


### CI

* lint and audit in CI, release with release-please ([754beda](https://github.com/giraffesyo/portunus/commit/754bedac15b0826f53e0dae76bfed430b6a75b44))
