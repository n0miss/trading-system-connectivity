Rust project, low latency, low GC (zero-allocation patterns), high performance.
aims to be deployed on k8s. 
scalability is very important, and easy deploymenet / upgrade of format/data, retro compatilibity.
after each step, validate with testing and if test are successfull then commit the changes without asking for confirmation


**Stage 1 — Scaffolding & core types**
1. Set up a Cargo workspace with empty stub crates: connector-core, binance-spot-adapter, binance-futures-adapter, protocol-json, protocol-sbe, order-book, refdata, aeron-publisher, metrics, replay, config. Add CI for build/test/lint.
2. In connector-core, define the common message header (schema version, message type, venue id, market type, instrument id, source connection id, publisher instance id, sequence number, timestamps) with binary encode/decode using fixed-point integers (no floats), plus round-trip tests.
3. Define the remaining message types (InstrumentDefinition, BookSnapshot, BookDelta, BestBidOffer, Trade, MarkPrice, FundingRate, Liquidation, OpenInterest, Heartbeat, FeedStatus, GapDetected, BookStale, BookRecovered) with binary serialization and golden-file tests.
4. Build the config crate: load instance config (instance_id, total_instances, venue, market type, symbol universe, shard assignment, Aeron params) from file/env with validation.
5. Build refdata: REST client for Binance exchangeInfo, normalize into InstrumentDefinition, derive price/quantity scale, periodic refresh, and detect symbol status changes.

**Stage 2 — Spot happy path, single symbol**
6. binance-spot-adapter: WebSocket connection manager — connect, subscribe, ping/pong, basic reconnect, forced 24h rotation.
7. protocol-json: parse Binance Spot depth update, trade, and bookTicker JSON into exchange-native structs.
8. Normalizer: convert Spot exchange-native messages into internal BestBidOffer, Trade, BookDelta.
9. order-book: single-symbol in-memory L2 book that applies deltas and exposes best bid/ask (no sequence validation yet).
10. aeron-publisher: wrap Aeron, single-writer-per-shard, publish framed binary messages over aeron:ipc.
11. Wire it end-to-end for one symbol (e.g. BTCUSDT) and run it continuously against live Binance as a smoke test.

**Stage 3 — Sequence validation & recovery**
12. Implement Spot sequence validation (U/u rules, §2.2) and mark the book stale on a gap.
13. Implement the per-symbol recovery buffer with the 2,048-event / 4 MiB / 10s limits (addendum §1).
14. Implement the recovery procedure: buffer while stale, fetch REST snapshot, discard stale events, bridge, apply, mark recovered, publish BookStale/BookRecovered.
15. Implement overflow handling: drop buffer, mark DEGRADED, circuit breaker (5 attempts / 30s cooldown), overflow metric.
16. Implement top-of-book validation against the BBO stream with the 250ms/1s degrade/stale thresholds (§2.3).
17. Implement periodic REST snapshot validation with the tolerance rules in §2.4.

**Stage 4 — Multi-symbol sharding**
18. Implement deterministic shard hashing (`logical_shard_id = hash(venue, market, symbol) % total_shards`) and shard→instance assignment from config.
19. Make the order book engine multi-symbol with one thread owning each shard's symbol set; restructure the pipeline around per-shard threads.
20. Implement the Aeron stream layout (per-shard market data, refdata, status streams) and FeedStatus heartbeat publishing.

**Stage 5 — Futures adapter**
21. binance-futures-adapter: WS connection manager for the futures routed stream paths, reusing the spot connection-manager abstractions.
22. protocol-json: parse Futures depth diff (U/u/pu), aggTrade, markPrice, bookTicker, and liquidation order JSON.
23. Futures sequence validation: pu == previous u, integrated with the existing recovery module.
24. Normalizer additions for MarkPrice/FundingRate/Liquidation, and the aggregate-trade-only policy for futures.
25. Wire futures end-to-end for one symbol, then scale to the full symbol universe with sharding.

**Stage 6 — Reference & slow-path data**
26. Open interest / long-short REST polling module publishing OpenInterest on a slow cadence.
27. TradingStatus detection from refdata refresh, publishing TradingStatus and pausing/resuming book maintenance accordingly.

**Stage 7 — SBE**
28. protocol-sbe: vendor the official Binance Spot SBE XML schema, generate the Rust decoder, validate schema id/version at startup.
29. Implement decoders for TradesStreamEvent, BestBidAskStreamEvent, DepthSnapshotStreamEvent, DepthDiffStreamEvent with golden binary fixtures and cross-checks against JSON-normalized output.
30. Wire SBE as the preferred Spot feed with JSON fallback (§3.4).

**Stage 8 — Metrics**
31. Build the metrics crate: latency hop counters/histograms, message rate, reconnect count, stale-book count, sequence gaps, Aeron offer failures, exported Prometheus-style.
32. Instrument the hot path with the required timestamps (§7.1) without adding allocations; produce p50/p99 latency numbers.

**Stage 9 — Backpressure & redundancy**
33. Implement the Aeron backpressure policy (spin/retry → warn at 100µs → degrade at 1ms → restart/failover at 10ms, §5.3).
34. Implement active/passive redundancy: passive instance builds its own book and publishes checksums to the status stream.
35. Implement cross-instance checksum comparison and failover triggers (§2.5, §10) — a Phase-1-appropriate stub of the full arbiter is fine here.

**Stage 10 — Replay & test infrastructure**
36. Build the replay crate supporting raw WS payload, normalized message, and Aeron Archive replay, with as-fast-as-possible / original-timing / scaled / deterministic / fault-injection modes.
37. Build a synthetic test harness covering 1/10/100/1000+ symbols and the listed edge cases (zero updates, wide spreads, empty side, delisting, large batch updates).
38. Add property tests for order book invariants (bid < ask, no negative quantities, monotonic sequence).
39. Add chaos tests for disconnects, REST slowness, and malformed events, asserting recovery completes within the §7.3 SLA targets.

**Stage 11 — Private execution data (Phase 1.5)**
40. Build the order gateway, client order id generator, and persistent order journal.
41. Implement the order state machine handling out-of-order acks/fills, unknown-status timeouts, and duplicate execution reports (§6.3).
42. Build the user data stream listener and execution report normalizer producing AccountUpdate/OrderUpdate.
43. Implement REST reconciliation (open orders, fills, balances, positions, funding) triggered at startup, reconnect, unknown status, and periodically.

**Stage 12 — Deployment**
44. Write the Dockerfile/systemd unit and AWS deployment notes (placement group, time sync, NIC/CPU tuning) per §10.
45. Write the deployment-generation migration runbook and supporting tooling for shadow-mode build/compare/switchover (§4.4).