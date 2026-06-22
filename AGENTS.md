# CLAUDE.md — Crypto CEX Market Data Connector

This file is loaded automatically by Claude Code at session start. It captures the architecture, conventions, and current state of the project so any AI agent can contribute immediately without re-deriving context.

---

## Project purpose

A high-performance Rust market-data connectivity service for crypto centralized exchanges, starting with **Binance Spot** and **Binance USD-M Futures**. It ingests live market data over WebSocket, normalises it into versioned binary messages, and publishes to downstream trading algorithms through **Aeron IPC** (shared-memory, same host) or **Aeron UDP** (cross-host).

Target consumers are latency-sensitive: HFT strategies, execution engines, risk systems.

Full specification: `SPEC.md` and the spec addendum embedded in the same file.

---

## Repository layout

```
Cargo.toml                  workspace root
config/default.toml         runtime configuration (the single source of truth)
SPEC.md                     product specification v0.1 + addendum v0.2
deploy/                     Dockerfile, systemd units, AWS tuning notes, migration runbook
docs/                       (empty — Notion is used for documentation)

bin/
  connector/                main binary — the market-data connector
  aeron-driver/             standalone Aeron C media driver process
  aeron-observer/           latency validation tool (subscribe + decode + report)
  shadow-compare/           active/shadow generation comparison tool

crates/
  connector-core/           shared types, message header, all message structs, binary codec
  connector-config/         config loading, shard routing (FNV-1a), owned-shard logic
  binance-spot-adapter/     WebSocket connection manager, JSON/SBE parser, normaliser
  binance-futures-adapter/  futures WebSocket connection manager, parser, normaliser
  protocol-json/            low-level Binance JSON parsers (spot + futures)
  protocol-sbe/             Binance Spot SBE decoder (official schema vendored)
  order-book/               in-memory L2 order book, delta apply, BBO validation
  refdata/                  REST client for exchange info, InstrumentDefinition
  aeron-publisher/          Aeron client wrapper, ShardedPublisher, NullPublication
  metrics/                  lock-free counters + latency histograms, Prometheus export
  redundancy/               active/passive redundancy, BookChecksum, cross-instance compare
  replay/                   market-data replayer (raw WS, normalised, Aeron Archive)
  order-gateway/            order state machine, CLOID generator, order journal (Phase 1.5)
```

---

## Build & run

```bash
# Build everything
cargo build

# Run all unit tests (excludes examples)
cargo test --workspace --lib --bins

# Run with the default config (futures connector, single shard)
cargo run -p aeron-driver -- --dir /tmp/aeron          # terminal 1 — start first
cargo run -p connector   -- -c config/default.toml     # terminal 2
cargo run -p aeron-observer -- --stream 1 --interval 5 # terminal 3 — latency stats
```

`cargo test --workspace` also compiles examples; those are always valid. If they fail, it is a bug.

Rust toolchain: **stable** (see `rust-toolchain.toml`).

---

## Configuration

`config/default.toml` is the single source of truth. Key sections:

```toml
[instance]
id     = 0              # zero-based; 0 = active, 1+ = passive
total  = 1
venue  = "binance_futures"   # binance_spot | binance_futures
market = "usdm_futures"      # spot | usdm_futures

[sharding]
total_logical_shards = 16   # NEVER change without a full generation migration

[aeron]
media_driver_dir = "/tmp/aeron"   # Linux prod: /dev/shm/aeron
```

**Do not embed CLI flags for config values that live in the config file.** The connector reads `--config <path>` and nothing else — no `--shard-id`, `--total-shards`, etc. Config is the single source of truth.

---

## Shard and stream routing

```
shard_id  = fnv1a_32(venue_byte || market_byte || symbol_utf8) % total_logical_shards
stream_id = shard_id + 1          # Aeron stream IDs must be > 0
```

Where `BinanceFutures = 2`, `UsdmFutures = 2`, `BinanceSpot = 1`, `Spot = 1`.

With the default config (`total_logical_shards = 16`, `venue = binance_futures`):

| Symbol   | shard | stream | Observer flag  |
|----------|-------|--------|----------------|
| BTCUSDT  | 12    | 13     | `--stream 13`  |
| ETHUSDT  | 0     | 1      | `--stream 1`   |
| SOLUSDT  | 13    | 14     | `--stream 14`  |
| BNBUSDT  | 5     | 6      | `--stream 6`   |
| XRPUSDT  | 1     | 2      | `--stream 2`   |

**Changing `total_logical_shards` rehashes all symbols.** Requires a full generation migration (see `deploy/runbook.md`).

Multi-instance ownership: `owner_instance_id = shard_id % total_instances`. Instance 0 owns even shards, instance 1 owns odd shards (for a 2-instance deployment).

---

## Wire protocol — Aeron message format

Every Aeron fragment: **56-byte fixed header + payload**. All integers little-endian.

```
Offset  Len  Field               Type
     0    1   schema_version      u8    = 1
     1    1   message_type        u8    see MessageType enum
     2    1   venue_id            u8    BinanceSpot=1, BinanceFutures=2
     3    1   market_type         u8    Spot=1, UsdmFutures=2
     4    4   instrument_id       u32
     8    4   connection_id       u32   = shard_id
    12    4   instance_id         u32   0 = active
    16    8   sequence_number     u64
    24    8   exchange_event_ts   i64   ns since epoch; 0 (TS_NONE) if absent
    32    8   exchange_tx_ts      i64   ns since epoch; 0 (TS_NONE) if absent
    40    8   local_recv_ts       i64   ns since epoch
    48    8   local_publish_ts    i64   ns since epoch
```

**TS_NONE = 0**: sentinel for a missing exchange timestamp. `bookTicker` streams have no `E` field, so `exchange_event_ts = 0` for those messages.

MessageType values: `InstrumentDefinition=1, TradingStatus=2, BookSnapshot=3, BookDelta=4, BestBidOffer=5, Trade=6, MarkPrice=7, FundingRate=8, Liquidation=9, OpenInterest=10, AccountUpdate=11, OrderUpdate=12, Heartbeat=13, FeedStatus=14, GapDetected=15, BookStale=16, BookRecovered=17, BookChecksum=18`.

Strings: 2-byte LE u16 length prefix + UTF-8. PriceLevel arrays: 4-byte LE u32 count then `(i64 price, i64 qty)` pairs. `qty = 0` removes a level. **No floats anywhere** — all prices and quantities are scaled integers; divide by `10^scale` from `InstrumentDefinition`.

Decoding entry point: `connector_core::{MessageHeader::decode, NormalizedMessage::from_bytes}`.

---

## Key conventions

### No floats
Prices and quantities are `i64` scaled integers everywhere. `actual = mantissa / 10^scale`. Scales come from `InstrumentDefinition`.

### No channels in the WebSocket hot path
`ConnectionManager::run` takes `FnMut(RawFrame)` — not a `Sender`. The closure is called inline on each frame without going through an mpsc channel. This was an explicit refactor to reduce latency.

```rust
// CORRECT
mgr.run(&url, move |frame| { /* process inline */ }, shutdown).await;

// WRONG — Sender<RawFrame> does not implement FnMut(RawFrame)
mgr.run(&url, frame_tx, shutdown).await;
```

### Single writer per Aeron stream
`ShardedPublisher` enforces one writer per shard. `DynShardedPublisher = ShardedPublisher<Box<dyn Publication + Send>>` allows runtime switching between `NullPublication` and `AeronClientPublication` without changing the pipeline type.

### Aeron feature flag
Real Aeron publishing is behind `features = ["aeron"]` in `connector-aeron`. The connector's `Cargo.toml` enables it. `build_aeron` falls back to `build_null_boxed` with a warning if the media driver is not running.

### Timestamps
Binance `E` field (event_time_ms) is **milliseconds** — multiply by `1_000_000` to get nanoseconds before storing in `exchange_event_ts`.

---

## Current implementation state

### Done
- Full binary message codec (`connector-core`): all 18 message types, round-trip tests
- Config loading with env override, shard routing, owned-shard logic
- Binance Spot WebSocket adapter: connection manager, reconnect, ping/pong, 24h rotation
- Binance Futures WebSocket adapter: same, plus futures-specific stream paths
- JSON parsers: spot (bookTicker, depthUpdate, trade) + futures (depth, aggTrade, markPrice, bookTicker, liquidation)
- SBE decoder: official Binance Spot schema vendored, TradesStreamEvent / BBO / DepthSnapshot / DepthDiff
- Order book engine: delta apply, sequence validation, gap detection, BBO validation, snapshot validation, recovery buffer, circuit breaker
- Aeron publisher: real publishing via `rusteron-client` (precompiled macOS + Linux), fallback to null
- `aeron-driver` binary: standalone Aeron C media driver, Ctrl-C shutdown
- `aeron-observer` binary: subscribes to Aeron stream, decodes headers, reports wire/encode/ipc/e2e latency percentiles per interval
- Metrics: lock-free counters and latency histograms, Prometheus export on port 9090
- Active/passive redundancy: `BookChecksum` published by passive instances to status stream
- Replay crate: raw WS / normalised / Aeron Archive replay with timing modes
- Order gateway: CLOID generator, order state machine, journal (Phase 1.5 — not yet wired to live trading)
- Deploy: Dockerfile, systemd template (`connector@.service`), AWS tuning script, migration runbook

### Not yet done / stubs
- `AccountUpdate` / `OrderUpdate` payloads: message types allocated, bodies empty
- Live trading: order gateway built but not connected to exchange
- Active/active arbiter (Phase 2): cross-instance selection logic
- Aeron Archive recording: config knob exists (`archive_enabled = false`), not wired
- UDP transport: config knob exists (`udp_endpoint`), not wired
- Open interest REST polling: struct defined, not scheduled
- Multi-exchange (non-Binance): not started

---

## aeron-observer — latency tool

Validates data flow and measures pipeline latency. Subscribe to any shard:

```bash
cargo run -p aeron-observer -- \
  --dir /tmp/aeron \
  --stream 13 \       # BTCUSDT futures with default config
  --interval 5        # print stats every 5 seconds
```

Output: `wire` (exchange→socket), `encode` (parse+normalise+SBE), `ipc` (Aeron shared memory), `e2e` (exchange→observer) with p50/p99/p99.9/max per interval. Messages without `exchange_event_ts` (bookTicker) are counted separately.

---

## Aeron setup (macOS dev)

`/dev/shm` does not exist on macOS. Use `/tmp/aeron`.

```bash
# Terminal 1 — media driver (start before connector)
cargo run -p aeron-driver -- --dir /tmp/aeron

# Terminal 2 — connector
cargo run -p connector -- -c config/default.toml

# Terminal 3 — observe (stream = shard + 1)
cargo run -p aeron-observer -- --dir /tmp/aeron --stream 1
```

Linux production: change `media_driver_dir` to `/dev/shm/aeron` in config.

---

## Rusteron dependencies

`rusteron-client` and `rusteron-media-driver` use precompiled C binaries downloaded at build time:

```toml
features = ["precompile-rustls", "static"]
```

Both `precompile-rustls` AND `static` are required. `precompile-rustls` alone fails with "No build method available".

`rusteron-media-driver` must NOT be added to the `connector` binary — it causes duplicate TLS/crypto crate builds that fill disk. It lives only in `bin/aeron-driver`.

Key Aeron API:
- `Handlers::no_available_image_handler()` / `Handlers::no_unavailable_image_handler()` — correct None handlers for `add_subscription`
- `publication.offer_once(bytes, |_: *mut u8, _: usize| 0i64)` — single offer attempt
- Return codes: `>= 0` ok, `-1` not connected, `-2` back-pressured, `-3` admin action, `-4` closed

---

## Testing

```bash
cargo test --workspace --lib --bins   # 855 tests, all pass
cargo build --examples                # ensures examples compile too
```

Tests are in-crate (`#[cfg(test)]`). No integration test crate yet. Golden-file fixtures in `crates/protocol-sbe/`.

---

## Documentation

Living documentation is in **Notion** (connected via MCP with token in `~/.claude/settings.json`):
- *Aeron Observer — Latency Validation Tool*: how the observer works, latency segments, output format
- *Aeron Message Format — Wire Protocol Reference*: complete wire protocol, all 18 message types, encoding conventions, shard routing formula, per-symbol stream lookup table

The Notion API token is in `settings.json`. Use `curl` directly when MCP tools are not available in session.
