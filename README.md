# trading-system-connectivity

High-performance Rust market-data connectivity for crypto centralised exchanges. Ingests live market data over WebSocket, normalises it into versioned binary messages, and publishes downstream via **Aeron IPC** (same-host shared memory) or **Aeron UDP** (cross-host). Built for latency-sensitive consumers: HFT strategies, execution engines, risk systems.

Currently supported venues: **Binance Spot** and **Binance USD-M Futures**.

---

## latency

Live Prometheus metrics: **http://35.77.39.5:30091/metrics**

**Wire latency** (Binance exchange event timestamp → local socket receive) measured on the production deployment — AWS EC2 `ap-northeast-1` (Tokyo), over a live Binance USD-M Futures feed, sample count 1,469,257:

| Percentile | Latency |
| ---------: | ------: |
|        p50 | **1.748 ms** |
|        p90 | **2.359 ms** |
|        p95 | **2.435 ms** |
|        p99 | **2.496 ms** |
|      p99.9 | **45.249 ms** |

The p50–p99 band is tight at ~1.7–2.5 ms, dominated by Binance's 1 ms event-timestamp quantization (the `E` field is millisecond-resolution) plus ~0.7 ms true network transit from EC2 Tokyo to Binance. The p99.9 spike reflects OS/container scheduling jitter on a burstable `t4g.small` instance; a dedicated bare-metal server in the same data centre (Equinix TY3) would bring this below 5 ms.

**Processing latency** (local socket receive → Aeron offer), same deployment, sample count 10,043,996:

| Percentile | Latency |
| ---------: | ------: |
|        p50 | **4.8 µs** |
|        p90 | **14.6 µs** |
|        p95 | **23.4 µs** |
|        p99 | **55 µs** |
|      p99.9 | **218 µs** |

This covers the full hot path per message: JSON parsing · field normalisation (string prices → scaled `i64`) · order-book delta apply and BBO validation · binary encoding into the 56-byte wire header · Aeron IPC offer. The ~5 µs median is negligible relative to wire latency, confirming the bottleneck is the network, not the processing pipeline.

Latency segments: `wire` (exchange→socket) · `encode` (parse+normalise+validate+publish) · `e2e` (exchange→Aeron offer = wire + encode).

---

## Market data ingested from Binance

All streams are subscribed via the Binance combined-stream WebSocket endpoint
(`/stream?streams=…`), which wraps every frame as
`{"stream":"<name>","data":{…}}`. Prices and quantities arrive as strings and
are converted to scaled `i64` by the normaliser before publication.

### Binance Spot — `wss://stream.binance.com:443`

| Stream | Subscription name | Normalised message |
|---|---|---|
| Best bid/ask | `{symbol}@bookTicker` | `BestBidOffer` |
| L2 depth incremental | `{symbol}@depth@{100\|250\|500}ms` | `BookDelta` |
| Individual trade | `{symbol}@trade` | `Trade` |

`bookTicker` carries no exchange event timestamp (`E` field absent) — the
header `exchange_event_ts` is set to `TS_NONE (0)` for those messages.

`depth` updates include `U` (first update ID) and `u` (last update ID) for
sequence-gap detection. A REST depth snapshot is fetched on startup to seed the
order book; subsequent deltas are applied and validated against the BBO.

### Binance USD-M Futures — `wss://fstream.binance.com:443`

| Stream | Subscription name | Normalised message(s) |
|---|---|---|
| Best bid/ask | `{symbol}@bookTicker` | `BestBidOffer` |
| L2 depth incremental | `{symbol}@depth@{100\|250\|500}ms` | `BookDelta` |
| Aggregated trade | `{symbol}@aggTrade` | `Trade` |
| Mark price | `{symbol}@markPrice` or `{symbol}@markPrice@1s` | `MarkPrice` + `FundingRate`* |
| Liquidation order | `{symbol}@forceOrder` | `Liquidation` |

\* A single `markPriceUpdate` event produces two Aeron messages: one `MarkPrice`
and one `FundingRate`. When the funding rate field is absent (empty string
between settlement windows) only `MarkPrice` is emitted.

The futures `depth` stream includes `pu` (previous final update ID), enabling
gap detection without a REST snapshot handshake. `T` (transaction time) is also
captured as `exchange_tx_ts` in the message header.

### REST API — reference data and order book seeding

In addition to the WebSocket streams, two REST endpoints are called via the
`refdata` crate on startup (and periodically for refresh):

| Endpoint | Venue | Purpose | Normalised message |
|---|---|---|---|
| `GET /api/v3/exchangeInfo` | Spot | Full symbol catalogue with filters | `InstrumentDefinition` + `TradingStatus` |
| `GET /fapi/v1/exchangeInfo` | Futures | Full symbol catalogue with filters | `InstrumentDefinition` + `TradingStatus` |
| `GET /api/v3/depth?symbol=X&limit=1000` | Spot | L2 full snapshot to seed the order book | `BookSnapshot` |
| `GET /fapi/v1/depth?symbol=X&limit=1000` | Futures | L2 full snapshot to seed the order book | `BookSnapshot` |

**`exchangeInfo`** is parsed into `InstrumentDefinition` structs that carry the
symbol's price/quantity scales, tick size, step size, minimum order size, and
trading status. An `InstrumentRegistry` tracks additions and field changes; when
`is_trading` flips a `TradingStatus` message is also published on the Aeron
stream. All numeric filter values (`tickSize`, `stepSize`, `minQty`,
`minNotional`) are converted to scaled `i64` with no floats.

**Depth snapshots** are fetched once per symbol at startup to seed the in-memory
L2 order book before incremental `depth` WebSocket updates begin. The snapshot's
`lastUpdateId` is used to correctly splice in the first delta from the stream
(gap detection).

### User data stream — order gateway (Phase 1.5, not yet wired to live trading)

The `order-gateway` crate parses the Binance user data WebSocket stream, which
carries private account events for a logged-in API key:

| Event type (`e` field) | Description | Fields captured |
|---|---|---|
| `executionReport` | Order lifecycle update | symbol, client order ID, side, type, TIF, qty, price, exec type (`NEW`/`TRADE`/`CANCELED`/`REJECTED`/`EXPIRED`), fill qty, fill price, commission, trade ID, timestamps |
| `outboundAccountPosition` | Balance snapshot pushed after any change | per-asset free balance + locked balance |
| `balanceUpdate` | Signed balance delta (deposit, withdrawal, dust sweep) | asset, signed delta, clear time |

These events are parsed into typed Rust structs, normalised, and will publish
`OrderUpdate` and `AccountUpdate` messages on the Aeron stream once wired to
live trading. Unknown event types are captured as `Unknown { event_type }` and
logged without crashing the stream listener.

---

## Architecture

```
Binance WebSocket
       │  JSON frames
       ▼
  Adapter crate          (binance-spot-adapter / binance-futures-adapter)
  ├─ ConnectionManager   reconnect, ping/pong, 24h rotation
  ├─ JSON parser         protocol-json / protocol-sbe (SBE decoder)
  └─ Normaliser          → NormalizedMessage (scaled-integer, no floats)
       │  56-byte header + payload
       ▼
  ShardedPublisher       (aeron-publisher)
  ├─ shard_id = fnv1a_32(venue || market || symbol) % 16
  └─ stream_id = shard_id + 1
       │
  Aeron IPC / UDP
       │
  Downstream consumers   (strategies, risk, execution)
```

---

## Repository layout

```
Cargo.toml                  workspace root
config/default.toml         runtime configuration
SPEC.md                     product specification v0.1 + addendum v0.2
deploy/                     Dockerfile, systemd units, AWS tuning, runbook

bin/
  connector/                main binary — market-data connector
  aeron-driver/             standalone Aeron C media driver
  aeron-observer/           latency validation tool (subscribe + decode + report)
  clickhouse-bridge/        subscribes to all Aeron shards and inserts into ClickHouse
  shadow-compare/           active/shadow generation comparison tool

crates/
  connector-core/           shared types, message header, binary codec (18 message types)
  connector-config/         config loading, shard routing (FNV-1a), owned-shard logic
  binance-spot-adapter/     WebSocket manager, JSON/SBE parser, normaliser
  binance-futures-adapter/  futures WebSocket manager, parser, normaliser
  protocol-json/            low-level Binance JSON parsers (spot + futures)
  protocol-sbe/             Binance Spot SBE decoder (official schema vendored)
  order-book/               in-memory L2 order book, delta apply, BBO validation
  refdata/                  REST client for exchange info, InstrumentDefinition
  aeron-publisher/          Aeron client wrapper, ShardedPublisher, NullPublication
  metrics/                  lock-free counters + latency histograms, Prometheus export
  redundancy/               active/passive redundancy, BookChecksum, cross-instance compare
  replay/                   market-data replayer (raw WS, normalised, Aeron Archive)
  order-gateway/            order state machine, CLOID generator, order journal
```

---

## Quick start

**Prerequisites:** Rust stable (see `rust-toolchain.toml`) and `cmake` (Aeron C is compiled from source).

```bash
# macOS
brew install cmake

# Ubuntu / Debian
sudo apt-get install cmake g++ uuid-dev libbsd-dev libclang-dev
```

```bash
# Build
cargo build

# Terminal 1 — Aeron media driver (start first)
cargo run -p aeron-driver -- --dir /tmp/aeron

# Terminal 2 — connector
cargo run -p connector -- -c config/default.toml

# Terminal 3 — observe latency on ETHUSDT futures (stream 1)
cargo run -p aeron-observer -- --dir /tmp/aeron --stream 1 --interval 5
```

---

## Configuration

`config/default.toml` is the single source of truth:

```toml
[instance]
id     = 0                      # 0 = active, 1+ = passive
total  = 1
venue  = "binance_futures"      # binance_spot | binance_futures
market = "usdm_futures"         # spot | usdm_futures

[sharding]
total_logical_shards = 16       # NEVER change without a full generation migration

[aeron]
media_driver_dir = "/tmp/aeron" # Linux prod: /dev/shm/aeron
```

---

## Shard and stream routing

```
shard_id  = fnv1a_32(venue_byte || market_byte || symbol_utf8) % total_logical_shards
stream_id = shard_id + 1
```

With `total_logical_shards = 16` and `venue = binance_futures`:

| Symbol   | shard | stream | Observer flag  |
|----------|-------|--------|----------------|
| BTCUSDT  | 12    | 13     | `--stream 13`  |
| ETHUSDT  | 0     | 1      | `--stream 1`   |
| SOLUSDT  | 13    | 14     | `--stream 14`  |
| BNBUSDT  | 5     | 6      | `--stream 6`   |
| XRPUSDT  | 1     | 2      | `--stream 2`   |

---

## Wire protocol

Every Aeron fragment: **56-byte fixed header + payload**. All integers little-endian.

```
Offset  Len  Field
     0    1   schema_version      u8  = 1
     1    1   message_type        u8
     2    1   venue_id            u8  BinanceSpot=1, BinanceFutures=2
     3    1   market_type         u8  Spot=1, UsdmFutures=2
     4    4   instrument_id       u32
     8    4   connection_id       u32 (= shard_id)
    12    4   instance_id         u32 (0 = active)
    16    8   sequence_number     u64
    24    8   exchange_event_ts   i64 ns since epoch; 0 = absent
    32    8   exchange_tx_ts      i64 ns since epoch; 0 = absent
    40    8   local_recv_ts       i64 ns since epoch
    48    8   local_publish_ts    i64 ns since epoch
```

Message types: `InstrumentDefinition=1` `TradingStatus=2` `BookSnapshot=3` `BookDelta=4` `BestBidOffer=5` `Trade=6` `MarkPrice=7` `FundingRate=8` `Liquidation=9` `OpenInterest=10` `AccountUpdate=11` `OrderUpdate=12` `Heartbeat=13` `FeedStatus=14` `GapDetected=15` `BookStale=16` `BookRecovered=17` `BookChecksum=18`.

No floats — all prices and quantities are scaled `i64`. Divide by `10^scale` from `InstrumentDefinition`.

---

## Testing

```bash
# 854 unit tests
cargo test --workspace --lib --bins

# Ensure examples compile
cargo build --examples
```

---

## Metrics

Prometheus metrics exported on `:9090`. Covers per-shard message counts, publish latency histograms, reconnect counters, and order-book gap events.

---

## Deployment

Production runs on a single AWS EC2 node (`ap-northeast-1`) under **k3s** (lightweight Kubernetes). Three containers share an Aeron IPC memory region via a K8s `emptyDir(medium=Memory)` volume:

1. `aeron-driver` — Aeron C media driver
2. `connector` — market-data connector (Prometheus metrics on `:9090`)
3. `clickhouse-bridge` — reads from Aeron and writes to ClickHouse

Images are built by GitHub Actions on a free GitHub-hosted `ubuntu-24.04-arm` runner (4 vCPU, 16 GB RAM) and pushed to ECR. Merging to `main` triggers an automatic deploy via SSH to the EC2 node.

See `deploy/` for:
- `Dockerfile.runtime-connector`, `Dockerfile.runtime-aeron-driver`, `Dockerfile.runtime-bridge` — lightweight runtime images (copy pre-built binary into ubuntu:24.04; requires glibc 2.38+ and libbsd0)
- `Dockerfile`, `Dockerfile.aeron-driver`, `Dockerfile.clickhouse-bridge` — full build images (compile from source)
- `k8s/` — K8s manifests (namespace, ClickHouse StatefulSet, connector Deployment, schema Job)
- `scripts/aws-setup.sh` — creates ECR repos and GitHub OIDC IAM role
- `scripts/node-setup.sh` — bootstraps k3s on the EC2 node
- `aws-tuning.sh` — CPU isolation, IRQ affinity, huge pages
- `runbook.md` — shard migration procedure
