use std::ffi::CString;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use clickhouse::Client;
use clickhouse::inserter::Inserter;
use connector_core::{NormalizedMessage, HEADER_SIZE};
use rusteron_client::{Aeron, AeronContext, Handlers};
use tokio::sync::mpsc;
use tracing::{info, warn};

mod rows;
use rows::*;

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

const CREATE_TABLES: &str = r#"
CREATE TABLE IF NOT EXISTS trades (
    exchange_event_ts  Int64,
    local_recv_ts      Int64,
    local_publish_ts   Int64,
    venue_id           UInt8,
    market_type        UInt8,
    instrument_id      UInt32,
    symbol             String,
    sequence_number    UInt64,
    trade_id           UInt64,
    price              Int64,
    qty                Int64,
    trade_ts           Int64,
    is_buyer_maker     UInt8,
    aggressor_side     UInt8
) ENGINE = MergeTree()
ORDER BY (symbol, exchange_event_ts);

CREATE TABLE IF NOT EXISTS best_bid_offers (
    exchange_event_ts  Int64,
    local_recv_ts      Int64,
    local_publish_ts   Int64,
    venue_id           UInt8,
    market_type        UInt8,
    instrument_id      UInt32,
    symbol             String,
    sequence_number    UInt64,
    bid_price          Int64,
    bid_qty            Int64,
    ask_price          Int64,
    ask_qty            Int64,
    update_id          UInt64
) ENGINE = MergeTree()
ORDER BY (symbol, exchange_event_ts);

CREATE TABLE IF NOT EXISTS mark_prices (
    exchange_event_ts  Int64,
    local_recv_ts      Int64,
    local_publish_ts   Int64,
    venue_id           UInt8,
    market_type        UInt8,
    instrument_id      UInt32,
    symbol             String,
    sequence_number    UInt64,
    mark_price         Int64,
    index_price        Int64
) ENGINE = MergeTree()
ORDER BY (symbol, exchange_event_ts);

CREATE TABLE IF NOT EXISTS funding_rates (
    exchange_event_ts  Int64,
    local_recv_ts      Int64,
    local_publish_ts   Int64,
    venue_id           UInt8,
    market_type        UInt8,
    instrument_id      UInt32,
    symbol             String,
    sequence_number    UInt64,
    funding_rate       Int64,
    next_funding_time  Int64
) ENGINE = MergeTree()
ORDER BY (symbol, exchange_event_ts);

CREATE TABLE IF NOT EXISTS liquidations (
    exchange_event_ts  Int64,
    local_recv_ts      Int64,
    local_publish_ts   Int64,
    venue_id           UInt8,
    market_type        UInt8,
    instrument_id      UInt32,
    symbol             String,
    sequence_number    UInt64,
    side               UInt8,
    price              Int64,
    qty                Int64,
    avg_price          Int64,
    last_filled_qty    Int64
) ENGINE = MergeTree()
ORDER BY (symbol, exchange_event_ts);

CREATE TABLE IF NOT EXISTS open_interest (
    exchange_event_ts  Int64,
    local_recv_ts      Int64,
    local_publish_ts   Int64,
    venue_id           UInt8,
    market_type        UInt8,
    instrument_id      UInt32,
    symbol             String,
    sequence_number    UInt64,
    open_interest      Int64
) ENGINE = MergeTree()
ORDER BY (symbol, exchange_event_ts);

CREATE TABLE IF NOT EXISTS book_deltas (
    exchange_event_ts  Int64,
    local_recv_ts      Int64,
    local_publish_ts   Int64,
    venue_id           UInt8,
    market_type        UInt8,
    instrument_id      UInt32,
    symbol             String,
    sequence_number    UInt64,
    first_update_id    UInt64,
    final_update_id    UInt64,
    prev_update_id     UInt64,
    bid_prices         Array(Int64),
    bid_qtys           Array(Int64),
    ask_prices         Array(Int64),
    ask_qtys           Array(Int64)
) ENGINE = MergeTree()
ORDER BY (symbol, exchange_event_ts);
"#;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "clickhouse-bridge", about = "Aeron → ClickHouse market data bridge")]
struct Args {
    /// Aeron media driver directory.
    #[arg(long, default_value = "/tmp/aeron")]
    dir: String,

    /// Aeron channel URI.
    #[arg(long, default_value = "aeron:ipc")]
    channel: String,

    /// Aeron stream IDs to subscribe to (comma-separated).
    /// Default: 1..=total_shards (subscribe to every shard).
    #[arg(long, value_delimiter = ',')]
    streams: Vec<i32>,

    /// Total number of logical shards — used to derive default stream IDs.
    #[arg(long, default_value_t = 16)]
    total_shards: i32,

    /// ClickHouse HTTP URL.
    #[arg(long, default_value = "http://localhost:8123")]
    clickhouse_url: String,

    /// ClickHouse database name.
    #[arg(long, default_value = "market_data")]
    database: String,

    #[arg(long, default_value = "default")]
    username: String,

    #[arg(long, default_value = "")]
    password: String,

    /// Max rows buffered per table before forcing a ClickHouse insert.
    #[arg(long, default_value_t = 10_000)]
    batch_rows: u64,

    /// Max seconds between ClickHouse inserts per table.
    #[arg(long, default_value_t = 1)]
    batch_secs: u64,

    /// Print the CREATE TABLE DDL and exit.
    #[arg(long)]
    print_schema: bool,
}

// ---------------------------------------------------------------------------
// Inserter bundle
// ---------------------------------------------------------------------------

struct Inserters {
    trades:        Inserter<TradeRow>,
    bbo:           Inserter<BboRow>,
    mark_price:    Inserter<MarkPriceRow>,
    funding:       Inserter<FundingRateRow>,
    liquidation:   Inserter<LiquidationRow>,
    open_interest: Inserter<OpenInterestRow>,
    book_delta:    Inserter<BookDeltaRow>,
}

impl Inserters {
    fn new(client: &Client, batch_rows: u64, batch_period: Duration) -> Self {
        Self {
            trades: client.inserter("trades")
                .with_max_rows(batch_rows)
                .with_period(Some(batch_period)),
            bbo: client.inserter("best_bid_offers")
                .with_max_rows(batch_rows)
                .with_period(Some(batch_period)),
            mark_price: client.inserter("mark_prices")
                .with_max_rows(batch_rows)
                .with_period(Some(batch_period)),
            funding: client.inserter("funding_rates")
                .with_max_rows(batch_rows)
                .with_period(Some(batch_period)),
            liquidation: client.inserter("liquidations")
                .with_max_rows(batch_rows)
                .with_period(Some(batch_period)),
            open_interest: client.inserter("open_interest")
                .with_max_rows(batch_rows)
                .with_period(Some(batch_period)),
            book_delta: client.inserter("book_deltas")
                .with_max_rows(batch_rows)
                .with_period(Some(batch_period)),
        }
    }

    async fn write(&mut self, msg: &NormalizedMessage) {
        let result: clickhouse::error::Result<()> = match msg {
            NormalizedMessage::Trade(m)        => self.trades.write(&TradeRow::from(m)).await,
            NormalizedMessage::BestBidOffer(m) => self.bbo.write(&BboRow::from(m)).await,
            NormalizedMessage::MarkPrice(m)    => self.mark_price.write(&MarkPriceRow::from(m)).await,
            NormalizedMessage::FundingRate(m)  => self.funding.write(&FundingRateRow::from(m)).await,
            NormalizedMessage::Liquidation(m)  => self.liquidation.write(&LiquidationRow::from(m)).await,
            NormalizedMessage::OpenInterest(m) => self.open_interest.write(&OpenInterestRow::from(m)).await,
            NormalizedMessage::BookDelta(m)    => self.book_delta.write(&BookDeltaRow::from(m)).await,
            _ => return,
        };
        if let Err(e) = result {
            warn!(error = %e, "inserter write error");
        }
    }

    async fn commit(&mut self) {
        macro_rules! try_commit {
            ($ins:expr, $name:literal) => {
                if let Err(e) = $ins.commit().await {
                    warn!(error = %e, table = $name, "commit failed");
                }
            };
        }
        try_commit!(self.trades,        "trades");
        try_commit!(self.bbo,           "best_bid_offers");
        try_commit!(self.mark_price,    "mark_prices");
        try_commit!(self.funding,       "funding_rates");
        try_commit!(self.liquidation,   "liquidations");
        try_commit!(self.open_interest, "open_interest");
        try_commit!(self.book_delta,    "book_deltas");
    }

    async fn end(self) {
        macro_rules! try_end {
            ($ins:expr, $name:literal) => {
                if let Err(e) = $ins.end().await {
                    warn!(error = %e, table = $name, "final flush failed");
                }
            };
        }
        try_end!(self.trades,        "trades");
        try_end!(self.bbo,           "best_bid_offers");
        try_end!(self.mark_price,    "mark_prices");
        try_end!(self.funding,       "funding_rates");
        try_end!(self.liquidation,   "liquidations");
        try_end!(self.open_interest, "open_interest");
        try_end!(self.book_delta,    "book_deltas");
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    if args.print_schema {
        println!("{CREATE_TABLES}");
        return Ok(());
    }

    let stream_ids: Vec<i32> = if args.streams.is_empty() {
        (1..=args.total_shards).collect()
    } else {
        args.streams.clone()
    };

    info!(
        streams    = ?stream_ids,
        dir        = %args.dir,
        clickhouse = %args.clickhouse_url,
        database   = %args.database,
        batch_rows = args.batch_rows,
        batch_secs = args.batch_secs,
        "clickhouse-bridge starting"
    );

    let client = Client::default()
        .with_url(&args.clickhouse_url)
        .with_database(&args.database)
        .with_user(&args.username)
        .with_password(&args.password);

    let mut inserters = Inserters::new(
        &client,
        args.batch_rows,
        Duration::from_secs(args.batch_secs),
    );

    // The Aeron poll thread sends decoded messages; the async task consumes
    // them and writes to ClickHouse.  Unbounded so the poll thread never
    // blocks — if ClickHouse is slow, memory grows until the next commit.
    let (tx, mut rx) = mpsc::unbounded_channel::<NormalizedMessage>();
    let shutdown_flag  = Arc::new(AtomicBool::new(false));
    let poll_shutdown  = shutdown_flag.clone();

    let dir     = args.dir.clone();
    let channel = args.channel.clone();

    // Aeron C library is synchronous — run the poll loop on a blocking thread.
    let _poll_handle = tokio::task::spawn_blocking(move || -> Result<()> {
        let context  = AeronContext::new()?;
        let dir_cstr = CString::new(dir.as_str())?;
        context.set_dir(&dir_cstr)?;

        let aeron = Aeron::new(&context)?;
        aeron.start()?;

        let channel_cstr = CString::new(channel.as_str())?;
        let mut subs     = Vec::with_capacity(stream_ids.len());

        for &stream_id in &stream_ids {
            let sub = aeron.add_subscription(
                &channel_cstr,
                stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
                Duration::from_secs(5),
            )?;
            subs.push(sub);
        }

        info!(streams = subs.len(), "Aeron subscriptions established");

        loop {
            if poll_shutdown.load(Ordering::Relaxed) {
                break;
            }

            let mut fragments = 0i32;
            for sub in &subs {
                fragments += sub.poll_once(|bytes, _| {
                    if bytes.len() < HEADER_SIZE {
                        return;
                    }
                    match NormalizedMessage::from_bytes(bytes) {
                        Ok(msg) => { tx.send(msg).ok(); }
                        Err(e)  => { warn!(error = %e, "decode error"); }
                    }
                }, 256).unwrap_or(0);
            }

            if fragments == 0 {
                std::thread::sleep(Duration::from_micros(50));
            }
        }

        info!("Aeron poll thread stopped");
        Ok(())
    });

    let mut flush_ticker = tokio::time::interval(Duration::from_secs(args.batch_secs));
    flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut msgs_written: u64 = 0;

    loop {
        tokio::select! {
            // biased: shutdown and flush are checked before messages so that a
            // flood of incoming frames cannot starve the Ctrl+C branch.
            biased;

            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signal received");
                break;
            }
            _ = flush_ticker.tick() => {
                inserters.commit().await;
            }
            msg = rx.recv() => {
                match msg {
                    Some(msg) => {
                        inserters.write(&msg).await;
                        msgs_written += 1;
                        if msgs_written % 10_000 == 0 {
                            inserters.commit().await;
                        }
                    }
                    None => {
                        info!("message channel closed");
                        break;
                    }
                }
            }
        }
    }

    // Signal the Aeron poll thread to stop.
    shutdown_flag.store(true, Ordering::Relaxed);

    // Flush remaining buffered rows to ClickHouse; give it 5 s then give up.
    // process::exit(0) is used instead of returning so we don't wait for the
    // spawn_blocking thread (which may still be mid-poll) to be joined by the
    // tokio runtime drop.
    let flush = inserters.end();
    if tokio::time::timeout(Duration::from_secs(5), flush).await.is_err() {
        warn!("ClickHouse final flush timed out after 5 s");
    }

    std::process::exit(0);
}
