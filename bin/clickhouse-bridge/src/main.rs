use std::collections::HashSet;
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
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Signal handling
//
// Two subtle interactions make standard Ctrl+C handling impossible here:
//
// 1. Tokio blocks all signals in its worker threads via pthread_sigmask so
//    that it can manage delivery itself.  Every thread spawned after the
//    runtime starts inherits the blocked mask.  sigaction handlers (ctrlc
//    crate, tokio::signal) and the default SIG_DFL disposition therefore
//    never fire — the signal stays pending and nobody consumes it.
//
// 2. The Aeron C client calls tcsetattr() during initialisation and clears
//    the ISIG flag, which disables signal generation from control characters.
//    Ctrl+C no longer sends SIGINT to the process; the terminal just echoes
//    the literal byte sequence "^C".
//
// Fix:
//   a) Block SIGINT+SIGTERM in main() BEFORE building the tokio runtime so
//      all worker threads inherit the blocked mask.
//   b) Spawn a std::thread that polls until Aeron has finished setup, then
//      restores ISIG via tcsetattr (so Ctrl+C sends SIGINT again) and calls
//      sigwait(), the only POSIX mechanism that can atomically consume a
//      blocked-pending signal.
// ---------------------------------------------------------------------------

fn install_sigint_handler() -> Arc<AtomicBool> {
    unsafe {
        let mut mask: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut mask);
        libc::sigaddset(&mut mask, libc::SIGINT);
        libc::sigaddset(&mut mask, libc::SIGTERM);
        libc::pthread_sigmask(libc::SIG_BLOCK, &mask, std::ptr::null_mut());
    }

    let aeron_ready = Arc::new(AtomicBool::new(false));
    let ready = aeron_ready.clone();

    std::thread::Builder::new()
        .name("sigint-watcher".into())
        .spawn(move || {
            while !ready.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(5));
            }

            unsafe {
                // Restore signal dispositions in case Aeron touched them.
                let mut sa: libc::sigaction = std::mem::zeroed();
                sa.sa_sigaction = libc::SIG_DFL;
                libc::sigaction(libc::SIGINT,  &sa, std::ptr::null_mut());
                libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());

                // Re-enable ISIG so Ctrl+C generates SIGINT again.
                // Aeron's tcsetattr() call during start-up clears this flag.
                let mut tty: libc::termios = std::mem::zeroed();
                if libc::tcgetattr(libc::STDIN_FILENO, &mut tty) == 0 {
                    tty.c_lflag |= libc::ISIG;
                    libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &tty);
                }

                let mut wait_mask: libc::sigset_t = std::mem::zeroed();
                libc::sigemptyset(&mut wait_mask);
                libc::sigaddset(&mut wait_mask, libc::SIGINT);
                libc::sigaddset(&mut wait_mask, libc::SIGTERM);
                let mut sig = 0i32;
                libc::sigwait(&wait_mask as *const libc::sigset_t, &mut sig as *mut libc::c_int);
            }

            eprintln!("\nshutdown signal received, stopping bridge");
            unsafe { libc::_exit(0); }
        })
        .expect("failed to spawn sigint-watcher thread");

    aeron_ready
}

mod rows;
use rows::*;

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

// Price and quantity columns use Decimal(18, 8) — stored on the wire as Int64
// with an implicit scale of 8.  The bridge rescales every mantissa from its
// instrument-specific scale to the fixed 8-decimal representation before
// inserting (see rows::to_d8).
//
// funding_rate uses Decimal(18, 9) because the normalizer hardcodes scale=9
// for that field, so the mantissa maps directly with no conversion.
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
    price              Decimal(18, 8),
    qty                Decimal(18, 8),
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
    bid_price          Decimal(18, 8),
    bid_qty            Decimal(18, 8),
    ask_price          Decimal(18, 8),
    ask_qty            Decimal(18, 8),
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
    mark_price         Decimal(18, 8),
    index_price        Decimal(18, 8)
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
    funding_rate       Decimal(18, 9),
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
    price              Decimal(18, 8),
    qty                Decimal(18, 8),
    avg_price          Decimal(18, 8),
    last_filled_qty    Decimal(18, 8)
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
    open_interest      Decimal(18, 8)
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
    bid_prices         Array(Decimal(18, 8)),
    bid_qtys           Array(Decimal(18, 8)),
    ask_prices         Array(Decimal(18, 8)),
    ask_qtys           Array(Decimal(18, 8))
) ENGINE = MergeTree()
ORDER BY (symbol, exchange_event_ts);

-- Instrument reference data.  ReplacingMergeTree keeps the most recent
-- definition per instrument so re-published updates (e.g. is_trading flip)
-- replace old entries rather than accumulate.
CREATE TABLE IF NOT EXISTS instruments (
    local_recv_ts   Int64,
    venue_id        UInt8,
    market_type     UInt8,
    instrument_id   UInt32,
    symbol          String,
    base_asset      String,
    quote_asset     String,
    price_scale     UInt32,
    qty_scale       UInt32,
    tick_size       Decimal(18, 8),
    step_size       Decimal(18, 8),
    min_qty         Decimal(18, 8),
    min_notional    Decimal(18, 8),
    contract_size   Decimal(18, 8),
    is_trading      UInt8
) ENGINE = ReplacingMergeTree(local_recv_ts)
ORDER BY (venue_id, market_type, instrument_id);
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
    instruments:   Inserter<InstrumentRow>,
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
            // Instrument definitions arrive rarely (startup + on change) so
            // a small batch limit is fine; the period flush handles the rest.
            instruments: client.inserter("instruments")
                .with_max_rows(1_000)
                .with_period(Some(batch_period)),
        }
    }

    async fn write(&mut self, msg: &NormalizedMessage) {
        let result: clickhouse::error::Result<()> = match msg {
            NormalizedMessage::InstrumentDefinition(m) => self.instruments.write(&InstrumentRow::from(m)).await,
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
        try_commit!(self.instruments,   "instruments");
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
        try_end!(self.instruments,   "instruments");
    }
}

// ---------------------------------------------------------------------------
// Startup table check
// ---------------------------------------------------------------------------

const REQUIRED_TABLES: &[&str] = &[
    "trades",
    "best_bid_offers",
    "mark_prices",
    "funding_rates",
    "liquidations",
    "open_interest",
    "book_deltas",
    "instruments",
];

#[derive(clickhouse::Row, Deserialize)]
struct TableRow {
    name: String,
}


async fn check_tables(client: &Client, database: &str) -> Result<()> {
    let rows: Vec<TableRow> = client
        .query("SELECT name FROM system.tables WHERE database = ? AND name IN ('trades','best_bid_offers','mark_prices','funding_rates','liquidations','open_interest','book_deltas','instruments')")
        .bind(database)
        .fetch_all::<TableRow>()
        .await
        .map_err(|e| anyhow::anyhow!("ClickHouse connection failed: {e}\nIs ClickHouse running at the configured URL?"))?;

    let existing: HashSet<&str> = rows.iter().map(|r| r.name.as_str()).collect();
    let missing: Vec<&&str> = REQUIRED_TABLES.iter().filter(|t| !existing.contains(**t)).collect();

    if !missing.is_empty() {
        let list = missing.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(", ");
        anyhow::bail!(
            "Missing tables in database '{database}': {list}\n\
             Create them with:\n\
             \n  cargo run -p clickhouse-bridge -- --print-schema \\\n  | clickhouse client --database {database} --multiquery\n"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let aeron_ready = install_sigint_handler();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(aeron_ready))
}

async fn run(aeron_ready: Arc<AtomicBool>) -> Result<()> {
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

    check_tables(&client, &args.database).await?;
    info!(database = %args.database, "all required tables present");

    let mut inserters = Inserters::new(
        &client,
        args.batch_rows,
        Duration::from_secs(args.batch_secs),
    );

    // The Aeron poll thread sends decoded messages; the async task consumes
    // them and writes to ClickHouse.  Unbounded so the poll thread never
    // blocks — if ClickHouse is slow, memory grows until the next commit.
    let (tx, mut rx)  = mpsc::unbounded_channel::<NormalizedMessage>();
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let poll_shutdown = shutdown_flag.clone();

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
        // Tell the sigint-watcher thread that Aeron has finished all setup,
        // including any sigaction() calls.  The watcher will now restore
        // SIGINT to SIG_DFL (overriding any SIG_IGN set by Aeron) and call
        // sigwait() to catch the next Ctrl+C.
        aeron_ready.store(true, Ordering::Release);

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
                        // Stale/garbage ring-buffer frames on inactive streams
                        // are expected — log at debug to avoid spam.
                        Err(e)  => { debug!(error = %e, "decode error"); }
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

    let write_timeout  = Duration::from_secs(3);
    let commit_timeout = Duration::from_secs(3);
    let mut msgs_written: u64 = 0;

    loop {
        tokio::select! {
            _ = flush_ticker.tick() => {
                if tokio::time::timeout(commit_timeout, inserters.commit()).await.is_err() {
                    warn!("ClickHouse commit timed out");
                }
            }
            msg = rx.recv() => {
                let msg = match msg {
                    Some(m) => m,
                    None    => { info!("message channel closed"); break; }
                };
                if tokio::time::timeout(write_timeout, inserters.write(&msg)).await.is_err() {
                    warn!("ClickHouse write timed out");
                }
                msgs_written += 1;
                if msgs_written % 10_000 == 0 {
                    if tokio::time::timeout(commit_timeout, inserters.commit()).await.is_err() {
                        warn!("ClickHouse commit timed out");
                    }
                }
            }
        }
    }

    // Only reached if the message channel closes (poll thread exited).
    shutdown_flag.store(true, Ordering::Relaxed);
    let flush = inserters.end();
    if tokio::time::timeout(Duration::from_secs(5), flush).await.is_err() {
        warn!("ClickHouse final flush timed out after 5 s");
    }
    std::process::exit(0);
}
