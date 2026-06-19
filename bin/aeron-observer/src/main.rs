use clap::Parser;
use connector_core::{MessageHeader, TS_NONE, HEADER_SIZE};
use rusteron_client::{Aeron, AeronContext};
use std::ffi::CString;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "aeron-observer", about = "Subscribe to the connector Aeron stream and report latency breakdowns")]
struct Args {
    /// Aeron media driver directory. Must match aeron.media_driver_dir in the connector config.
    #[arg(long, default_value = "/tmp/aeron")]
    dir: String,

    /// Aeron channel URI.
    #[arg(long, default_value = "aeron:ipc")]
    channel: String,

    /// Aeron stream ID to subscribe to (shard N uses stream N+1).
    #[arg(long, default_value_t = 1)]
    stream: i32,

    /// Stats print interval in seconds.
    #[arg(long, default_value_t = 5)]
    interval: u64,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

fn fmt_ns(ns: i64) -> String {
    let ns = ns.max(0) as u64;
    if ns < 1_000 {
        format!("{ns:>8} ns")
    } else if ns < 1_000_000 {
        format!("{:>7.1} µs", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:>7.1} ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:>7.2}  s", ns as f64 / 1_000_000_000.0)
    }
}

// ---------------------------------------------------------------------------
// Interval stats
// ---------------------------------------------------------------------------

#[derive(Default)]
struct LatencyBucket {
    samples: Vec<i64>,
}

impl LatencyBucket {
    fn push(&mut self, ns: i64) {
        if ns > 0 {
            self.samples.push(ns);
        }
    }

    fn percentiles(&mut self) -> Option<(i64, i64, i64, i64)> {
        if self.samples.is_empty() {
            return None;
        }
        self.samples.sort_unstable();
        let s = &self.samples;
        let n = s.len();
        let p = |f: f64| s[((n as f64 * f).ceil() as usize).saturating_sub(1).min(n - 1)];
        Some((p(0.5), p(0.99), p(0.999), *s.last().unwrap()))
    }

    fn count(&self) -> usize { self.samples.len() }

    fn reset(&mut self) { self.samples.clear(); }
}

struct IntervalStats {
    wire:            LatencyBucket, // exchange_event → local_recv
    encode:          LatencyBucket, // local_recv → local_publish
    ipc:             LatencyBucket, // local_publish → observer_recv
    e2e:             LatencyBucket, // exchange_event → observer_recv
    total_msgs:      u64,
    no_exchange_ts:  u64,           // msgs without exchange timestamp (bookTicker etc.)
}

impl IntervalStats {
    fn new() -> Self {
        Self {
            wire:           LatencyBucket::default(),
            encode:         LatencyBucket::default(),
            ipc:            LatencyBucket::default(),
            e2e:            LatencyBucket::default(),
            total_msgs:     0,
            no_exchange_ts: 0,
        }
    }

    fn record(&mut self, hdr: &MessageHeader, observer_recv_ts: i64) {
        self.total_msgs += 1;

        self.encode.push(hdr.local_publish_ts - hdr.local_recv_ts);
        self.ipc   .push(observer_recv_ts - hdr.local_publish_ts);

        if hdr.exchange_event_ts != TS_NONE {
            self.wire.push(hdr.local_recv_ts   - hdr.exchange_event_ts);
            self.e2e .push(observer_recv_ts    - hdr.exchange_event_ts);
        } else {
            self.no_exchange_ts += 1;
        }
    }

    fn print_and_reset(&mut self, elapsed: Duration) {
        let secs = elapsed.as_secs_f64().max(0.001);
        let rate  = self.total_msgs as f64 / secs;

        let now = chrono_local();
        println!("────────────────────────────────────────────────────────────────");
        println!("{now} │ {:.0}s │ {} msgs │ {:.0}/s",
            secs, fmt_count(self.total_msgs), rate);
        println!("{:<10}  {:>12}  {:>12}  {:>12}  {:>12}  {:>8}",
            "", "p50", "p99", "p99.9", "max", "count");

        print_bucket("wire",   &mut self.wire);
        print_bucket("encode", &mut self.encode);
        print_bucket("ipc",    &mut self.ipc);
        print_bucket("e2e",    &mut self.e2e);

        if self.no_exchange_ts > 0 {
            println!("  (no exchange_ts: {} msgs — bookTicker streams have no E field)",
                fmt_count(self.no_exchange_ts));
        }

        self.wire.reset();
        self.encode.reset();
        self.ipc.reset();
        self.e2e.reset();
        self.total_msgs     = 0;
        self.no_exchange_ts = 0;
    }
}

fn print_bucket(label: &str, b: &mut LatencyBucket) {
    match b.percentiles() {
        None => println!("{label:<10}  {:>12}  {:>12}  {:>12}  {:>12}  {:>8}",
            "-", "-", "-", "-", 0),
        Some((p50, p99, p999, max)) => {
            println!("{label:<10}  {p50:>12}  {p99:>12}  {p999:>12}  {max:>12}  {cnt:>8}",
                p50  = fmt_ns(p50),
                p99  = fmt_ns(p99),
                p999 = fmt_ns(p999),
                max  = fmt_ns(max),
                cnt  = fmt_count(b.count() as u64),
            );
        }
    }
}

fn fmt_count(n: u64) -> String {
    // Simple thousands separator
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { out.push(','); }
        out.push(c);
    }
    out.chars().rev().collect()
}

fn chrono_local() -> String {
    // Wall-clock string without a chrono dependency.
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02} UTC")
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    eprintln!("aeron-observer: connecting, dir={} channel={} stream={}",
        args.dir, args.channel, args.stream);

    let context = AeronContext::new()?;
    let dir_cstr = CString::new(args.dir.as_str())?;
    context.set_dir(&dir_cstr)?;

    let aeron = Aeron::new(&context)?;
    aeron.start()?;

    let channel_cstr = CString::new(args.channel.as_str())?;

    let subscription = aeron.add_subscription(
        &channel_cstr,
        args.stream,
        rusteron_client::Handlers::no_available_image_handler(),
        rusteron_client::Handlers::no_unavailable_image_handler(),
        Duration::from_secs(5),
    )?;

    eprintln!("aeron-observer: subscription added, waiting for publisher...");
    while !subscription.is_connected() {
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("aeron-observer: publisher connected — streaming latency stats");
    println!("────────────────────────────────────────────────────────────────");
    println!("{:<10}  {:>12}  {:>12}  {:>12}  {:>12}  {:>8}",
        "segment", "p50", "p99", "p99.9", "max", "count");

    let mut stats    = IntervalStats::new();
    let mut deadline = Instant::now() + Duration::from_secs(args.interval);

    loop {
        let fragments = subscription.poll_once(|bytes: &[u8], _hdr| {
            let obs_ts = now_nanos();
            if bytes.len() < HEADER_SIZE {
                return;
            }
            match MessageHeader::decode(bytes) {
                Ok(hdr) => stats.record(&hdr, obs_ts),
                Err(_)  => {}
            }
        }, 256)?;

        if fragments == 0 {
            // No data — yield briefly rather than burning a core.
            std::thread::sleep(Duration::from_micros(10));
        }

        let now = Instant::now();
        if now >= deadline {
            let elapsed = Duration::from_secs(args.interval); // fixed interval for rate
            stats.print_and_reset(elapsed);
            deadline = now + Duration::from_secs(args.interval);
        }
    }
}
