use clap::Parser;
use rusteron_media_driver::{AeronDriver, AeronDriverContext};
use std::ffi::CString;
use std::sync::atomic::Ordering;

/// Standalone Aeron C media driver.
///
/// Run this as a long-lived background process before starting any connector
/// instances. The connector connects to the same `--dir` path via IPC.
///
/// Production tip: run under a process supervisor (systemd, launchd) so the
/// driver survives connector restarts. Do NOT embed the driver in the
/// connector binary for the same reason.
#[derive(Parser)]
#[command(name = "aeron-driver", about = "Aeron C media driver")]
struct Args {
    /// Directory for Aeron CnC and term buffers.
    ///
    /// Must match aeron.media_driver_dir in the connector config.
    /// Linux: /dev/shm/aeron   macOS: /tmp/aeron
    #[arg(long, default_value = "/tmp/aeron")]
    dir: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    std::fs::create_dir_all(&args.dir)?;
    eprintln!("aeron-driver: starting, dir={}", args.dir);

    let ctx = AeronDriverContext::new()?;
    let dir_cstr = CString::new(args.dir.as_str())?;
    ctx.set_dir(&dir_cstr)?;

    let (stop, handle) = AeronDriver::launch_embedded(ctx, false);

    // Block until Ctrl-C.
    ctrlc::set_handler(move || {
        eprintln!("aeron-driver: shutdown signal received");
        stop.store(true, Ordering::SeqCst);
    })?;

    handle.join().ok();
    eprintln!("aeron-driver: stopped");
    Ok(())
}
