use std::collections::HashMap;
use std::sync::mpsc;

use connector_config::AeronConfig;
use tracing::warn;

use crate::error::PublisherError;
use crate::publication::{ChannelPublication, NullPublication, OfferResult, Publication};

// ---------------------------------------------------------------------------
// Channel helpers
// ---------------------------------------------------------------------------

/// Aeron IPC channel string (same-host shared memory).
pub fn ipc_channel() -> &'static str {
    "aeron:ipc"
}

/// Aeron UDP channel string for cross-host delivery.
pub fn udp_channel(endpoint: &str) -> String {
    format!("aeron:udp?endpoint={}", endpoint)
}

/// Map a logical shard ID to an Aeron stream ID.
///
/// Stream IDs must be > 0 per the Aeron spec. Shard 0 → stream 1.
pub fn shard_stream_id(shard_id: u32) -> i32 {
    (shard_id + 1) as i32
}

/// Select the channel string from [`AeronConfig`] (IPC takes priority).
pub fn channel_from_config(cfg: &AeronConfig) -> String {
    if cfg.ipc_enabled {
        ipc_channel().to_owned()
    } else if let Some(ep) = &cfg.udp_endpoint {
        udp_channel(ep)
    } else {
        warn!("AeronConfig has neither ipc_enabled nor udp_endpoint; defaulting to IPC");
        ipc_channel().to_owned()
    }
}

// ---------------------------------------------------------------------------
// ShardedPublisher
// ---------------------------------------------------------------------------

/// Routes encoded binary messages to the correct per-shard [`Publication`].
///
/// A single `ShardedPublisher` owns all publications for the shards
/// assigned to this process instance. It is `!Sync` so that only one thread
/// can drive a given shard at a time (single-writer guarantee).
pub struct ShardedPublisher<P> {
    shards: HashMap<u32, P>,
}

impl<P: Publication> ShardedPublisher<P> {
    /// Create a publisher from a list of `(shard_id, publication)` pairs.
    ///
    /// Duplicate shard IDs are silently overwritten.
    pub fn new(shards: impl IntoIterator<Item = (u32, P)>) -> Self {
        Self { shards: shards.into_iter().collect() }
    }

    /// Offer `bytes` to the publication for `shard_id`.
    ///
    /// Returns `Err(UnknownShard)` if no publication was registered for this
    /// shard. Otherwise returns the [`OfferResult`] from the publication.
    pub fn offer(&mut self, shard_id: u32, bytes: &[u8]) -> Result<OfferResult, PublisherError> {
        match self.shards.get_mut(&shard_id) {
            None      => Err(PublisherError::UnknownShard(shard_id)),
            Some(pub_) => Ok(pub_.offer(bytes)),
        }
    }

    /// Access the publication for `shard_id` (e.g., to read stats).
    pub fn publication(&self, shard_id: u32) -> Option<&P> {
        self.shards.get(&shard_id)
    }

    pub fn publication_mut(&mut self, shard_id: u32) -> Option<&mut P> {
        self.shards.get_mut(&shard_id)
    }

    /// Iterate over all registered shard IDs.
    pub fn shard_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.shards.keys().copied()
    }

    /// Number of shards this publisher owns.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Returns `true` if all publications report at least one connected subscriber.
    pub fn all_connected(&self) -> bool {
        self.shards.values().all(|p| p.is_connected())
    }
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

/// A [`ShardedPublisher`] using boxed trait objects.
///
/// Allows mixing publication backends (null, Aeron, channel) at runtime
/// without changing the type seen by the rest of the pipeline.
pub type DynShardedPublisher = ShardedPublisher<Box<dyn Publication + Send>>;

/// Builds a [`DynShardedPublisher`] backed by [`NullPublication`]s.
///
/// Used when no Aeron media driver is available (tests, benchmarks, dev).
pub fn build_null_boxed(owned_shards: &[u32]) -> DynShardedPublisher {
    ShardedPublisher::new(owned_shards.iter().map(|&id| {
        let p: Box<dyn Publication + Send> = Box::new(NullPublication::default());
        (id, p)
    }))
}

/// Builds a [`ShardedPublisher`] backed by [`NullPublication`]s.
///
/// Used when no Aeron media driver is available (tests, benchmarks, dev).
pub fn build_null(owned_shards: &[u32]) -> ShardedPublisher<NullPublication> {
    ShardedPublisher::new(
        owned_shards.iter().map(|&id| (id, NullPublication::default())),
    )
}

/// Connects to the Aeron media driver and builds a [`DynShardedPublisher`]
/// with one publication per owned shard.
///
/// Each shard `n` publishes to stream `n + 1` (Aeron stream IDs start at 1).
/// The channel is chosen from config: IPC takes priority over UDP.
///
/// Blocks until all publications are connected (up to 5 s per shard).
/// Returns [`PublisherError::AeronConnect`] if the driver is unreachable or
/// a publication cannot be registered within the timeout.
#[cfg(feature = "aeron")]
pub fn build_aeron(
    cfg: &connector_config::AeronConfig,
    owned_shards: &[u32],
) -> Result<DynShardedPublisher, crate::error::PublisherError> {
    use rusteron_client::{Aeron, AeronContext};
    use std::ffi::CString;
    use std::time::Duration;
    use crate::error::PublisherError;
    use crate::publication::AeronClientPublication;

    let context = AeronContext::new()
        .map_err(|e| PublisherError::AeronConnect(e.to_string()))?;

    let dir = CString::new(cfg.media_driver_dir.as_str())
        .map_err(|e| PublisherError::AeronConnect(e.to_string()))?;
    context.set_dir(&dir)
        .map_err(|e| PublisherError::AeronConnect(e.to_string()))?;

    let aeron = Aeron::new(&context)
        .map_err(|e| PublisherError::AeronConnect(e.to_string()))?;
    aeron.start()
        .map_err(|e| PublisherError::AeronConnect(e.to_string()))?;

    let channel = channel_from_config(cfg);
    let channel_cstr = CString::new(channel.as_str())
        .map_err(|e| PublisherError::AeronConnect(e.to_string()))?;

    let mut shards: Vec<(u32, Box<dyn Publication + Send>)> = Vec::with_capacity(owned_shards.len());
    for &shard_id in owned_shards {
        let stream_id = shard_stream_id(shard_id);
        let pub_ = aeron
            .add_publication(&channel_cstr, stream_id, Duration::from_secs(5))
            .map_err(|e| PublisherError::AeronConnect(e.to_string()))?;
        shards.push((shard_id, Box::new(AeronClientPublication::new(pub_, aeron.clone()))));
    }

    Ok(ShardedPublisher::new(shards))
}

/// Connects to the Aeron media driver, retrying forever until it succeeds.
///
/// Waits `retry_delay` between each attempt. The call suspends the async task
/// while sleeping so other tasks are not blocked.
///
/// Without the `aeron` feature this returns a null publisher immediately.
pub async fn build_aeron_with_retry(
    cfg:         &connector_config::AeronConfig,
    shards:      &[u32],
    retry_delay: std::time::Duration,
) -> DynShardedPublisher {
    #[cfg(feature = "aeron")]
    {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match build_aeron(cfg, shards) {
                Ok(p) => {
                    tracing::info!(
                        attempt,
                        channel = %channel_from_config(cfg),
                        "Aeron publisher connected"
                    );
                    return p;
                }
                Err(e) => {
                    tracing::warn!(
                        error    = %e,
                        attempt,
                        delay_ms = retry_delay.as_millis(),
                        "Aeron connect failed — retrying"
                    );
                    tokio::time::sleep(retry_delay).await;
                }
            }
        }
    }

    #[cfg(not(feature = "aeron"))]
    let _ = (cfg, retry_delay);

    build_null_boxed(shards)
}

/// Replace the publications for `shard_id` after a runtime `Closed` event.
///
/// Called synchronously from the publish hot path when [`OfferResult::Closed`]
/// is returned, meaning the media driver has died or restarted.  Retries
/// forever until the driver comes back.  Blocks the calling thread while
/// reconnecting — acceptable because messages are already being lost.
///
/// Without the `aeron` feature this is a no-op (the publisher is already null).
pub fn reconnect_sync(
    cfg:       &connector_config::AeronConfig,
    shard_id:  u32,
    publisher: &mut DynShardedPublisher,
) {
    #[cfg(feature = "aeron")]
    {
        let delay = std::time::Duration::from_millis(cfg.connect_retry_delay_ms);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match build_aeron(cfg, &[shard_id]) {
                Ok(p) => {
                    tracing::info!(shard_id, attempt, "Aeron publication reconnected");
                    *publisher = p;
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        shard_id,
                        error   = %e,
                        attempt,
                        "Aeron reconnect failed — retrying"
                    );
                    std::thread::sleep(delay);
                }
            }
        }
    }

    #[cfg(not(feature = "aeron"))]
    let _ = (cfg, shard_id, publisher);
}

/// Builds a [`ShardedPublisher`] where each shard sends to an mpsc channel.
///
/// Returns the publisher and a map of `shard_id → Receiver<Vec<u8>>`.
/// Useful for testing the full pipeline end-to-end without Aeron.
pub fn build_channel(
    owned_shards: &[u32],
    channel_capacity: usize,
) -> (ShardedPublisher<ChannelPublication>, HashMap<u32, mpsc::Receiver<Vec<u8>>>) {
    let mut publisher_shards = Vec::with_capacity(owned_shards.len());
    let mut receivers         = HashMap::with_capacity(owned_shards.len());

    for &shard_id in owned_shards {
        let (pub_, rx) = ChannelPublication::new(channel_capacity);
        publisher_shards.push((shard_id, pub_));
        receivers.insert(shard_id, rx);
    }

    (ShardedPublisher::new(publisher_shards), receivers)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publication::{NullPublication, OfferResult};
    use std::time::Duration;

    // --- shard_stream_id ---

    #[test]
    fn shard_zero_maps_to_stream_one() {
        assert_eq!(shard_stream_id(0), 1);
    }

    #[test]
    fn shard_stream_id_is_shard_plus_one() {
        for shard in 0u32..16 {
            assert_eq!(shard_stream_id(shard), (shard + 1) as i32);
        }
    }

    // --- channel strings ---

    #[test]
    fn ipc_channel_is_correct() {
        assert_eq!(ipc_channel(), "aeron:ipc");
    }

    #[test]
    fn udp_channel_embeds_endpoint() {
        assert_eq!(udp_channel("10.0.0.1:40123"), "aeron:udp?endpoint=10.0.0.1:40123");
    }

    // --- build_null ---

    #[test]
    fn build_null_creates_one_shard_per_id() {
        let pub_ = build_null(&[0, 2, 4]);
        assert_eq!(pub_.shard_count(), 3);
    }

    #[test]
    fn build_null_all_connected() {
        let pub_ = build_null(&[0, 1]);
        assert!(pub_.all_connected());
    }

    // --- ShardedPublisher routing ---

    #[test]
    fn offer_to_registered_shard_returns_ok() {
        let mut pub_ = ShardedPublisher::new([(0u32, NullPublication::default())]);
        let result = pub_.offer(0, b"msg").unwrap();
        assert!(result.is_ok());
    }

    #[test]
    fn offer_to_unknown_shard_returns_error() {
        let mut pub_: ShardedPublisher<NullPublication> = ShardedPublisher::new([]);
        let err = pub_.offer(99, b"msg").unwrap_err();
        assert_eq!(err, PublisherError::UnknownShard(99));
    }

    #[test]
    fn offer_routes_to_correct_shard() {
        let mut pub_ = build_null(&[0, 1, 2]);

        pub_.offer(0, b"aaa").unwrap();
        pub_.offer(0, b"bb").unwrap();
        pub_.offer(2, b"c").unwrap();

        assert_eq!(pub_.publication(0).unwrap().messages_offered, 2);
        assert_eq!(pub_.publication(1).unwrap().messages_offered, 0);
        assert_eq!(pub_.publication(2).unwrap().messages_offered, 1);
    }

    #[test]
    fn shard_ids_covers_all_registered_shards() {
        let pub_ = build_null(&[3, 7, 15]);
        let mut ids: Vec<u32> = pub_.shard_ids().collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![3, 7, 15]);
    }

    // --- build_channel ---

    #[test]
    fn build_channel_receiver_gets_offered_bytes() {
        let (mut pub_, mut rxs) = build_channel(&[0, 1], 8);

        pub_.offer(0, b"shard0_msg").unwrap();
        pub_.offer(1, b"shard1_msg").unwrap();

        assert_eq!(rxs.remove(&0).unwrap().recv().unwrap(), b"shard0_msg");
        assert_eq!(rxs.remove(&1).unwrap().recv().unwrap(), b"shard1_msg");
    }

    #[test]
    fn build_channel_back_pressure_when_full() {
        let (mut pub_, _rxs) = build_channel(&[0], 1);
        pub_.offer(0, b"fill").unwrap();
        let result = pub_.offer(0, b"overflow").unwrap();
        assert_eq!(result, OfferResult::BackPressured);
    }

    // --- channel_from_config ---

    #[test]
    fn channel_from_config_ipc_preferred() {
        let cfg = AeronConfig {
            media_driver_dir:      "/dev/shm/aeron".into(),
            ipc_enabled:           true,
            udp_endpoint:          Some("10.0.0.1:9999".into()),
            mtu:                   1408,
            term_length_mib:       64,
            archive_enabled:       false,
            connect_retry_delay_ms: 1000,
        };
        assert_eq!(channel_from_config(&cfg), "aeron:ipc");
    }

    #[test]
    fn channel_from_config_udp_when_ipc_disabled() {
        let cfg = AeronConfig {
            media_driver_dir:      "/dev/shm/aeron".into(),
            ipc_enabled:           false,
            udp_endpoint:          Some("10.0.0.2:40123".into()),
            mtu:                   1408,
            term_length_mib:       64,
            archive_enabled:       false,
            connect_retry_delay_ms: 1000,
        };
        assert_eq!(channel_from_config(&cfg), "aeron:udp?endpoint=10.0.0.2:40123");
    }

    // --- build_aeron_with_retry / reconnect_sync ---
    //
    // These tests run only without the `aeron` feature compiled in.  With the
    // feature enabled, both functions retry forever until a real media driver is
    // present; running them in unit tests would block indefinitely.

    #[cfg(not(feature = "aeron"))]
    fn no_driver_cfg() -> AeronConfig {
        AeronConfig {
            media_driver_dir:      "/tmp/aeron-nonexistent-test-dir".into(),
            ipc_enabled:           true,
            udp_endpoint:          None,
            mtu:                   1408,
            term_length_mib:       64,
            archive_enabled:       false,
            connect_retry_delay_ms: 1,
        }
    }

    #[cfg(not(feature = "aeron"))]
    #[tokio::test]
    async fn build_aeron_with_retry_returns_null_without_aeron_feature() {
        let cfg = no_driver_cfg();
        // Without the aeron feature, the function returns a null publisher immediately.
        let mut pub_ = build_aeron_with_retry(&cfg, &[0], Duration::from_millis(1)).await;
        assert!(matches!(pub_.offer(0, b"ping"), Ok(OfferResult::Ok(_))));
    }

    #[cfg(not(feature = "aeron"))]
    #[test]
    fn reconnect_sync_is_noop_without_aeron_feature() {
        let cfg = no_driver_cfg();
        let mut pub_ = build_null_boxed(&[0]);
        // Without the aeron feature, reconnect_sync is a no-op and the null
        // publisher keeps accepting offers.
        reconnect_sync(&cfg, 0, &mut pub_);
        assert!(matches!(pub_.offer(0, b"x"), Ok(OfferResult::Ok(_))));
    }
}
