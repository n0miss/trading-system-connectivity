use connector_core::{MarketType, VenueId};
use serde::Deserialize;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config load error: {0}")]
    Load(#[from] config::ConfigError),

    #[error("instance.total must be at least 1")]
    ZeroInstances,

    #[error("instance.id ({instance_id}) must be less than instance.total ({total_instances})")]
    InvalidInstanceId { instance_id: u32, total_instances: u32 },

    #[error("unknown venue \"{0}\" (expected: binance_spot | binance_futures)")]
    UnknownVenue(String),

    #[error("unknown market \"{0}\" (expected: spot | usdm_futures)")]
    UnknownMarket(String),

    #[error("sharding.total_logical_shards must be at least 1")]
    ZeroShards,

    #[error(
        "sharding.total_logical_shards ({shards}) must be >= instance.total ({instances})"
    )]
    InsufficientShards { shards: u32, instances: u32 },

    #[error("aeron.mtu {0} is out of range (576..=65535)")]
    InvalidMtu(u32),

    #[error("aeron.term_length_mib must be a power of two, got {0}")]
    InvalidTermLength(u64),
}

// ---------------------------------------------------------------------------
// Config structs  (all fields have serde defaults where reasonable)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct InstanceConfig {
    /// Zero-based index of this process.
    pub id:    u32,
    /// Total number of connector processes for this venue/market.
    pub total: u32,
    /// "binance_spot" | "binance_futures"
    pub venue: String,
    /// "spot" | "usdm_futures"
    pub market: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShardingConfig {
    /// Total number of logical shards. Fixed for a deployment generation.
    /// Recommended: 16 (small), 64 (medium), 128+ (large/multi-exchange).
    #[serde(default = "defaults::total_logical_shards")]
    pub total_logical_shards: u32,
}

/// The set of symbols this connector instance subscribes to.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct SymbolConfig {
    /// Empty list means auto-discover from exchange info at startup.
    #[serde(default)]
    pub universe: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AeronConfig {
    #[serde(default = "defaults::media_driver_dir")]
    pub media_driver_dir: String,

    #[serde(default = "defaults::ipc_enabled")]
    pub ipc_enabled: bool,

    /// UDP endpoint for cross-host consumers, e.g. "10.0.0.2:40123".
    pub udp_endpoint: Option<String>,

    /// UDP MTU in bytes. Must be in 576..=65535.
    #[serde(default = "defaults::mtu")]
    pub mtu: u32,

    /// Aeron term buffer size in MiB. Must be a power of two.
    #[serde(default = "defaults::term_length_mib")]
    pub term_length_mib: u64,

    #[serde(default)]
    pub archive_enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebSocketConfig {
    pub url: String,

    #[serde(default = "defaults::ping_interval_secs")]
    pub ping_interval_secs: u32,

    #[serde(default = "defaults::max_streams_per_connection")]
    pub max_streams_per_connection: u32,

    #[serde(default = "defaults::reconnect_delay_ms")]
    pub reconnect_delay_ms: u64,

    /// Binance mandates a reconnect every 24 h. This is the hard ceiling.
    #[serde(default = "defaults::forced_reconnect_secs")]
    pub forced_reconnect_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RestConfig {
    pub base_url: String,

    #[serde(default = "defaults::timeout_ms")]
    pub timeout_ms: u64,

    #[serde(default = "defaults::max_retries")]
    pub max_retries: u32,
}

/// Per-symbol recovery buffer limits (addendum §1.3).
#[derive(Debug, Clone, Deserialize)]
pub struct RecoveryConfig {
    #[serde(default = "defaults::max_buffered_events")]
    pub max_buffered_events: u32,

    #[serde(default = "defaults::max_buffered_bytes")]
    pub max_buffered_bytes: u64,

    #[serde(default = "defaults::max_buffer_age_secs")]
    pub max_buffer_age_secs: u32,

    #[serde(default = "defaults::max_recovery_attempts")]
    pub max_recovery_attempts: u32,

    #[serde(default = "defaults::circuit_break_cooldown_secs")]
    pub circuit_break_cooldown_secs: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "defaults::prometheus_port")]
    pub prometheus_port: u16,
}

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorConfig {
    pub instance:  InstanceConfig,
    pub sharding:  ShardingConfig,
    #[serde(default)]
    pub symbols:   SymbolConfig,
    pub aeron:     AeronConfig,
    pub websocket: WebSocketConfig,
    pub rest:      RestConfig,
    pub recovery:  RecoveryConfig,
    pub metrics:   MetricsConfig,
}

impl ConnectorConfig {
    /// Load from a TOML file, then overlay `CONNECTOR__*` environment variables.
    ///
    /// Environment variable mapping: `CONNECTOR__INSTANCE__ID=1` overrides `instance.id`.
    /// Multiple symbols via env: `CONNECTOR__SYMBOLS__UNIVERSE=BTCUSDT,ETHUSDT`.
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let raw = config::Config::builder()
            .add_source(config::File::with_name(path))
            .add_source(
                config::Environment::with_prefix("CONNECTOR")
                    .separator("__")
                    .list_separator(",")
                    .try_parsing(true),
            )
            .build()?;
        let cfg: Self = raw.try_deserialize()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Resolve the venue to the core `VenueId` type.
    pub fn venue_id(&self) -> Result<VenueId, ConfigError> {
        match self.instance.venue.as_str() {
            "binance_spot"    => Ok(VenueId::BinanceSpot),
            "binance_futures" => Ok(VenueId::BinanceFutures),
            other             => Err(ConfigError::UnknownVenue(other.to_owned())),
        }
    }

    /// Resolve the market to the core `MarketType` type.
    pub fn market_type(&self) -> Result<MarketType, ConfigError> {
        match self.instance.market.as_str() {
            "spot"         => Ok(MarketType::Spot),
            "usdm_futures" => Ok(MarketType::UsdmFutures),
            other          => Err(ConfigError::UnknownMarket(other.to_owned())),
        }
    }

    /// Returns the logical shard ids owned by this instance.
    ///
    /// Assignment rule: `shard_id % total_instances == instance_id`.
    pub fn owned_shards(&self) -> Vec<u32> {
        let total = self.sharding.total_logical_shards;
        let id    = self.instance.id;
        let n     = self.instance.total;
        (0..total).filter(|&s| s % n == id).collect()
    }

    /// Validate all fields. Called automatically by `load`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.instance.total == 0 {
            return Err(ConfigError::ZeroInstances);
        }
        if self.instance.id >= self.instance.total {
            return Err(ConfigError::InvalidInstanceId {
                instance_id:     self.instance.id,
                total_instances: self.instance.total,
            });
        }
        // parse venue/market to validate the strings now rather than at runtime
        self.venue_id()?;
        self.market_type()?;

        if self.sharding.total_logical_shards == 0 {
            return Err(ConfigError::ZeroShards);
        }
        if self.sharding.total_logical_shards < self.instance.total {
            return Err(ConfigError::InsufficientShards {
                shards:    self.sharding.total_logical_shards,
                instances: self.instance.total,
            });
        }
        if !(576..=65_535).contains(&self.aeron.mtu) {
            return Err(ConfigError::InvalidMtu(self.aeron.mtu));
        }
        if self.aeron.term_length_mib == 0 || !self.aeron.term_length_mib.is_power_of_two() {
            return Err(ConfigError::InvalidTermLength(self.aeron.term_length_mib));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Serde default helpers
// ---------------------------------------------------------------------------

mod defaults {
    pub fn total_logical_shards() -> u32  { 16 }
    pub fn media_driver_dir()     -> String { "/dev/shm/aeron".to_owned() }
    pub fn ipc_enabled()          -> bool  { true }
    pub fn mtu()                  -> u32   { 1408 }
    pub fn term_length_mib()      -> u64   { 64 }
    pub fn ping_interval_secs()   -> u32   { 20 }
    pub fn max_streams_per_connection() -> u32  { 1024 }
    pub fn reconnect_delay_ms()   -> u64   { 500 }
    pub fn forced_reconnect_secs() -> u64  { 86_400 }
    pub fn timeout_ms()           -> u64   { 5_000 }
    pub fn max_retries()          -> u32   { 3 }
    pub fn max_buffered_events()  -> u32   { 2_048 }
    pub fn max_buffered_bytes()   -> u64   { 4 * 1024 * 1024 }  // 4 MiB
    pub fn max_buffer_age_secs()  -> u32   { 10 }
    pub fn max_recovery_attempts() -> u32  { 5 }
    pub fn circuit_break_cooldown_secs() -> u32 { 30 }
    pub fn prometheus_port()      -> u16   { 9090 }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use connector_core::{MarketType, VenueId};

    // Build a ConnectorConfig from a TOML string (no env overlay, no validate).
    fn parse(toml: &str) -> ConnectorConfig {
        config::Config::builder()
            .add_source(config::File::from_str(toml, config::FileFormat::Toml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap()
    }

    // Parse + validate.
    fn load(toml: &str) -> Result<ConnectorConfig, ConfigError> {
        let cfg = parse(toml);
        cfg.validate()?;
        Ok(cfg)
    }

    const VALID_TOML: &str = r#"
[instance]
id     = 0
total  = 2
venue  = "binance_spot"
market = "spot"

[sharding]
total_logical_shards = 16

[symbols]
universe = ["BTCUSDT", "ETHUSDT"]

[aeron]
media_driver_dir = "/dev/shm/aeron"
ipc_enabled      = true
mtu              = 1408
term_length_mib  = 64
archive_enabled  = false

[websocket]
url = "wss://stream.binance.com:9443/stream"

[rest]
base_url = "https://api.binance.com"

[recovery]

[metrics]
"#;

    #[test]
    fn valid_config_loads() {
        let cfg = load(VALID_TOML).unwrap();
        assert_eq!(cfg.instance.id,    0);
        assert_eq!(cfg.instance.total, 2);
        assert_eq!(cfg.symbols.universe, vec!["BTCUSDT", "ETHUSDT"]);
        assert_eq!(cfg.sharding.total_logical_shards, 16);
        assert_eq!(cfg.aeron.mtu, 1408);
        assert_eq!(cfg.aeron.term_length_mib, 64);
        assert_eq!(cfg.recovery.max_buffered_events, 2_048);
        assert_eq!(cfg.recovery.max_buffered_bytes,  4 * 1024 * 1024);
        assert_eq!(cfg.recovery.max_recovery_attempts, 5);
        assert_eq!(cfg.recovery.circuit_break_cooldown_secs, 30);
        assert_eq!(cfg.metrics.prometheus_port, 9090);
    }

    #[test]
    fn venue_and_market_type_resolve() {
        let cfg = load(VALID_TOML).unwrap();
        assert_eq!(cfg.venue_id().unwrap(),    VenueId::BinanceSpot);
        assert_eq!(cfg.market_type().unwrap(), MarketType::Spot);
    }

    #[test]
    fn futures_venue_and_market_resolve() {
        let toml = VALID_TOML
            .replace("binance_spot", "binance_futures")
            .replace(r#"market = "spot""#, r#"market = "usdm_futures""#)
            .replace(
                "wss://stream.binance.com:9443/stream",
                "wss://fstream.binance.com/stream",
            )
            .replace("https://api.binance.com", "https://fapi.binance.com");
        let cfg = load(&toml).unwrap();
        assert_eq!(cfg.venue_id().unwrap(),    VenueId::BinanceFutures);
        assert_eq!(cfg.market_type().unwrap(), MarketType::UsdmFutures);
    }

    #[test]
    fn shard_assignment_two_instances() {
        let cfg = load(VALID_TOML).unwrap();  // instance 0 of 2, 16 shards
        let shards = cfg.owned_shards();
        // instance 0 gets shards 0, 2, 4, 6, 8, 10, 12, 14
        assert_eq!(shards, vec![0, 2, 4, 6, 8, 10, 12, 14]);

        let toml1 = VALID_TOML.replace("id     = 0", "id     = 1");
        let cfg1  = load(&toml1).unwrap();
        assert_eq!(cfg1.owned_shards(), vec![1, 3, 5, 7, 9, 11, 13, 15]);
    }

    #[test]
    fn shard_assignment_covers_all_shards() {
        let cfg   = load(VALID_TOML).unwrap();
        let toml1 = VALID_TOML.replace("id     = 0", "id     = 1");
        let cfg1  = load(&toml1).unwrap();
        let mut all: Vec<u32> = cfg.owned_shards().into_iter().chain(cfg1.owned_shards()).collect();
        all.sort_unstable();
        let expected: Vec<u32> = (0..16).collect();
        assert_eq!(all, expected);
    }

    #[test]
    fn single_instance_owns_all_shards() {
        let toml = VALID_TOML.replace("total  = 2", "total  = 1");
        let cfg  = load(&toml).unwrap();
        let expected: Vec<u32> = (0..16).collect();
        assert_eq!(cfg.owned_shards(), expected);
    }

    #[test]
    fn validate_zero_instances() {
        let toml = VALID_TOML.replace("total  = 2", "total  = 0");
        let err  = load(&toml).unwrap_err();
        assert!(matches!(err, ConfigError::ZeroInstances));
    }

    #[test]
    fn validate_instance_id_gte_total() {
        let toml = VALID_TOML.replace("id     = 0", "id     = 2");
        let err  = load(&toml).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidInstanceId { .. }));
    }

    #[test]
    fn validate_unknown_venue() {
        let toml = VALID_TOML.replace("binance_spot", "kraken");
        let err  = load(&toml).unwrap_err();
        assert!(matches!(err, ConfigError::UnknownVenue(_)));
    }

    #[test]
    fn validate_unknown_market() {
        let toml = VALID_TOML.replace(r#"market = "spot""#, r#"market = "coinm_futures""#);
        let err  = load(&toml).unwrap_err();
        assert!(matches!(err, ConfigError::UnknownMarket(_)));
    }

    #[test]
    fn validate_zero_shards() {
        let toml = VALID_TOML.replace("total_logical_shards = 16", "total_logical_shards = 0");
        let err  = load(&toml).unwrap_err();
        assert!(matches!(err, ConfigError::ZeroShards));
    }

    #[test]
    fn validate_shards_less_than_instances() {
        let toml = VALID_TOML.replace("total_logical_shards = 16", "total_logical_shards = 1");
        // instance.total = 2, shards = 1 → InsufficientShards
        let err = load(&toml).unwrap_err();
        assert!(matches!(err, ConfigError::InsufficientShards { .. }));
    }

    #[test]
    fn validate_mtu_too_low() {
        let toml = VALID_TOML.replace("mtu              = 1408", "mtu = 100");
        let err  = load(&toml).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidMtu(100)));
    }

    #[test]
    fn validate_mtu_too_high() {
        let toml = VALID_TOML.replace("mtu              = 1408", "mtu = 70000");
        let err  = load(&toml).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidMtu(70000)));
    }

    #[test]
    fn validate_term_length_not_power_of_two() {
        let toml = VALID_TOML.replace("term_length_mib  = 64", "term_length_mib = 63");
        let err  = load(&toml).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidTermLength(63)));
    }

    #[test]
    fn validate_term_length_zero() {
        let toml = VALID_TOML.replace("term_length_mib  = 64", "term_length_mib = 0");
        let err  = load(&toml).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidTermLength(0)));
    }

    #[test]
    fn defaults_are_applied() {
        // Omit all optional fields — serde defaults kick in.
        let toml = r#"
[instance]
id     = 0
total  = 1
venue  = "binance_spot"
market = "spot"

[sharding]

[aeron]

[websocket]
url = "wss://stream.binance.com:9443/stream"

[rest]
base_url = "https://api.binance.com"

[recovery]

[metrics]
"#;
        let cfg = load(toml).unwrap();
        assert_eq!(cfg.sharding.total_logical_shards, 16);
        assert_eq!(cfg.aeron.mtu, 1408);
        assert_eq!(cfg.aeron.term_length_mib, 64);
        assert_eq!(cfg.aeron.media_driver_dir, "/dev/shm/aeron");
        assert!(cfg.aeron.ipc_enabled);
        assert_eq!(cfg.websocket.ping_interval_secs, 20);
        assert_eq!(cfg.websocket.forced_reconnect_secs, 86_400);
        assert_eq!(cfg.rest.timeout_ms, 5_000);
        assert_eq!(cfg.rest.max_retries, 3);
        assert_eq!(cfg.recovery.max_buffered_events, 2_048);
        assert_eq!(cfg.metrics.prometheus_port, 9090);
        assert!(cfg.symbols.universe.is_empty());
    }

    #[test]
    fn empty_symbol_universe_is_valid() {
        let toml = VALID_TOML.replace(r#"universe = ["BTCUSDT", "ETHUSDT"]"#, r#"universe = []"#);
        load(&toml).unwrap();
    }
}
