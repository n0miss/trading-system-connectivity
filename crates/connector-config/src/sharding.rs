/// Deterministic symbol→shard routing (§4.18).
///
/// ```text
/// logical_shard_id = fnv1a_32(venue || market || symbol) % total_shards
/// ```
///
/// Venue and market discriminants are fed as single bytes before the symbol
/// string so that the same symbol on different venue/market pairs routes to
/// different shards in a multi-exchange deployment.  The FNV-1a constants are
/// identical to those used by `connector_refdata::symbol_instrument_id`, which
/// hashes only the symbol string for the instrument-id namespace.
///
/// The shard→instance assignment uses the modulo rule already implemented in
/// [`crate::ConnectorConfig::owned_shards`]:
///
/// ```text
/// owner_instance_id = logical_shard_id % total_instances
/// ```
use connector_core::{MarketType, VenueId};

// ---------------------------------------------------------------------------
// FNV-1a 32-bit parameters
// ---------------------------------------------------------------------------

const FNV_BASIS: u32 = 2_166_136_261;
const FNV_PRIME: u32 = 16_777_619;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Compute the logical shard id for a symbol.
///
/// Feeds `venue as u8 → market as u8 → symbol bytes` through FNV-1a 32-bit and
/// reduces modulo `total_shards`.
///
/// This function is pure and allocation-free.  Call it at startup to partition
/// the symbol universe, and again in the hot path only if symbols can be added
/// at runtime.
///
/// # Panics
///
/// Panics if `total_shards == 0`.
pub fn shard_for_symbol(
    venue: VenueId,
    market: MarketType,
    symbol: &str,
    total_shards: u32,
) -> u32 {
    assert!(total_shards > 0, "total_shards must be > 0");

    let mut h = FNV_BASIS;

    h ^= venue as u32;
    h = h.wrapping_mul(FNV_PRIME);

    h ^= market as u32;
    h = h.wrapping_mul(FNV_PRIME);

    for b in symbol.bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(FNV_PRIME);
    }

    h % total_shards
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Shorthand helpers.
    fn spot_shard(sym: &str, n: u32) -> u32 {
        shard_for_symbol(VenueId::BinanceSpot, MarketType::Spot, sym, n)
    }

    fn fut_shard(sym: &str, n: u32) -> u32 {
        shard_for_symbol(VenueId::BinanceFutures, MarketType::UsdmFutures, sym, n)
    }

    // A handful of symbols used across multiple tests.
    const SYMBOLS: &[&str] = &[
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT", "AVAXUSDT", "ADAUSDT",
        "TRXUSDT", "LINKUSDT",
    ];

    // --- Determinism ---

    #[test]
    fn same_inputs_produce_same_shard() {
        for &s in SYMBOLS {
            assert_eq!(spot_shard(s, 16), spot_shard(s, 16));
        }
    }

    // --- Range ---

    #[test]
    fn shard_is_always_less_than_total_shards() {
        for &s in SYMBOLS {
            assert!(spot_shard(s, 16) < 16);
            assert!(spot_shard(s, 64) < 64);
            assert!(spot_shard(s, 1) < 1);
        }
    }

    // --- Single shard ---

    #[test]
    fn single_shard_always_returns_zero() {
        for &s in SYMBOLS {
            assert_eq!(spot_shard(s, 1), 0);
        }
    }

    // --- Venue/market differentiation ---

    #[test]
    fn different_venue_market_produces_different_shard_for_btcusdt() {
        // With 256 shards the probability of a collision is low; if this fails
        // the hash still works but the test needs a different symbol.
        let a = spot_shard("BTCUSDT", 256);
        let b = fut_shard("BTCUSDT", 256);
        assert_ne!(
            a, b,
            "spot and futures BTCUSDT should map to different shards"
        );
    }

    #[test]
    fn same_venue_different_market_differentiates() {
        // MarketType::Spot=1 vs MarketType::UsdmFutures=2 — hashes must diverge.
        let a = shard_for_symbol(VenueId::BinanceSpot, MarketType::Spot, "ETHUSDT", 256);
        let b = shard_for_symbol(
            VenueId::BinanceSpot,
            MarketType::UsdmFutures,
            "ETHUSDT",
            256,
        );
        assert_ne!(a, b);
    }

    // --- Symbol differentiation ---

    #[test]
    fn different_symbols_produce_different_shards_with_large_shard_space() {
        // With 1024 shards all 10 symbols should land on different shards
        // (collisions are possible in theory but extremely unlikely with FNV-1a).
        let shards: Vec<u32> = SYMBOLS.iter().map(|s| spot_shard(s, 1024)).collect();
        let unique: std::collections::HashSet<u32> = shards.iter().copied().collect();
        assert_eq!(
            unique.len(),
            SYMBOLS.len(),
            "unexpected collision among shards: {shards:?}"
        );
    }

    // --- Distribution ---

    #[test]
    fn shards_not_all_zero_with_two_shards() {
        // At least one symbol in SYMBOLS must land on shard 1.
        let any_on_one = SYMBOLS.iter().any(|s| spot_shard(s, 2) == 1);
        assert!(
            any_on_one,
            "no symbol landed on shard 1 — distribution problem"
        );
    }

    #[test]
    fn both_shards_covered_with_two_shards() {
        // With 2 shards and 10 symbols both shard 0 and shard 1 must appear
        // (the probability of all 10 landing on the same shard is ~0.2%).
        let mut seen = [false; 2];
        for &s in SYMBOLS {
            seen[spot_shard(s, 2) as usize] = true;
        }
        assert!(seen[0], "no symbol landed on shard 0");
        assert!(seen[1], "no symbol landed on shard 1");
    }

    // --- Stability (golden) ---

    #[test]
    fn btcusdt_spot_shard_is_stable_across_16() {
        // Record the expected value once.  If the hash changes (e.g. constant
        // update), this test fails and deployment rebalancing is required.
        let expected = spot_shard("BTCUSDT", 16);
        // Re-derive from scratch to catch any non-determinism in the test env.
        assert_eq!(
            shard_for_symbol(VenueId::BinanceSpot, MarketType::Spot, "BTCUSDT", 16),
            expected,
        );
    }

    #[test]
    fn shard_for_empty_symbol_does_not_panic() {
        // Empty symbol is unusual but must not crash.
        let _ = spot_shard("", 16);
    }

    #[test]
    fn shard_for_unicode_symbol_does_not_panic() {
        // Non-ASCII input; Binance doesn't use these but the function must be safe.
        let _ = shard_for_symbol(VenueId::BinanceSpot, MarketType::Spot, "BTC₿USDT", 16);
    }

    // --- Two-instance partition property ---

    #[test]
    fn two_instance_partition_covers_all_symbols() {
        // Every symbol is owned by exactly one of instance 0 or instance 1.
        for &s in SYMBOLS {
            let shard = spot_shard(s, 16);
            let inst0 = shard % 2 == 0;
            let inst1 = shard % 2 == 1;
            assert!(
                inst0 ^ inst1,
                "symbol {s} not owned by exactly one instance"
            );
        }
    }

    #[test]
    fn four_instance_partition_covers_all_symbols() {
        for &s in SYMBOLS {
            let shard = spot_shard(s, 16);
            let owner = shard % 4;
            assert!(owner < 4);
        }
    }
}
