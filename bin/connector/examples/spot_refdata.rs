/// Verifies Stage 1.5: fetches Binance Spot exchangeInfo, normalizes it,
/// and prints a summary.  Requires a network connection; no Aeron needed.
///
///   cargo run --example spot_refdata
use connector_core::{MarketType, VenueId};
use connector_refdata::RestClient;

#[tokio::main]
async fn main() {
    let client = RestClient::new("https://api.binance.com");

    println!("Fetching Binance Spot exchange info …");
    let defs = client
        .fetch_exchange_info(VenueId::BinanceSpot, MarketType::Spot, 0, 0)
        .await
        .expect("REST call failed");

    println!("Received {} instruments\n", defs.len());

    // Print first 10 with key fields
    for d in defs.iter().take(10) {
        println!(
            "{:<12}  trading={:<5}  price_scale={}  qty_scale={}  tick={:>12}  step={:>12}",
            d.symbol, d.is_trading, d.price_scale, d.qty_scale, d.tick_size, d.step_size,
        );
    }

    // Binary round-trip check on the first instrument
    let first = &defs[0];
    let mut buf = vec![0u8; 4096];
    let n = first.encode_into(&mut buf).expect("encode failed");
    let decoded = connector_core::InstrumentDefinition::decode(&buf[..n])
        .expect("decode failed");
    assert_eq!(first.symbol,      decoded.symbol);
    assert_eq!(first.price_scale, decoded.price_scale);
    assert_eq!(first.tick_size,   decoded.tick_size);
    println!("\nBinary round-trip OK for {}", first.symbol);
}
