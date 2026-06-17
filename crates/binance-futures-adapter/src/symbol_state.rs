/// Per-symbol protocol state bundle for Binance USDT-M Futures (§5.25).
///
/// Groups every stateful component belonging to one symbol: the order book,
/// sequence validator, recovery buffer, circuit breaker, and current feed state.
/// [`FuturesShardEngine`] owns a `HashMap<String, FuturesSymbolState>`.
///
/// No `BboValidator` — Futures does not cross-check the book BBO against
/// `bookTicker` the way Spot does.
///
/// [`FuturesShardEngine`]: crate::shard_engine::FuturesShardEngine

use connector_core::{FeedState, InstrumentDefinition};
use connector_order_book::OrderBook;

use crate::circuit_breaker::CircuitBreaker;
use crate::recovery_buffer::RecoveryBuffer;
use crate::sequence::FuturesSequenceValidator;

pub struct FuturesSymbolState {
    pub inst:         InstrumentDefinition,
    pub book:         OrderBook,
    pub validator:    FuturesSequenceValidator,
    pub recovery_buf: RecoveryBuffer,
    pub circuit:      CircuitBreaker,
    pub feed_state:   FeedState,
}

impl FuturesSymbolState {
    pub fn new(inst: InstrumentDefinition) -> Self {
        let book = OrderBook::new(&inst.symbol);
        Self {
            book,
            validator:    FuturesSequenceValidator::new(),
            recovery_buf: RecoveryBuffer::new(),
            circuit:      CircuitBreaker::new(),
            feed_state:   FeedState::Connecting,
            inst,
        }
    }

    pub fn symbol(&self)   -> &str  { &self.inst.symbol }
    pub fn is_stale(&self) -> bool  { self.book.is_stale() }
}
