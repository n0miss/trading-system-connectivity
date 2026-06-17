/// Per-symbol protocol state bundle (§4.19).
///
/// Groups every stateful component that belongs to one symbol:
/// the order book, sequence validator, recovery buffer, circuit
/// breaker, BBO validator, and current feed state.  `ShardEngine`
/// owns a `HashMap<String, SymbolState>`.

use connector_core::{FeedState, InstrumentDefinition};
use connector_order_book::OrderBook;

use crate::{BboValidator, CircuitBreaker, RecoveryBuffer, SequenceValidator};

pub struct SymbolState {
    pub inst:          InstrumentDefinition,
    pub book:          OrderBook,
    pub validator:     SequenceValidator,
    pub recovery_buf:  RecoveryBuffer,
    pub circuit:       CircuitBreaker,
    pub bbo_validator: BboValidator,
    pub feed_state:    FeedState,
}

impl SymbolState {
    pub fn new(inst: InstrumentDefinition) -> Self {
        let book = OrderBook::new(&inst.symbol);
        Self {
            book,
            validator:     SequenceValidator::new(),
            recovery_buf:  RecoveryBuffer::new(),
            circuit:       CircuitBreaker::new(),
            bbo_validator: BboValidator::new(),
            feed_state:    FeedState::Connecting,
            inst,
        }
    }

    pub fn symbol(&self) -> &str { &self.inst.symbol }
    pub fn is_stale(&self) -> bool { self.book.is_stale() }
}
