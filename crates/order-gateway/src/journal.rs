//! Persistent order journal — append-only write-ahead log.
//!
//! Wire format (per record):
//!
//! ```text
//! Offset  Len  Field
//! 0       1    entry_tag (u8, EntryTag repr)
//! 1       8    timestamp_ns (i64 LE)
//! 9       2    payload_len  (u16 LE)
//! 11      N    payload bytes
//! ```
//!
//! Minimum record size: 11 bytes.
//!
//! The journal is opened with [`Journal::open`] (file-backed, recovers existing
//! entries) or [`Journal::in_memory`] (tests).  All operations are synchronous.

use crate::{ClientOrderId, Error, OrderRequest, OrderSide, OrderType, TimeInForce};
use std::io::{BufWriter, Write};

// ---------------------------------------------------------------------------
// Entry tag
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum EntryTag {
    OrderRequested = 1,
    OrderAcknowledged = 2,
    OrderFilled = 3,
    OrderCancelled = 4,
    OrderRejected = 5,
    OrderExpired = 6,
}

impl TryFrom<u8> for EntryTag {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, u8> {
        match v {
            1 => Ok(Self::OrderRequested),
            2 => Ok(Self::OrderAcknowledged),
            3 => Ok(Self::OrderFilled),
            4 => Ok(Self::OrderCancelled),
            5 => Ok(Self::OrderRejected),
            6 => Ok(Self::OrderExpired),
            x => Err(x),
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry type
// ---------------------------------------------------------------------------

/// One logical event persisted in the journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalEntry {
    OrderRequested {
        timestamp_ns: i64,
        cloid: ClientOrderId,
        request: OrderRequest,
    },
    OrderAcknowledged {
        timestamp_ns: i64,
        cloid: ClientOrderId,
        exchange_id: u64,
    },
    OrderFilled {
        timestamp_ns: i64,
        cloid: ClientOrderId,
        fill_qty: i64,
        fill_price: i64,
        /// `true` when this fill completes the order (qty fully matched).
        is_final: bool,
    },
    OrderCancelled {
        timestamp_ns: i64,
        cloid: ClientOrderId,
    },
    OrderRejected {
        timestamp_ns: i64,
        cloid: ClientOrderId,
        reason: String,
    },
    OrderExpired {
        timestamp_ns: i64,
        cloid: ClientOrderId,
    },
}

impl JournalEntry {
    pub fn timestamp_ns(&self) -> i64 {
        match self {
            Self::OrderRequested { timestamp_ns, .. } => *timestamp_ns,
            Self::OrderAcknowledged { timestamp_ns, .. } => *timestamp_ns,
            Self::OrderFilled { timestamp_ns, .. } => *timestamp_ns,
            Self::OrderCancelled { timestamp_ns, .. } => *timestamp_ns,
            Self::OrderRejected { timestamp_ns, .. } => *timestamp_ns,
            Self::OrderExpired { timestamp_ns, .. } => *timestamp_ns,
        }
    }

    pub fn cloid(&self) -> &ClientOrderId {
        match self {
            Self::OrderRequested { cloid, .. } => cloid,
            Self::OrderAcknowledged { cloid, .. } => cloid,
            Self::OrderFilled { cloid, .. } => cloid,
            Self::OrderCancelled { cloid, .. } => cloid,
            Self::OrderRejected { cloid, .. } => cloid,
            Self::OrderExpired { cloid, .. } => cloid,
        }
    }
}

// ---------------------------------------------------------------------------
// Encoder / decoder helpers
// ---------------------------------------------------------------------------

struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(64),
        }
    }

    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    fn i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn bool(&mut self, v: bool) {
        self.u8(v as u8);
    }

    /// Encode a short string (len fits in u8 — max 255 bytes).
    fn str_u8(&mut self, s: &str) {
        let b = s.as_bytes();
        debug_assert!(b.len() <= 255, "string too long for u8 prefix: {}", s.len());
        self.u8(b.len() as u8);
        self.buf.extend_from_slice(b);
    }

    /// Encode a longer string (len fits in u16 — max 65535 bytes).
    fn str_u16(&mut self, s: &str) {
        let b = s.as_bytes();
        self.u16(b.len() as u16);
        self.buf.extend_from_slice(b);
    }

    fn finish(self) -> Vec<u8> {
        self.buf
    }
}

struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn need(&self, n: usize) -> Result<(), Error> {
        if self.remaining() < n {
            Err(Error::BufferTooShort {
                needed: self.pos + n,
                have: self.buf.len(),
            })
        } else {
            Ok(())
        }
    }

    fn u8(&mut self) -> Result<u8, Error> {
        self.need(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn i64(&mut self) -> Result<i64, Error> {
        self.need(8)?;
        let v = i64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    fn u64(&mut self) -> Result<u64, Error> {
        self.need(8)?;
        let v = u64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    fn u16(&mut self) -> Result<u16, Error> {
        self.need(2)?;
        let v = u16::from_le_bytes(self.buf[self.pos..self.pos + 2].try_into().unwrap());
        self.pos += 2;
        Ok(v)
    }

    fn bool(&mut self) -> Result<bool, Error> {
        Ok(self.u8()? != 0)
    }

    fn str_u8(&mut self) -> Result<String, Error> {
        let len = self.u8()? as usize;
        self.need(len)?;
        let s = std::str::from_utf8(&self.buf[self.pos..self.pos + len]).map_err(|e| {
            Error::JournalCorrupt {
                offset: self.pos,
                reason: e.to_string(),
            }
        })?;
        self.pos += len;
        Ok(s.to_string())
    }

    fn str_u16(&mut self) -> Result<String, Error> {
        let len = self.u16()? as usize;
        self.need(len)?;
        let s = std::str::from_utf8(&self.buf[self.pos..self.pos + len]).map_err(|e| {
            Error::JournalCorrupt {
                offset: self.pos,
                reason: e.to_string(),
            }
        })?;
        self.pos += len;
        Ok(s.to_string())
    }

    fn cloid(&mut self) -> Result<ClientOrderId, Error> {
        let s = self.str_u8()?;
        Ok(ClientOrderId::new_raw(s))
    }
}

// ---------------------------------------------------------------------------
// Entry encode / decode
// ---------------------------------------------------------------------------

fn encode_entry(entry: &JournalEntry) -> Vec<u8> {
    let (tag, payload) = match entry {
        JournalEntry::OrderRequested {
            timestamp_ns: _,
            cloid,
            request,
        } => {
            let mut enc = Encoder::new();
            enc.str_u8(cloid.as_str());
            enc.str_u8(&request.symbol);
            enc.u8(request.side as u8);
            enc.u8(request.order_type as u8);
            enc.u8(request.time_in_force as u8);
            enc.i64(request.qty);
            match request.limit_price {
                None => enc.u8(0),
                Some(p) => {
                    enc.u8(1);
                    enc.i64(p);
                }
            }
            (EntryTag::OrderRequested, enc.finish())
        }

        JournalEntry::OrderAcknowledged {
            timestamp_ns: _,
            cloid,
            exchange_id,
        } => {
            let mut enc = Encoder::new();
            enc.str_u8(cloid.as_str());
            enc.u64(*exchange_id);
            (EntryTag::OrderAcknowledged, enc.finish())
        }

        JournalEntry::OrderFilled {
            timestamp_ns: _,
            cloid,
            fill_qty,
            fill_price,
            is_final,
        } => {
            let mut enc = Encoder::new();
            enc.str_u8(cloid.as_str());
            enc.i64(*fill_qty);
            enc.i64(*fill_price);
            enc.bool(*is_final);
            (EntryTag::OrderFilled, enc.finish())
        }

        JournalEntry::OrderCancelled {
            timestamp_ns: _,
            cloid,
        } => {
            let mut enc = Encoder::new();
            enc.str_u8(cloid.as_str());
            (EntryTag::OrderCancelled, enc.finish())
        }

        JournalEntry::OrderRejected {
            timestamp_ns: _,
            cloid,
            reason,
        } => {
            let mut enc = Encoder::new();
            enc.str_u8(cloid.as_str());
            enc.str_u16(reason);
            (EntryTag::OrderRejected, enc.finish())
        }

        JournalEntry::OrderExpired {
            timestamp_ns: _,
            cloid,
        } => {
            let mut enc = Encoder::new();
            enc.str_u8(cloid.as_str());
            (EntryTag::OrderExpired, enc.finish())
        }
    };

    let ts = entry.timestamp_ns();
    let payload_len = payload.len() as u16;

    let mut out = Vec::with_capacity(11 + payload.len());
    out.push(tag as u8);
    out.extend_from_slice(&ts.to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

fn decode_one(buf: &[u8], offset: usize) -> Result<(JournalEntry, usize), Error> {
    let mut dec = Decoder { buf, pos: offset };

    let tag_byte = dec.u8()?;
    let timestamp_ns = dec.i64()?;
    let payload_len = dec.u16()? as usize;

    // Snapshot the start of the payload and advance past it.
    let payload_start = dec.pos;
    dec.need(payload_len).map_err(|_| Error::JournalCorrupt {
        offset,
        reason: format!("payload_len={payload_len} extends past buffer"),
    })?;
    let payload_end = payload_start + payload_len;
    let payload = &buf[payload_start..payload_end];
    let consumed = payload_end;

    let tag = EntryTag::try_from(tag_byte).map_err(|v| Error::JournalCorrupt {
        offset,
        reason: format!("unknown entry tag {v}"),
    })?;

    let mut pdec = Decoder::new(payload);

    let entry = match tag {
        EntryTag::OrderRequested => {
            let cloid = pdec.cloid()?;
            let symbol = pdec.str_u8()?;
            let side = OrderSide::try_from(pdec.u8()?).map_err(|v| Error::JournalCorrupt {
                offset,
                reason: format!("bad side {v}"),
            })?;
            let order_type =
                OrderType::try_from(pdec.u8()?).map_err(|v| Error::JournalCorrupt {
                    offset,
                    reason: format!("bad order_type {v}"),
                })?;
            let tif = TimeInForce::try_from(pdec.u8()?).map_err(|v| Error::JournalCorrupt {
                offset,
                reason: format!("bad tif {v}"),
            })?;
            let qty = pdec.i64()?;
            let limit_price = if pdec.u8()? != 0 {
                Some(pdec.i64()?)
            } else {
                None
            };
            JournalEntry::OrderRequested {
                timestamp_ns,
                cloid,
                request: OrderRequest {
                    symbol,
                    side,
                    order_type,
                    qty,
                    limit_price,
                    time_in_force: tif,
                },
            }
        }

        EntryTag::OrderAcknowledged => {
            let cloid = pdec.cloid()?;
            let exchange_id = pdec.u64()?;
            JournalEntry::OrderAcknowledged {
                timestamp_ns,
                cloid,
                exchange_id,
            }
        }

        EntryTag::OrderFilled => {
            let cloid = pdec.cloid()?;
            let fill_qty = pdec.i64()?;
            let fill_price = pdec.i64()?;
            let is_final = pdec.bool()?;
            JournalEntry::OrderFilled {
                timestamp_ns,
                cloid,
                fill_qty,
                fill_price,
                is_final,
            }
        }

        EntryTag::OrderCancelled => {
            let cloid = pdec.cloid()?;
            JournalEntry::OrderCancelled {
                timestamp_ns,
                cloid,
            }
        }

        EntryTag::OrderRejected => {
            let cloid = pdec.cloid()?;
            let reason = pdec.str_u16()?;
            JournalEntry::OrderRejected {
                timestamp_ns,
                cloid,
                reason,
            }
        }

        EntryTag::OrderExpired => {
            let cloid = pdec.cloid()?;
            JournalEntry::OrderExpired {
                timestamp_ns,
                cloid,
            }
        }
    };

    Ok((entry, consumed))
}

/// Decode all journal entries from a contiguous byte slice (e.g. a file read).
pub fn decode_all(buf: &[u8]) -> Result<Vec<JournalEntry>, Error> {
    let mut entries = Vec::new();
    let mut pos = 0;
    while pos < buf.len() {
        let (entry, next) = decode_one(buf, pos)?;
        entries.push(entry);
        pos = next;
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------
// Journal
// ---------------------------------------------------------------------------

enum JournalWriter {
    File(BufWriter<std::fs::File>),
    Memory(Vec<u8>),
}

/// Append-only write-ahead log for order lifecycle events.
///
/// Every call to [`append`][Self::append] is synchronous and `flush`es
/// automatically (file backend) or accumulates in memory (test backend).
pub struct Journal {
    writer: JournalWriter,
}

impl Journal {
    /// Open a file-backed journal at `path`.
    ///
    /// If the file already exists all existing records are decoded and returned
    /// as the `recovered` list; the writer is then positioned for appending.
    /// The file is created if it does not yet exist.
    pub fn open(path: &std::path::Path) -> Result<(Vec<JournalEntry>, Self), Error> {
        let recovered = if path.exists() {
            let bytes = std::fs::read(path)?;
            decode_all(&bytes)?
        } else {
            Vec::new()
        };

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok((
            recovered,
            Self {
                writer: JournalWriter::File(BufWriter::new(file)),
            },
        ))
    }

    /// Create an in-memory journal (no persistence).  Use in tests.
    pub fn in_memory() -> Self {
        Self {
            writer: JournalWriter::Memory(Vec::new()),
        }
    }

    /// Encode `entry` and write it to the journal immediately.
    ///
    /// For the file backend the OS-level write is flushed before returning so
    /// the entry survives a process crash (OS buffer may still be in-flight).
    pub fn append(&mut self, entry: &JournalEntry) -> Result<(), Error> {
        let bytes = encode_entry(entry);
        match &mut self.writer {
            JournalWriter::File(w) => {
                w.write_all(&bytes)?;
                w.flush()?;
            }
            JournalWriter::Memory(buf) => {
                buf.extend_from_slice(&bytes);
            }
        }
        Ok(())
    }

    /// Read back all entries written to this in-memory journal.
    ///
    /// # Panics
    /// Panics if called on a file-backed journal (use `decode_all` on the file
    /// bytes instead).
    pub fn read_all_in_memory(&self) -> Result<Vec<JournalEntry>, Error> {
        match &self.writer {
            JournalWriter::Memory(buf) => decode_all(buf),
            JournalWriter::File(_) => panic!("read_all_in_memory called on file journal"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientOrderIdGenerator;

    fn cloid() -> ClientOrderId {
        ClientOrderIdGenerator::new(0).next()
    }

    fn limit_request() -> OrderRequest {
        OrderRequest {
            symbol: "ETHUSDT".into(),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            qty: 500,
            limit_price: Some(3_000_000),
            time_in_force: TimeInForce::GoodTillCancel,
        }
    }

    fn market_request() -> OrderRequest {
        OrderRequest {
            symbol: "BTCUSDT".into(),
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            qty: 10,
            limit_price: None,
            time_in_force: TimeInForce::ImmediateOrCancel,
        }
    }

    fn write_read(entries: &[JournalEntry]) -> Vec<JournalEntry> {
        let mut j = Journal::in_memory();
        for e in entries {
            j.append(e).unwrap();
        }
        j.read_all_in_memory().unwrap()
    }

    #[test]
    fn order_requested_limit_round_trips() {
        let original = JournalEntry::OrderRequested {
            timestamp_ns: 123_456_789,
            cloid: cloid(),
            request: limit_request(),
        };
        let recovered = write_read(&[original.clone()]);
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0], original);
    }

    #[test]
    fn order_requested_market_round_trips() {
        let original = JournalEntry::OrderRequested {
            timestamp_ns: 0,
            cloid: cloid(),
            request: market_request(),
        };
        let recovered = write_read(&[original.clone()]);
        assert_eq!(recovered[0], original);
    }

    #[test]
    fn order_acknowledged_round_trips() {
        let original = JournalEntry::OrderAcknowledged {
            timestamp_ns: 999,
            cloid: cloid(),
            exchange_id: 0xdeadbeef_cafebabe,
        };
        let recovered = write_read(&[original.clone()]);
        assert_eq!(recovered[0], original);
    }

    #[test]
    fn order_filled_partial_round_trips() {
        let original = JournalEntry::OrderFilled {
            timestamp_ns: 1_000_000,
            cloid: cloid(),
            fill_qty: 50,
            fill_price: 99_500,
            is_final: false,
        };
        let recovered = write_read(&[original.clone()]);
        assert_eq!(recovered[0], original);
    }

    #[test]
    fn order_filled_final_round_trips() {
        let original = JournalEntry::OrderFilled {
            timestamp_ns: 2_000_000,
            cloid: cloid(),
            fill_qty: 100,
            fill_price: 99_900,
            is_final: true,
        };
        let recovered = write_read(&[original.clone()]);
        assert_eq!(recovered[0], original);
    }

    #[test]
    fn order_cancelled_round_trips() {
        let original = JournalEntry::OrderCancelled {
            timestamp_ns: 5_000,
            cloid: cloid(),
        };
        let recovered = write_read(&[original.clone()]);
        assert_eq!(recovered[0], original);
    }

    #[test]
    fn order_rejected_round_trips() {
        let original = JournalEntry::OrderRejected {
            timestamp_ns: 7_000,
            cloid: cloid(),
            reason: "MIN_NOTIONAL filter violated".into(),
        };
        let recovered = write_read(&[original.clone()]);
        assert_eq!(recovered[0], original);
    }

    #[test]
    fn order_expired_round_trips() {
        let original = JournalEntry::OrderExpired {
            timestamp_ns: 8_000,
            cloid: cloid(),
        };
        let recovered = write_read(&[original.clone()]);
        assert_eq!(recovered[0], original);
    }

    #[test]
    fn multiple_entries_round_trip_in_order() {
        let mut gen = ClientOrderIdGenerator::new(0);
        let c1 = gen.next();
        let c2 = gen.next();

        let entries = vec![
            JournalEntry::OrderRequested {
                timestamp_ns: 1,
                cloid: c1.clone(),
                request: limit_request(),
            },
            JournalEntry::OrderAcknowledged {
                timestamp_ns: 2,
                cloid: c1.clone(),
                exchange_id: 100,
            },
            JournalEntry::OrderFilled {
                timestamp_ns: 3,
                cloid: c1.clone(),
                fill_qty: 500,
                fill_price: 3_001_000,
                is_final: true,
            },
            JournalEntry::OrderRequested {
                timestamp_ns: 4,
                cloid: c2.clone(),
                request: market_request(),
            },
            JournalEntry::OrderRejected {
                timestamp_ns: 5,
                cloid: c2.clone(),
                reason: "INSUFFICIENT_FUNDS".into(),
            },
        ];

        let recovered = write_read(&entries);
        assert_eq!(recovered, entries);
    }

    #[test]
    fn empty_journal_returns_empty_vec() {
        let j = Journal::in_memory();
        let recovered = j.read_all_in_memory().unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn timestamp_is_preserved() {
        let ts: i64 = 1_700_000_000_000_000_000;
        let entry = JournalEntry::OrderExpired {
            timestamp_ns: ts,
            cloid: cloid(),
        };
        let recovered = write_read(&[entry]);
        assert_eq!(recovered[0].timestamp_ns(), ts);
    }

    #[test]
    fn cloid_accessor_is_consistent() {
        let c = cloid();
        let entry = JournalEntry::OrderCancelled {
            timestamp_ns: 0,
            cloid: c.clone(),
        };
        let recovered = write_read(&[entry]);
        assert_eq!(recovered[0].cloid(), &c);
    }

    #[test]
    fn all_tif_variants_round_trip() {
        for tif in [
            TimeInForce::GoodTillCancel,
            TimeInForce::ImmediateOrCancel,
            TimeInForce::FillOrKill,
        ] {
            let entry = JournalEntry::OrderRequested {
                timestamp_ns: 0,
                cloid: cloid(),
                request: OrderRequest {
                    symbol: "BTCUSDT".into(),
                    side: OrderSide::Buy,
                    order_type: OrderType::Limit,
                    qty: 1,
                    limit_price: Some(1),
                    time_in_force: tif,
                },
            };
            let recovered = write_read(&[entry.clone()]);
            assert_eq!(recovered[0], entry, "TIF round-trip failed for {tif:?}");
        }
    }
}
