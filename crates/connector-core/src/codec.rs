use crate::Error;

/// Cursor-based encoder for message bodies (not the header — use `MessageHeader::encode_into`).
pub struct Encoder<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> Encoder<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline]
    fn need(&self, n: usize) -> Result<(), Error> {
        if self.buf.len().saturating_sub(self.pos) < n {
            Err(Error::BufferTooShort {
                needed: self.pos + n,
                have: self.buf.len(),
            })
        } else {
            Ok(())
        }
    }

    pub fn put_u8(&mut self, v: u8) -> Result<(), Error> {
        self.need(1)?;
        self.buf[self.pos] = v;
        self.pos += 1;
        Ok(())
    }

    pub fn put_bool(&mut self, v: bool) -> Result<(), Error> {
        self.put_u8(v as u8)
    }

    pub fn put_u16(&mut self, v: u16) -> Result<(), Error> {
        self.need(2)?;
        self.buf[self.pos..self.pos + 2].copy_from_slice(&v.to_le_bytes());
        self.pos += 2;
        Ok(())
    }

    pub fn put_u32(&mut self, v: u32) -> Result<(), Error> {
        self.need(4)?;
        self.buf[self.pos..self.pos + 4].copy_from_slice(&v.to_le_bytes());
        self.pos += 4;
        Ok(())
    }

    pub fn put_u64(&mut self, v: u64) -> Result<(), Error> {
        self.need(8)?;
        self.buf[self.pos..self.pos + 8].copy_from_slice(&v.to_le_bytes());
        self.pos += 8;
        Ok(())
    }

    pub fn put_i64(&mut self, v: i64) -> Result<(), Error> {
        self.need(8)?;
        self.buf[self.pos..self.pos + 8].copy_from_slice(&v.to_le_bytes());
        self.pos += 8;
        Ok(())
    }

    /// Encodes as u16 length prefix + UTF-8 bytes. Symbols are always short, so u16 is sufficient.
    pub fn put_str(&mut self, s: &str) -> Result<(), Error> {
        let b = s.as_bytes();
        if b.len() > u16::MAX as usize {
            return Err(Error::StringTooLong {
                len: b.len(),
                max: u16::MAX as usize,
            });
        }
        self.put_u16(b.len() as u16)?;
        self.need(b.len())?;
        self.buf[self.pos..self.pos + b.len()].copy_from_slice(b);
        self.pos += b.len();
        Ok(())
    }

    pub fn finish(self) -> usize {
        self.pos
    }
}

/// Cursor-based decoder for message bodies.
pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline]
    fn need(&self, n: usize) -> Result<(), Error> {
        if self.buf.len().saturating_sub(self.pos) < n {
            Err(Error::BufferTooShort {
                needed: self.pos + n,
                have: self.buf.len(),
            })
        } else {
            Ok(())
        }
    }

    pub fn get_u8(&mut self) -> Result<u8, Error> {
        self.need(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    pub fn get_bool(&mut self) -> Result<bool, Error> {
        Ok(self.get_u8()? != 0)
    }

    pub fn get_u16(&mut self) -> Result<u16, Error> {
        self.need(2)?;
        let v = u16::from_le_bytes(self.buf[self.pos..self.pos + 2].try_into().unwrap());
        self.pos += 2;
        Ok(v)
    }

    pub fn get_u32(&mut self) -> Result<u32, Error> {
        self.need(4)?;
        let v = u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    pub fn get_u64(&mut self) -> Result<u64, Error> {
        self.need(8)?;
        let v = u64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    pub fn get_i64(&mut self) -> Result<i64, Error> {
        self.need(8)?;
        let v = i64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    pub fn get_str(&mut self) -> Result<String, Error> {
        let len = self.get_u16()? as usize;
        self.need(len)?;
        let s = std::str::from_utf8(&self.buf[self.pos..self.pos + len])
            .map_err(|_| Error::InvalidUtf8)?
            .to_owned();
        self.pos += len;
        Ok(s)
    }

    #[allow(dead_code)]
    pub fn pos(&self) -> usize {
        self.pos
    }
}
