use super::*;

/// A conservative live-allocation charge covers geometric container capacity,
/// map keys, the decoded CBOR tree and typed copies. Input bytes are charged too.
struct Scan<'a> {
    input: &'a [u8],
    at: usize,
    entries: usize,
    allocation: usize,
    limits: crate::protocol::Limits,
    bootstrap: bool,
}

pub(super) fn check(input: &[u8], limits: &crate::protocol::Limits) -> Result<(), Error> {
    if input.len() > limits.encoded_frame as usize {
        return Err(Error::FrameTooLarge);
    }
    let mut scan = Scan {
        input,
        at: 0,
        entries: 0,
        allocation: input.len(),
        limits: limits.clone(),
        bootstrap: false,
    };
    scan.item(0, 0, false)?;
    if scan.bootstrap && (input.len() > MAX_BOOTSTRAP || scan.allocation > MAX_BOOTSTRAP_ALLOCATION)
    {
        return Err(Error::Bounds);
    }
    if scan.at != input.len() {
        return Err(Error::Malformed);
    }
    Ok(())
}

impl Scan<'_> {
    fn take(&mut self, count: usize) -> Result<&[u8], Error> {
        let end = self.at.checked_add(count).ok_or(Error::Bounds)?;
        let bytes = self.input.get(self.at..end).ok_or(Error::Malformed)?;
        self.at = end;
        Ok(bytes)
    }

    fn head(&mut self) -> Result<(u8, u64), Error> {
        let byte = self.take(1)?[0];
        let info = byte & 31;
        let value = match info {
            0..=23 => u64::from(info),
            24 => u64::from(self.take(1)?[0]),
            25 => u64::from(u16::from_be_bytes(
                self.take(2)?.try_into().map_err(|_| Error::Malformed)?,
            )),
            26 => u64::from(u32::from_be_bytes(
                self.take(4)?.try_into().map_err(|_| Error::Malformed)?,
            )),
            27 => u64::from_be_bytes(self.take(8)?.try_into().map_err(|_| Error::Malformed)?),
            _ => {
                return Err(Error::Malformed);
            }
        };
        Ok((byte >> 5, value))
    }

    fn charge(&mut self, bytes: usize) -> Result<(), Error> {
        self.allocation = self.allocation.checked_add(bytes).ok_or(Error::Bounds)?;
        if self.allocation > self.limits.decode_allocation as usize {
            return Err(Error::Bounds);
        }
        Ok(())
    }

    fn item(&mut self, depth: usize, parent: u64, data_payload: bool) -> Result<(), Error> {
        if depth > self.limits.nesting as usize {
            return Err(Error::Bounds);
        }
        self.entries += 1;
        if self.entries > self.limits.collection_entries as usize {
            return Err(Error::Bounds);
        }
        self.charge(4 * std::mem::size_of::<cbor::Cbor>() + 32)?;
        let (major, argument) = self.head()?;
        if depth == 1 && parent == 3 && major == 0 && argument == 0 {
            self.bootstrap = true;
        }
        match major {
            0 | 1 | 7 => {}
            2 | 3 => {
                let len = usize::try_from(argument).map_err(|_| Error::Bounds)?;
                let max = if major == 2 && data_payload {
                    self.limits.data_payload as usize
                } else {
                    self.limits.metadata_bytes as usize
                };
                if len > max {
                    return Err(Error::Bounds);
                }
                self.charge(len.checked_mul(4).ok_or(Error::Bounds)?)?;
                self.take(len)?;
            }
            4 | 5 => {
                let count = usize::try_from(argument).map_err(|_| Error::Bounds)?;
                if count > (self.limits.collection_entries as usize).saturating_sub(self.entries) {
                    return Err(Error::Bounds);
                }
                for _ in 0..count {
                    if major == 5 {
                        let (key_major, key) = self.head()?;
                        if key_major != 0 {
                            return Err(Error::Malformed);
                        }
                        self.charge(32)?;
                        // Envelope.data (tag 16) -> Data.payload (tag 2), only.
                        self.item(depth + 1, key, depth == 1 && parent == 16 && key == 2)?;
                    } else {
                        self.item(depth + 1, 0, false)?;
                    }
                }
            }
            _ => {
                return Err(Error::Malformed);
            }
        }
        Ok(())
    }
}
