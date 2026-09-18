use crate::codec::*;

/// Nonallocating typed walk: exact canonical encoded length and the same
/// conservative live-allocation charge used by serialized preflight.
pub(crate) struct Budget {
    encoded: usize,
    allocation: usize,
    entries: usize,
    bootstrap: bool,
    limits: crate::protocol::Limits,
}
fn head(value: u64) -> usize {
    match value {
        0..=23 => 1,
        24..=255 => 2,
        256..=65535 => 3,
        65536..=0xffff_ffff => 5,
        _ => 9,
    }
}
impl Budget {
    pub(crate) fn new(bootstrap: bool, limits: &crate::protocol::Limits) -> Self {
        Self {
            encoded: 0,
            allocation: 0,
            entries: 0,
            bootstrap,
            limits: limits.clone(),
        }
    }
    fn charge(&mut self, encoded: usize, allocation: usize) -> Result<(), Error> {
        self.encoded = self.encoded.checked_add(encoded).ok_or(Error::Bounds)?;
        self.allocation = self
            .allocation
            .checked_add(allocation)
            .ok_or(Error::Bounds)?;
        let frame = self.limits.encoded_frame as usize;
        let allocation = self.limits.decode_allocation as usize;
        let (max_frame, max_allocation) = if self.bootstrap {
            (
                frame.min(MAX_BOOTSTRAP),
                allocation.min(MAX_BOOTSTRAP_ALLOCATION),
            )
        } else {
            (frame, allocation)
        };
        if self.encoded > max_frame
            || self
                .allocation
                .checked_add(self.encoded)
                .ok_or(Error::Bounds)?
                > max_allocation
        {
            return Err(Error::Bounds);
        }
        Ok(())
    }
    fn node(&mut self, depth: usize, encoded: usize) -> Result<(), Error> {
        self.entries = self.entries.checked_add(1).ok_or(Error::Bounds)?;
        if depth > self.limits.nesting as usize
            || self.entries > self.limits.collection_entries as usize
        {
            return Err(Error::Bounds);
        }
        self.charge(encoded, 4 * std::mem::size_of::<crate::cbor::Cbor>() + 32)
    }
    pub(crate) fn scalar(&mut self, depth: usize) -> Result<(), Error> {
        self.node(depth, 1)
    }
    pub(crate) fn integer(&mut self, value: i64, depth: usize) -> Result<(), Error> {
        let argument = if value < 0 {
            value.unsigned_abs() - 1
        } else {
            value as u64
        };
        self.node(depth, head(argument))
    }
    pub(crate) fn key(&mut self, tag: u64) -> Result<(), Error> {
        self.charge(head(tag), 32)
    }
    pub(crate) fn bytes(&mut self, len: usize, max: usize, depth: usize) -> Result<(), Error> {
        let negotiated = if max == MAX_DATA {
            self.limits.data_payload
        } else {
            self.limits.metadata_bytes
        };
        if len > max.min(negotiated as usize) {
            return Err(Error::Bounds);
        }
        self.node(depth, head(len as u64))?;
        self.charge(len, len.checked_mul(4).ok_or(Error::Bounds)?)
    }
    pub(crate) fn container(&mut self, count: usize, depth: usize) -> Result<(), Error> {
        self.node(depth, head(count as u64))?;
        if count > (self.limits.collection_entries as usize).saturating_sub(self.entries) {
            return Err(Error::Bounds);
        }
        Ok(())
    }
}
