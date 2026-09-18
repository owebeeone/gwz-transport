use crate::codec::*;

/// Nonallocating conservative bound for encode temporaries plus generated copies.
pub(crate) struct Budget {
    encoded: usize,
    allocation: usize,
    entries: usize,
    bootstrap: bool,
    limits: crate::protocol::Limits,
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
        let (max_frame, max_alloc) = if self.bootstrap {
            (
                MAX_BOOTSTRAP.min(self.limits.encoded_frame as usize),
                MAX_BOOTSTRAP_ALLOCATION.min(self.limits.decode_allocation as usize),
            )
        } else {
            (
                self.limits.encoded_frame as usize,
                self.limits.decode_allocation as usize,
            )
        };
        if self.encoded > max_frame || self.allocation > max_alloc {
            return Err(Error::Bounds);
        }
        Ok(())
    }
    pub(crate) fn scalar(&mut self) -> Result<(), Error> {
        self.charge(9, 4 * std::mem::size_of::<crate::cbor::Cbor>() + 32)
    }
    pub(crate) fn bytes(&mut self, len: usize, max: usize) -> Result<(), Error> {
        let negotiated = if max == MAX_DATA {
            self.limits.data_payload
        } else {
            self.limits.metadata_bytes
        };
        if len > max.min(negotiated as usize) {
            return Err(Error::Bounds);
        }
        self.scalar()?;
        self.charge(len, len.checked_mul(4).ok_or(Error::Bounds)?)
    }
    pub(crate) fn container(&mut self, count: usize) -> Result<(), Error> {
        self.entries = self.entries.checked_add(count).ok_or(Error::Bounds)?;
        if self.entries > self.limits.collection_entries as usize {
            return Err(Error::Bounds);
        }
        self.scalar()?;
        self.charge(count * 9, count * 32)
    }
}
