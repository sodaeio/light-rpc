use crate::{Slot, SlotStatus};
use std::collections::BTreeMap;

/// Per-slot commitment state machine: processed → confirmed → finalized (or dead).
pub struct CommitmentTracker {
    slots: BTreeMap<Slot, SlotStatus>,
    processed: Slot,
    confirmed: Slot,
    finalized: Slot,
}

impl CommitmentTracker {
    pub fn new() -> Self {
        Self {
            slots: BTreeMap::new(),
            processed: 0,
            confirmed: 0,
            finalized: 0,
        }
    }

    pub fn processed_slot(&self) -> Slot {
        self.processed
    }

    pub fn confirmed_slot(&self) -> Slot {
        self.confirmed
    }

    pub fn finalized_slot(&self) -> Slot {
        self.finalized
    }

    pub fn set_processed(&mut self, slot: Slot) {
        self.slots
            .entry(slot)
            .or_insert(SlotStatus::ProcessedOrSkipped);
        if slot > self.processed {
            self.processed = slot;
        }
    }

    pub fn set_dead(&mut self, slot: Slot) {
        self.slots.insert(slot, SlotStatus::Dead);
    }

    /// Returns false if `slot` is already confirmed/finalized/dead.
    pub fn set_confirmed(&mut self, slot: Slot) -> bool {
        match self.slots.get(&slot) {
            Some(SlotStatus::Confirmed | SlotStatus::Finalized) => return false,
            Some(SlotStatus::Dead) => return false,
            _ => {}
        }
        self.slots.insert(slot, SlotStatus::Confirmed);
        if slot > self.confirmed {
            self.confirmed = slot;
        }
        true
    }

    /// Returns false if `slot` is already finalized or dead.
    pub fn set_finalized(&mut self, slot: Slot) -> bool {
        match self.slots.get(&slot) {
            Some(SlotStatus::Finalized) => return false,
            Some(SlotStatus::Dead) => return false,
            _ => {}
        }
        self.slots.insert(slot, SlotStatus::Finalized);
        if slot > self.finalized {
            self.finalized = slot;
        }
        self.gc();
        true
    }

    pub fn status(&self, slot: Slot) -> Option<SlotStatus> {
        self.slots.get(&slot).copied()
    }

    fn gc(&mut self) {
        if self.finalized > 0 {
            self.slots = self.slots.split_off(&self.finalized);
        }
    }
}

impl Default for CommitmentTracker {
    fn default() -> Self {
        Self::new()
    }
}
