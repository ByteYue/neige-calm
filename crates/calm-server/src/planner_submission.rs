//! Provider admission is distinct from a turn's eventual outcome.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnAdmission {
    Accepted {
        turn_id: String,
    },
    /// Positive evidence that no operation was admitted.
    Rejected {
        reason: String,
    },
    /// The attempt may have executed. Preserve its projection and never resend it.
    Unknown {
        turn_id: String,
        reason: String,
    },
}
