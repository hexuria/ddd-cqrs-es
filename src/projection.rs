use crate::event::EventEnvelope;

/// A read-model updater.
///
/// Projections consume event envelopes and update query-optimized state.
pub trait Projection<E, M = ()> {
    type Error;

    fn apply(&mut self, event: &EventEnvelope<E, M>) -> Result<(), Self::Error>;
}
