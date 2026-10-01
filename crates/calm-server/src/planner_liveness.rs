//! Planner harness ownership for OpenCode session reaping. Native outcomes stay with the adapter.
use crate::harness::HarnessRegistry;
use crate::session_projection_repo::AgentProvider;
use async_trait::async_trait;
use calm_exec::{SpawnCtx, WorkerProvider};
use calm_types::error::CoreError;
use calm_types::worker::{
    ExitEvidence, ExitInterpretation, Liveness, SessionMode, WorkerContract, WorkerSession,
};

pub struct OpenCodePlannerProvider(pub HarnessRegistry);

#[async_trait]
impl WorkerProvider for OpenCodePlannerProvider {
    fn kind(&self) -> &'static str {
        "opencode"
    }
    fn session_mode(&self) -> SessionMode {
        SessionMode::Resumable
    }
    async fn probe_liveness(
        &self,
        session: &WorkerSession,
        ctx: &SpawnCtx,
    ) -> Result<Liveness, CoreError> {
        if session.contract == WorkerContract::Planner
            && let Some(handle) = self.0.get(&session.id.0)
            && handle.provider() == AgentProvider::OpenCode
        {
            // A registered harness owns pending input even before its native process starts.
            // This never serves as evidence of an OpenCode turn's completion.
            return Ok(Liveness::Alive {
                active_turn_id: session.active_turn_id.clone(),
            });
        }
        Ok(Liveness::Unknown {
            since_ms: ctx.now_ms,
        })
    }
    async fn interpret_exit(
        &self,
        _session: &WorkerSession,
        _evidence: &ExitEvidence,
        _ctx: &SpawnCtx,
    ) -> Result<ExitInterpretation, CoreError> {
        // Keep the durable intent: loss of a process does not prove whether a tool executed.
        Ok(ExitInterpretation::PreserveCard)
    }
}
