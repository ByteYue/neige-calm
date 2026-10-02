//! Public metadata for operator-registered native OpenCode connections.
use serde::{Deserialize, Serialize};
use ts_rs::TS;
use utoipa::ToSchema;

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, TS)]
#[ts(export, export_to = "fe/core/api/generated/wire.ts")]
pub struct ConnectionSummary {
    pub id: String,
    pub label: String,
    pub directory: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema, TS)]
#[ts(export, export_to = "fe/core/api/generated/wire.ts")]
pub struct ConnectionsResponse {
    pub connections: Vec<ConnectionSummary>,
}
