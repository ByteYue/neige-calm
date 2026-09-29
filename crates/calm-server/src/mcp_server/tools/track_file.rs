//! Read-only MCP file views (`calm.track.ls`, `calm.track.cat`) rooted at the track bound to the caller's MCP connection.
//! A path under `area/` is the Planner's `area/reports/` view instead (#1838, [`super::area_reports`]).

use crate::area_reports;
use crate::mcp_server::framing::RpcError;
use crate::mcp_server::registry::{
    AppContext, ToolCallIdentity, ToolDescriptor, ToolHandler, ToolHandlerFuture, ToolRegistry,
    read_only_annotations, require_role_any,
};
use crate::mcp_server::tools::report_links::{unknown_block, unknown_section};
use crate::mcp_server::tools::track_report::load_report_for_track;
use crate::model::{Card, CardRole, Track};
use crate::report_sections::section_block_ids;
use crate::track_fs_view::{TrackFsContent, TrackFsError, TrackFsView, normalize_path};
use crate::track_report::ReportBlock;
use crate::track_report_read::{load_report_doc_snapshot, selected_blocks_text};
use serde_json::{Value, json};
use std::sync::Arc;

pub const TOOL_TRACK_LS: &str = "calm.track.ls";
pub const TOOL_TRACK_CAT: &str = "calm.track.cat";

pub fn register_into(registry: &mut ToolRegistry) {
    registry.register(ls_descriptor(), wrap(track_ls));
    registry.register(cat_descriptor(), wrap(track_cat));
}

fn wrap<F, Fut>(f: F) -> ToolHandler
where
    F: Fn(Arc<AppContext>, ToolCallIdentity, Value) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<Value, RpcError>> + Send + 'static,
{
    Arc::new(move |ctx, identity, args| -> ToolHandlerFuture {
        let result = f(ctx, identity, args);
        Box::pin(async move {
            result
                .await
                .map(crate::mcp_server::result::ToolResult::structured)
        })
    })
}

/// Return-shape contract consumed by `mcp_server::cli::render`: `ls` returns a bare JSON array; `cat` returns `{ content, content_type }`.
fn ls_descriptor() -> ToolDescriptor {
    ToolDescriptor {
        name: TOOL_TRACK_LS.into(),
        description: include_str!("../../../prompts/tools/calm.track.ls.md")
            .trim_end()
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "path": { "type": "string" }
            }
        }),
        annotations: Some(read_only_annotations()),
        visible_to_roles: &[],
    }
}

fn cat_descriptor() -> ToolDescriptor {
    ToolDescriptor {
        name: TOOL_TRACK_CAT.into(),
        description: include_str!("../../../prompts/tools/calm.track.cat.md")
            .trim_end()
            .to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": { "type": "string" },
                "blocks": {
                    "type": "array",
                    "items": { "type": "string" },
                    "minItems": 1,
                    "description": "Report paths only: print just these blocks, as calm.report.read select.blocks does."
                },
                "sections": {
                    "type": "array",
                    "items": { "type": "string" },
                    "minItems": 1,
                    "description": "Report paths only: print just these H1 sections, as select.sections does."
                }
            }
        }),
        annotations: Some(read_only_annotations()),
        visible_to_roles: &[],
    }
}

async fn track_ls(
    ctx: Arc<AppContext>,
    identity: ToolCallIdentity,
    args: Value,
) -> Result<Value, RpcError> {
    require_role_any(&identity, &[CardRole::Planner, CardRole::Worker])?;
    let path = parse_path_arg(&args, false)?;
    if let Some(area_path) = area_reports::classify(&path) {
        return super::area_reports::ls(&ctx, &identity, area_path).await;
    }
    let (_, track) = resolve_track_for_identity(&ctx, &identity).await?;
    let view = TrackFsView::new(ctx.repo.as_ref(), &ctx.write);
    let entries = view
        .ls(&track, Some(path.as_str()))
        .await
        .map_err(track_fs_error_to_rpc)?;
    serde_json::to_value(entries)
        .map_err(|e| RpcError::internal(format!("track_file: json serialization: {e}")))
}

async fn track_cat(
    ctx: Arc<AppContext>,
    identity: ToolCallIdentity,
    args: Value,
) -> Result<Value, RpcError> {
    require_role_any(&identity, &[CardRole::Planner, CardRole::Worker])?;
    let path = parse_path_arg(&args, true)?;
    let selection = parse_selection_arg(&args)?;
    if let Some(area_path) = area_reports::classify(&path) {
        return super::area_reports::cat(&ctx, &identity, &path, area_path, selection.as_ref())
            .await;
    }
    if path == OWN_REPORT {
        return own_report(&ctx, &identity, selection.as_ref()).await;
    }
    if let Some(selection) = selection {
        return Err(not_a_report(&path, &selection));
    }
    let (_, track) = resolve_track_for_identity(&ctx, &identity).await?;
    // `plan/<key>/gate.log` is enabled only here (MCP carries a card identity); the gate-logs dir is the configured one, never recomputed from env.
    let view = TrackFsView::new(ctx.repo.as_ref(), &ctx.write)
        .with_gate_log_access(identity.role, ctx.gate_logs_dir.clone());
    let content = view
        .cat(&track, path.as_str())
        .await
        .map_err(track_fs_error_to_rpc)?;
    serde_json::to_value(content)
        .map_err(|e| RpcError::internal(format!("track_file: json serialization: {e}")))
}

fn parse_path_arg(args: &Value, required: bool) -> Result<String, RpcError> {
    let obj = args
        .as_object()
        .ok_or_else(|| RpcError::invalid_params("calm.track: arguments must be an object"))?;
    let Some(raw) = obj.get("path") else {
        if required {
            return Err(RpcError::invalid_params(
                "calm.track.cat: missing `path` (string)",
            ));
        }
        return Ok(String::new());
    };
    let path = raw
        .as_str()
        .ok_or_else(|| RpcError::invalid_params("calm.track: `path` must be a string"))?;
    Ok(normalize_path(path))
}

/// The caller's own report in the track view.
const OWN_REPORT: &str = "report.md";

/// A partial report read, the same on every report path and in `calm.report.read`'s `select`:
/// chosen blocks (#1874) or chosen H1 sections (#1877).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Selection {
    Blocks(Vec<String>),
    Sections(Vec<String>),
}

impl Selection {
    /// `{ blocks: [..] }` / `{ sections: [..] }` as one key of `map`; `None` when neither is present.
    pub(crate) fn parse(
        map: &serde_json::Map<String, Value>,
        tool: &str,
    ) -> Result<Option<Self>, RpcError> {
        let list = |key: &str, what: &str| -> Result<Option<Vec<String>>, RpcError> {
            match map.get(key) {
                None | Some(Value::Null) => Some(None),
                Some(Value::Array(items)) if !items.is_empty() => items
                    .iter()
                    .map(|item| item.as_str().map(str::to_string))
                    .collect::<Option<Vec<_>>>()
                    .map(Some),
                Some(_) => None,
            }
            .ok_or_else(|| {
                RpcError::invalid_params(format!(
                    "{tool}: `{key}` must be a non-empty array of {what}"
                ))
            })
        };
        match (
            list("blocks", "block ids")?,
            list("sections", "section headings")?,
        ) {
            (Some(_), Some(_)) => Err(RpcError::invalid_params(format!(
                "{tool}: pass `blocks` or `sections`, not both"
            ))),
            (Some(ids), None) => Ok(Some(Self::Blocks(ids))),
            (None, Some(names)) => Ok(Some(Self::Sections(names))),
            (None, None) => Ok(None),
        }
    }

    /// The block ids this selection renders; an unknown or ambiguous section is refused with the
    /// report's sections, an unknown block id later by the renderer.
    pub(crate) fn block_ids(&self, blocks: &[ReportBlock]) -> Result<Vec<String>, RpcError> {
        match self {
            Self::Blocks(ids) => Ok(ids.clone()),
            Self::Sections(names) => {
                section_block_ids(blocks, names).map_err(|error| unknown_section(blocks, &error))
            }
        }
    }
}

/// `blocks` / `sections` (`neige cat --blocks b_x,b_y`, `--sections A,B`): absent reads the whole file.
fn parse_selection_arg(args: &Value) -> Result<Option<Selection>, RpcError> {
    match args.as_object() {
        Some(map) => Selection::parse(map, TOOL_TRACK_CAT),
        None => Ok(None),
    }
}

/// The one refusal of a selection on a path that names no report.
pub(crate) fn not_a_report(path: &str, selection: &Selection) -> RpcError {
    let (flag, unit) = match selection {
        Selection::Blocks(_) => ("--blocks", "blocks"),
        Selection::Sections(_) => ("--sections", "sections"),
    };
    RpcError::invalid_params(format!(
        "`{path}` is not a report; {flag} reads {unit} of `{OWN_REPORT}` or `{}/<name>.md` \
         only",
        area_reports::REPORTS_DIR
    ))
}

/// The caller's own `report.md`, whole or narrowed, from one snapshot. Every text it returns is
/// recorded as this session's read of those blocks (#1877), but not as a read of the docRev: cat
/// shows no summary, docRev or index, so it cannot anchor a whole-document write.
async fn own_report(
    ctx: &Arc<AppContext>,
    identity: &ToolCallIdentity,
    selection: Option<&Selection>,
) -> Result<Value, RpcError> {
    let (_, track) = resolve_track_for_identity(ctx, identity).await?;
    let (report_card, _) = load_report_for_track(ctx, &track).await?;
    let snapshot = load_report_doc_snapshot(ctx.repo.as_ref(), report_card.id.as_str())
        .await
        .map_err(|e| RpcError::internal(format!("track_file: {e}")))?;
    let (content, rendered) = match selection {
        Some(selection) => {
            let ids = selection.block_ids(&snapshot.blocks)?;
            (report_blocks_content(&snapshot.blocks, &ids)?, ids)
        }
        None => (
            markdown_content(snapshot.body.clone())?,
            snapshot
                .blocks
                .iter()
                .map(|block| block.id.clone())
                .collect(),
        ),
    };
    ctx.read_ledger.record(
        &identity.session_id,
        &identity.card_id,
        report_card.id.as_str(),
        None,
        &snapshot.blocks,
        &rendered,
    );
    Ok(content)
}

/// A report narrowed to `ids`: the exact `text` of `calm.report.read { select: { blocks: ids } }`.
/// An unknown id is refused with the report's blocks as `<id>  <heading>` lines.
pub(crate) fn report_blocks_content(
    blocks: &[ReportBlock],
    ids: &[String],
) -> Result<Value, RpcError> {
    markdown_content(selected_blocks_text(blocks, ids).map_err(|id| unknown_block(blocks, id))?)
}

fn markdown_content(content: String) -> Result<Value, RpcError> {
    serde_json::to_value(TrackFsContent {
        content,
        content_type: "text/markdown".into(),
    })
    .map_err(|e| RpcError::internal(format!("track_file: json serialization: {e}")))
}

pub(crate) async fn resolve_track_for_identity(
    ctx: &Arc<AppContext>,
    identity: &ToolCallIdentity,
) -> Result<(Card, Track), RpcError> {
    let card_id_str = identity.card_id.as_str().to_string();
    let card = ctx
        .repo
        .card_get(&card_id_str)
        .await
        .map_err(|e| RpcError::internal(format!("track_file: card lookup: {e}")))?
        .ok_or_else(|| {
            RpcError::internal(format!(
                "track_file: bound card {card_id_str} not found (deleted mid-connection?)"
            ))
        })?;
    let track = ctx
        .repo
        .track_get(card.track_id.as_str())
        .await
        .map_err(|e| RpcError::internal(format!("track_file: track lookup: {e}")))?
        .ok_or_else(|| {
            RpcError::internal(format!(
                "track_file: track {} for card {} not found",
                card.track_id.as_str(),
                card_id_str
            ))
        })?;
    Ok((card, track))
}

pub(crate) fn track_fs_error_to_rpc(err: TrackFsError) -> RpcError {
    match err {
        TrackFsError::PathNotAvailable(message) => RpcError::invalid_params(message),
        TrackFsError::Forbidden(message) => RpcError::custom(-32403, message),
        TrackFsError::Internal(message) => RpcError::internal(message),
    }
}
