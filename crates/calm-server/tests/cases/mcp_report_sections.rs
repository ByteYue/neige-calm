//! #1877: section-addressed `calm.report.commit` ops anchored by the session's read ledger. A write
//! passes no rev; the kernel checks it against what this session last read, inside the persist tx.

#![cfg(unix)]

use crate::mcp_track_report::{
    Boot, assistant_identity, boot, call_tool, planner_identity, seed_track_root_session,
};
use calm_server::mcp_server::ToolCallIdentity;
use calm_server::mcp_server::tools::track_file::TOOL_TRACK_CAT;
use calm_server::mcp_server::tools::track_report::TOOL_REPORT_READ;
use calm_server::mcp_server::tools::track_report_blocks::{
    RPC_REV_CONFLICT, TOOL_REPORT_BLOCKS_UPSERT, TOOL_REPORT_COMMIT, TOOL_REPORT_WRITE_MARKDOWN,
};
use calm_server::plugin_host::mcp::RpcError;
use calm_server::track_report::TrackReportPayload;
use serde_json::{Value, json};

const INVALID_PARAMS: i64 = -32602;
const CONTRACT: &str = "<!-- neige:contract {\"version\":1,\"sections\":[{\"h1\":\"概要\"},\
                        {\"h1\":\"待你定\",\"omit_if_empty\":true},{\"h1\":\"已完成\"},\
                        {\"h1\":\"决策\"}]} -->\n";
/// `待你定` is declared but absent; `概要` spans two blocks.
const SECTIONS: &str = "# 概要\n\nalpha\n\n## 细节\n\na2\n\n# 已完成\n\nbeta\n\n# 决策\n\ngamma\n";

async fn seed(boot: &Boot, sections: &str) {
    call_tool(
        boot,
        TOOL_REPORT_WRITE_MARKDOWN,
        planner_identity(boot),
        json!({ "body": format!("{CONTRACT}{sections}"), "message": "seed", "if_doc_rev": 0 }),
    )
    .await
    .expect("seed write");
}

async fn payload(boot: &Boot) -> TrackReportPayload {
    let card = boot
        .repo
        .card_get(boot.report_card_id.as_str())
        .await
        .unwrap()
        .expect("report card row");
    serde_json::from_value(card.payload).expect("payload")
}

/// `(id, rev)` of the blocks of the section `heading` opens, straight from the row (not a tool read).
async fn section_blocks(boot: &Boot, heading: &str) -> Vec<(String, u64)> {
    let blocks = payload(boot).await.blocks.expect("blocks");
    let first_line = |b: &calm_server::track_report::ReportBlock| {
        b.payload["markdown"]
            .as_str()
            .and_then(|m| m.split('\n').next())
            .map(str::to_string)
    };
    let start = blocks
        .iter()
        .position(|b| first_line(b).as_deref() == Some(heading))
        .unwrap_or_else(|| panic!("no {heading}"));
    let end = blocks[start + 1..]
        .iter()
        .position(|b| first_line(b).is_some_and(|line| line.starts_with("# ")))
        .map_or(blocks.len(), |n| start + 1 + n);
    blocks[start..end]
        .iter()
        .map(|b| (b.id.clone(), u64::from(b.rev)))
        .collect()
}

async fn read_sections(boot: &Boot, who: ToolCallIdentity, sections: &[&str]) -> String {
    call_tool(
        boot,
        TOOL_REPORT_READ,
        who,
        json!({ "select": { "sections": sections } }),
    )
    .await
    .expect("read sections")["text"]
        .as_str()
        .expect("text")
        .to_string()
}

async fn commit(boot: &Boot, who: ToolCallIdentity, extra: Value) -> Result<Value, RpcError> {
    let mut args = json!({ "message": "改写" });
    args.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    call_tool(boot, TOOL_REPORT_COMMIT, who, args).await
}

fn replace(section: &str, markdown: &str) -> Value {
    json!({ "ops": [{ "op": "replace", "section": section, "markdown": markdown }] })
}

/// Another session edits the first block of `heading` with explicit revs.
async fn assistant_edits(boot: &Boot, heading: &str, markdown: &str) {
    let (id, rev) = section_blocks(boot, heading).await.remove(0);
    call_tool(
        boot,
        TOOL_REPORT_BLOCKS_UPSERT,
        assistant_identity(boot),
        json!({ "id": id, "if_rev": rev, "kind": "prose", "markdown": markdown }),
    )
    .await
    .expect("the other writer's edit");
}

/// Supersede the planner card's session with a fresh one, as a planner restart does; the successor's identity.
async fn supersede_planner(boot: &Boot, current: &str, successor: &str) -> ToolCallIdentity {
    let pool = boot.repo.sqlite_pool().expect("sqlite pool");
    let mut tx = calm_server::db::sqlite::begin_immediate_tx(&pool)
        .await
        .unwrap();
    calm_server::db::sqlite::session_supersede_active_tx(
        &mut tx,
        &current.to_string(),
        calm_server::model::now_ms(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    seed_track_root_session(
        boot.repo.as_ref(),
        &boot.track_id,
        &boot.planner_card_id,
        successor,
    )
    .await;
    ToolCallIdentity {
        session_id: successor.to_string(),
        ..planner_identity(boot)
    }
}

#[tokio::test]
async fn read_sections_then_replace_without_revs_keeps_ids_and_touches_only_the_section() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    let before = section_blocks(&boot, "# 概要").await;
    let text = read_sections(&boot, planner_identity(&boot), &["概要"]).await;
    assert!(
        text.starts_with(&format!("<!-- neige:{} -->\n# 概要", before[0].0)),
        "{text}"
    );

    let out = commit(
        &boot,
        planner_identity(&boot),
        replace("概要", &text.replace("a2", "a3")),
    )
    .await
    .expect("no revs: the read anchors the write");
    assert!(out["docRev"].as_u64().is_some(), "{out}");
    let after = section_blocks(&boot, "# 概要").await;
    assert_eq!(
        after[0], before[0],
        "the unchanged H1 block keeps id and rev"
    );
    assert_eq!(
        (after[1].0.as_str(), after[1].1),
        (before[1].0.as_str(), before[1].1 + 1)
    );
    let body = payload(&boot).await.body;
    assert!(
        body.starts_with(CONTRACT) && body.contains("a3\n\n# 已完成\n\nbeta\n"),
        "{body}"
    );

    // `neige cat report.md --sections` is a read of the same kind.
    let cat = call_tool(
        &boot,
        TOOL_TRACK_CAT,
        planner_identity(&boot),
        json!({ "path": "report.md", "sections": ["决策"] }),
    )
    .await
    .expect("cat --sections");
    let decision = cat["content"].as_str().expect("content");
    commit(
        &boot,
        planner_identity(&boot),
        replace("决策", &decision.replace("gamma", "delta")),
    )
    .await
    .expect("a cat --sections read anchors the write too");
    assert!(payload(&boot).await.body.ends_with("# 决策\n\ndelta\n"));
}

#[tokio::test]
async fn a_section_changed_after_the_read_is_a_conflict_until_it_is_read_again() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    read_sections(&boot, planner_identity(&boot), &["已完成"]).await;
    assistant_edits(&boot, "# 已完成", "# 已完成\n\nbeta by assistant\n").await;
    let before = payload(&boot).await;

    let err = commit(
        &boot,
        planner_identity(&boot),
        replace("已完成", "# 已完成\n\nmine\n"),
    )
    .await
    .expect_err("stale section");
    assert_eq!(err.code, RPC_REV_CONFLICT, "{err:?}");
    assert!(err.message.contains("section `已完成` changed"), "{err:?}");
    assert_eq!(payload(&boot).await.body, before.body, "nothing written");

    let text = read_sections(&boot, planner_identity(&boot), &["已完成"]).await;
    assert!(text.contains("beta by assistant"), "{text}");
    commit(
        &boot,
        planner_identity(&boot),
        replace("已完成", &format!("{text}mine\n")),
    )
    .await
    .expect("re-read, merge, retry");
    assert!(
        payload(&boot)
            .await
            .body
            .contains("beta by assistant\nmine\n")
    );
}

#[tokio::test]
async fn an_edit_elsewhere_refuses_only_a_commit_that_carries_a_summary() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    read_sections(&boot, planner_identity(&boot), &["已完成"]).await;
    assistant_edits(&boot, "# 决策", "# 决策\n\nelsewhere\n").await;
    commit(
        &boot,
        planner_identity(&boot),
        replace("已完成", "# 已完成\n\nb2\n"),
    )
    .await
    .expect("an unrelated section's edit does not refuse a section write");

    read_sections(&boot, planner_identity(&boot), &["已完成"]).await;
    assistant_edits(&boot, "# 决策", "# 决策\n\nelsewhere again\n").await;
    let before = payload(&boot).await;
    let mut args = replace("已完成", "# 已完成\n\nb3\n");
    args["summary"] = json!("新摘要");
    let err = commit(&boot, planner_identity(&boot), args)
        .await
        .expect_err("a summary is a whole-document write");
    assert_eq!(err.code, RPC_REV_CONFLICT, "{err:?}");
    assert!(
        err.message.contains("document revision conflict"),
        "{err:?}"
    );
    let after = payload(&boot).await;
    assert_eq!((after.body, after.summary), (before.body, before.summary));
}

#[tokio::test]
async fn a_session_that_never_read_is_refused_until_it_reads() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    let (id, _) = section_blocks(&boot, "# 决策").await.remove(0);
    let before = payload(&boot).await;
    for (args, needle) in [
        (
            replace("决策", "# 决策\n\nx\n"),
            "section `决策` has not been read",
        ),
        (
            json!({ "ops": [{ "op": "delete", "section": "决策" }] }),
            "has not been read",
        ),
        (
            json!({ "ops": [{ "op": "upsert", "id": id, "kind": "prose", "markdown": "# 决策\n" }] }),
            "has not been read by this session",
        ),
        (
            json!({ "summary": "s" }),
            "this session has not read the report",
        ),
    ] {
        let err = commit(&boot, planner_identity(&boot), args.clone())
            .await
            .expect_err("never read");
        assert_eq!(err.code, INVALID_PARAMS, "{args}: {err:?}");
        assert!(err.message.contains(needle), "{args}: {err:?}");
    }
    assert_eq!(payload(&boot).await.body, before.body);

    read_sections(&boot, planner_identity(&boot), &["决策"]).await;
    commit(
        &boot,
        planner_identity(&boot),
        replace("决策", "# 决策\n\nx\n"),
    )
    .await
    .expect("after the read");
    assert!(payload(&boot).await.body.ends_with("# 决策\n\nx\n"));
}

#[tokio::test]
async fn a_new_session_of_the_card_starts_with_an_empty_ledger() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    read_sections(&boot, planner_identity(&boot), &["决策"]).await;
    let old = planner_identity(&boot).session_id;
    let report = boot.report_card_id.as_str();
    assert!(boot.ctx.read_ledger.last_read(&old, report).is_some());

    let successor = supersede_planner(&boot, &old, "planner-session-2").await;
    let err = commit(&boot, successor.clone(), replace("决策", "# 决策\n\nx\n"))
        .await
        .expect_err("the predecessor's read is not the successor's");
    assert_eq!(err.code, INVALID_PARAMS, "{err:?}");
    read_sections(&boot, successor.clone(), &["决策"]).await;
    assert_eq!(
        boot.ctx.read_ledger.last_read(&old, report),
        None,
        "the successor's first read evicts the superseded session"
    );
    commit(&boot, successor.clone(), replace("决策", "# 决策\n\nx\n"))
        .await
        .expect("the successor's own read");
    // A session that never read writes with explicit revs exactly as before.
    let doc_rev = payload(&boot).await.doc_rev;
    let fresh = supersede_planner(&boot, "planner-session-2", "planner-session-3").await;
    commit(
        &boot,
        fresh,
        json!({ "if_doc_rev": doc_rev, "summary": "explicit" }),
    )
    .await
    .expect("an explicit if_doc_rev needs no read");
    assert_eq!(payload(&boot).await.summary, "explicit");
}

#[tokio::test]
async fn explicit_revs_still_decide_over_the_ledger() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    call_tool(&boot, TOOL_REPORT_READ, planner_identity(&boot), json!({}))
        .await
        .expect("full read");
    assistant_edits(&boot, "# 决策", "# 决策\n\nby assistant\n").await;
    let (id, rev) = section_blocks(&boot, "# 决策").await.remove(0);
    let doc_rev = payload(&boot).await.doc_rev;
    let upsert = |if_rev: u64| {
        json!({ "if_doc_rev": doc_rev, "summary": "s", "ops": [
            { "op": "upsert", "id": id, "if_rev": if_rev, "kind": "prose", "markdown": "# 决策\n\nmine\n" }
        ] })
    };
    let err = commit(&boot, planner_identity(&boot), upsert(rev - 1))
        .await
        .expect_err("a stale explicit if_rev");
    assert_eq!(err.code, RPC_REV_CONFLICT, "{err:?}");
    assert_eq!(err.data, Some(json!({ "docRev": doc_rev, "rev": rev })));
    commit(&boot, planner_identity(&boot), upsert(rev))
        .await
        .expect("current explicit revs win over the stale ledger");
    assert!(payload(&boot).await.body.ends_with("# 决策\n\nmine\n"));
}

#[tokio::test]
async fn a_section_delete_is_refused_while_it_holds_a_live_task() {
    let boot = boot().await;
    let task = calm_types::report_blocks::render_fence(
        "task",
        &json!({
            "key": "k1", "kind": "codex", "goal": "build it",
            "gate": {"steps": [{"name": "accept", "cmd": "true"}]},
            "declared_by": calm_types::report_blocks::tasks::PLANNER_DECLARATION_AUTHOR,
            "ready": true
        }),
    );
    seed(
        &boot,
        &SECTIONS.replace("beta\n", &format!("beta\n\n{task}")),
    )
    .await;
    call_tool(&boot, TOOL_REPORT_READ, planner_identity(&boot), json!({}))
        .await
        .expect("full read");
    let before = payload(&boot).await;
    let err = commit(
        &boot,
        planner_identity(&boot),
        json!({ "ops": [{ "op": "delete", "section": "已完成" }] }),
    )
    .await
    .expect_err("live task");
    assert_eq!(err.code, INVALID_PARAMS, "{err:?}");
    assert!(
        err.message
            .contains("must use the block-level DELETE endpoint"),
        "{err:?}"
    );
    assert_eq!(payload(&boot).await.body, before.body);

    commit(
        &boot,
        planner_identity(&boot),
        json!({ "ops": [{ "op": "delete", "section": "决策" }] }),
    )
    .await
    .expect("a section without a live task goes");
    assert!(!payload(&boot).await.body.contains("# 决策"));
}

#[tokio::test]
async fn replacing_an_absent_declared_section_creates_it_at_the_contract_position() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    call_tool(&boot, TOOL_REPORT_READ, planner_identity(&boot), json!({}))
        .await
        .expect("full read");
    commit(
        &boot,
        planner_identity(&boot),
        replace("待你定", "# 待你定\n\nq\n"),
    )
    .await
    .expect("a declared omit_if_empty section is created");
    let body = payload(&boot).await.body;
    assert!(body.contains("a2\n\n# 待你定\n\nq\n# 已完成\n"), "{body}");

    let err = commit(
        &boot,
        planner_identity(&boot),
        replace("杂项", "# 杂项\n\nz\n"),
    )
    .await
    .expect_err("undeclared");
    assert_eq!(err.code, INVALID_PARAMS, "{err:?}");
    assert!(
        err.message
            .contains("its contract declares:\n  # 概要\n  # 待你定\n  # 已完成\n  # 决策"),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_section_replace_may_not_reach_outside_its_section() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    let text = read_sections(&boot, planner_identity(&boot), &["已完成", "决策"]).await;
    let before = payload(&boot).await;
    let (outside, _) = section_blocks(&boot, "# 决策").await.remove(0);
    for (markdown, needle) in [
        (
            format!("# 已完成\n\nb\n<!-- neige:{outside} -->\n## 转移\n"),
            format!("marker `<!-- neige:{outside} -->` names a block outside section `已完成`"),
        ),
        (
            text.clone(),
            "must start with the line `# 已完成` and hold no other H1".to_string(),
        ),
        (
            "## 已完成\n".to_string(),
            "must start with the line `# 已完成`".to_string(),
        ),
    ] {
        let err = commit(&boot, planner_identity(&boot), replace("已完成", &markdown))
            .await
            .expect_err("out of bounds");
        assert_eq!(err.code, INVALID_PARAMS, "{markdown}: {err:?}");
        assert!(err.message.contains(&needle), "{markdown}: {err:?}");
    }
    assert_eq!(payload(&boot).await.body, before.body);
}

async fn read_full(boot: &Boot) {
    call_tool(boot, TOOL_REPORT_READ, planner_identity(boot), json!({}))
        .await
        .expect("full read");
}

#[tokio::test]
async fn a_section_deleted_then_read_again_is_created_by_a_replace() {
    let boot = boot().await;
    seed(
        &boot,
        &SECTIONS.replace("# 已完成", "# 待你定\n\nq0\n\n# 已完成"),
    )
    .await;
    read_full(&boot).await;
    commit(
        &boot,
        planner_identity(&boot),
        json!({ "ops": [{ "op": "delete", "section": "待你定" }] }),
    )
    .await
    .expect("delete");
    read_full(&boot).await;
    commit(
        &boot,
        planner_identity(&boot),
        replace("待你定", "# 待你定\n\nq1\n"),
    )
    .await
    .expect("a section gone at the latest read is created, not a stale read");
    assert!(
        payload(&boot)
            .await
            .body
            .contains("a2\n\n# 待你定\n\nq1\n# 已完成\n")
    );
}

#[tokio::test]
async fn a_section_another_writer_removed_is_a_conflict_until_the_report_is_read_again() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    read_full(&boot).await;
    let (id, rev) = section_blocks(&boot, "# 决策").await.remove(0);
    call_tool(
        &boot,
        calm_server::mcp_server::tools::track_report_blocks::TOOL_REPORT_BLOCKS_DELETE,
        assistant_identity(&boot),
        json!({ "id": id, "if_rev": rev }),
    )
    .await
    .expect("the other writer deletes the section");
    let delete = || json!({ "ops": [{ "op": "delete", "section": "决策" }] });
    let err = commit(&boot, planner_identity(&boot), delete())
        .await
        .expect_err("stale");
    assert_eq!(err.code, RPC_REV_CONFLICT, "{err:?}");
    assert!(
        err.message.contains("section `决策` was removed"),
        "{err:?}"
    );
    read_full(&boot).await;
    let err = commit(&boot, planner_identity(&boot), delete())
        .await
        .expect_err("gone");
    assert_eq!(err.code, INVALID_PARAMS, "{err:?}");
    assert!(
        err.message
            .ends_with("unknown section `决策`; this report's sections are:\n  # 概要\n  # 已完成"),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_cat_read_does_not_anchor_a_summary_over_one_it_never_showed() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    let doc_rev = payload(&boot).await.doc_rev;
    call_tool(
        &boot,
        TOOL_REPORT_WRITE_MARKDOWN,
        assistant_identity(&boot),
        json!({ "body": payload(&boot).await.body, "summary": "用户的", "if_doc_rev": doc_rev }),
    )
    .await
    .expect("another writer sets the summary");
    call_tool(
        &boot,
        TOOL_TRACK_CAT,
        planner_identity(&boot),
        json!({ "path": "report.md", "sections": ["决策"] }),
    )
    .await
    .expect("cat --sections");
    let err = commit(&boot, planner_identity(&boot), json!({ "summary": "mine" }))
        .await
        .expect_err("cat shows no docRev");
    assert_eq!(err.code, INVALID_PARAMS, "{err:?}");
    assert!(
        err.message.contains("has not read the report's docRev"),
        "{err:?}"
    );
    assert_eq!(payload(&boot).await.summary, "用户的");
}

#[tokio::test]
async fn an_own_commit_counts_as_read_but_another_writer_in_between_does_not() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    read_sections(&boot, planner_identity(&boot), &["概要"]).await;
    for text in ["# 概要\n\nv1\n\n## 细节\n\nx\n", "# 概要\n\nv2\n"] {
        commit(&boot, planner_identity(&boot), replace("概要", text))
            .await
            .expect("no re-read between own writes");
    }
    assert!(payload(&boot).await.body.contains("# 概要\n\nv2\n# 已完成"));

    assistant_edits(&boot, "# 概要", "# 概要\n\nby assistant\n").await;
    let err = commit(
        &boot,
        planner_identity(&boot),
        replace("概要", "# 概要\n\nv3\n"),
    )
    .await
    .expect_err("another writer's change stays a conflict");
    assert_eq!(err.code, RPC_REV_CONFLICT, "{err:?}");
}

#[tokio::test]
async fn a_block_this_session_created_is_editable_by_id_without_a_rev() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    read_full(&boot).await;
    let out = commit(
        &boot,
        planner_identity(&boot),
        json!({ "ops": [{ "op": "upsert", "kind": "prose", "markdown": "# 附录\n\nz\n" }] }),
    )
    .await
    .expect("create");
    let id = out["blocks"].as_array().unwrap().last().unwrap()["id"].clone();
    commit(
        &boot,
        planner_identity(&boot),
        json!({ "ops": [{ "op": "upsert", "id": id, "kind": "prose", "markdown": "# 附录\n\nz2\n" }] }),
    )
    .await
    .expect("the created block counts as read");
    assert!(payload(&boot).await.body.ends_with("# 附录\n\nz2\n"));
}

#[tokio::test]
async fn own_writes_keep_the_docrev_anchor_but_a_foreign_one_breaks_it() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    read_full(&boot).await;
    let (id, _) = section_blocks(&boot, "# 决策").await.remove(0);
    let own_upsert = |markdown: &str| json!({ "ops": [{ "op": "upsert", "id": id, "kind": "prose", "markdown": markdown }] });
    commit(&boot, planner_identity(&boot), own_upsert("# 决策\n\nd1\n"))
        .await
        .expect("own upsert");
    commit(&boot, planner_identity(&boot), json!({ "summary": "s1" }))
        .await
        .expect("no foreign write since the read: the docRev anchor advanced with the own write");

    assistant_edits(&boot, "# 概要", "# 概要\n\nforeign\n").await;
    commit(&boot, planner_identity(&boot), own_upsert("# 决策\n\nd2\n"))
        .await
        .expect("an unrelated block write needs no doc anchor");
    let err = commit(&boot, planner_identity(&boot), json!({ "summary": "s2" }))
        .await
        .expect_err("a foreign write happened since the read");
    assert_eq!(err.code, RPC_REV_CONFLICT, "{err:?}");
    assert_eq!(payload(&boot).await.summary, "s1");
}

#[tokio::test]
async fn a_block_an_own_replace_dropped_is_not_read_any_more() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    read_sections(&boot, planner_identity(&boot), &["概要"]).await;
    let (dropped, _) = section_blocks(&boot, "# 概要").await.remove(1);
    commit(
        &boot,
        planner_identity(&boot),
        replace("概要", "# 概要\n\nonly\n"),
    )
    .await
    .expect("replace drops the H2 block");
    let err = commit(
        &boot,
        planner_identity(&boot),
        json!({ "ops": [{ "op": "upsert", "id": dropped, "kind": "prose", "markdown": "## x\n" }] }),
    )
    .await
    .expect_err("a dropped id is no longer this session's read");
    assert_eq!(err.code, INVALID_PARAMS, "{err:?}");
    assert!(
        err.message.contains("has not been read by this session"),
        "{err:?}"
    );
}

/// Another writer appends `markdown` as a new block; its id is minted from content and position.
async fn assistant_appends(boot: &Boot, markdown: &str) -> (String, u64) {
    let doc_rev = payload(boot).await.doc_rev;
    let out = call_tool(
        boot,
        TOOL_REPORT_BLOCKS_UPSERT,
        assistant_identity(boot),
        json!({ "kind": "prose", "markdown": markdown, "if_doc_rev": doc_rev }),
    )
    .await
    .expect("the other writer appends");
    (
        out["id"].as_str().unwrap().to_string(),
        out["rev"].as_u64().unwrap(),
    )
}

/// Delete `id`'s block through `ops`, let another writer re-create the same content at the same
/// position (the same id at rev 1 again), then edit that id with no `if_rev`.
async fn assert_a_reminted_id_is_not_read(boot: &Boot, id: &str, rev: u64, ops: Value) {
    commit(boot, planner_identity(boot), ops)
        .await
        .expect("own delete");
    let reminted = assistant_appends(boot, "# 附录\n\nz\n").await;
    assert_eq!(
        reminted,
        (id.to_string(), rev),
        "premise: the id is minted again"
    );
    let err = commit(
        boot,
        planner_identity(boot),
        json!({ "ops": [{ "op": "upsert", "id": id, "kind": "prose", "markdown": "# 附录\n\nmine\n" }] }),
    )
    .await
    .expect_err("the re-minted block was never read by this session");
    assert_eq!(err.code, INVALID_PARAMS, "{err:?}");
    assert!(
        err.message.contains("has not been read by this session"),
        "{err:?}"
    );
}

#[tokio::test]
async fn an_id_deleted_by_an_own_op_and_minted_again_is_not_read() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    let (id, rev) = assistant_appends(&boot, "# 附录\n\nz\n").await;
    read_full(&boot).await;
    let ops = json!({ "ops": [{ "op": "delete", "id": id }] });
    assert_a_reminted_id_is_not_read(&boot, &id, rev, ops).await;
}

#[tokio::test]
async fn an_id_an_own_section_delete_removed_and_minted_again_is_not_read() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    let (id, rev) = assistant_appends(&boot, "# 附录\n\nz\n").await;
    read_full(&boot).await;
    let ops = json!({ "ops": [{ "op": "delete", "section": "附录" }] });
    assert_a_reminted_id_is_not_read(&boot, &id, rev, ops).await;
}

#[tokio::test]
async fn an_explicit_doc_anchor_past_the_read_does_not_advance_the_read() {
    let boot = boot().await;
    seed(&boot, SECTIONS).await;
    read_full(&boot).await;
    let body = payload(&boot).await.body;
    let doc_rev = payload(&boot).await.doc_rev;
    call_tool(
        &boot,
        TOOL_REPORT_WRITE_MARKDOWN,
        assistant_identity(&boot),
        json!({ "body": body, "summary": "用户的", "if_doc_rev": doc_rev }),
    )
    .await
    .expect("another writer sets the summary");
    let current = payload(&boot).await.doc_rev;
    let mut edit = replace("决策", "# 决策\n\nd1\n");
    edit["if_doc_rev"] = json!(current);
    commit(&boot, planner_identity(&boot), edit)
        .await
        .expect("an explicit current docRev is checked as given");
    let err = commit(&boot, planner_identity(&boot), json!({ "summary": "mine" }))
        .await
        .expect_err("the session never read the other writer's summary");
    assert_eq!(err.code, RPC_REV_CONFLICT, "{err:?}");
    assert_eq!(payload(&boot).await.summary, "用户的");
}
