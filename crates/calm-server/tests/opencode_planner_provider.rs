//! Production REST/MCP integration with an offline native OpenCode protocol executable.
//! Each boot has a private database/profile/marker scope; no real provider is contacted.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use calm_server::auth::Principal;
use calm_server::claude_planner::stop::{MARKER_KEY, MarkerInstance, sigkill_verified_for_test};
use calm_server::config::Config;
use calm_server::db::prelude::*;
use calm_server::model::{CardRole, NewArea};
use calm_server::proc_identity::read_proc_start_time;
use calm_server::routes;
use calm_server::session_projection_repo::{AgentProvider, WorkerSessionProjection};
use calm_server::state::AppState;
use clap::Parser as _;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tower::ServiceExt;

const FAKE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/opencode_planner_fake/opencode.py"
);
const BUDGET: Duration = Duration::from_secs(35);

struct Root(tempfile::TempDir);

impl Root {
    fn new(mode: &str) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let root = Self(tempfile::tempdir().unwrap());
        std::fs::create_dir_all(root.path().join("data")).unwrap();
        std::fs::create_dir_all(root.fake()).unwrap();
        let executable = root.fake().join("opencode");
        std::fs::copy(FAKE, &executable).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        root.mode(mode);
        std::fs::write(
            root.path().join("opencode.json"),
            json!({
                "opencode_binary": executable,
                "opencode_version": "1.18.34",
                "config_dir": root.path().join("profile"),
            })
            .to_string(),
        )
        .unwrap();
        root
    }

    fn path(&self) -> &Path {
        self.0.path()
    }

    fn fake(&self) -> PathBuf {
        self.path().join("fake")
    }

    fn mode(&self, mode: &str) {
        std::fs::write(self.fake().join("scenario"), mode).unwrap();
    }

    fn records(&self, name: &str) -> Vec<Value> {
        std::fs::read_to_string(self.fake().join(name))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn requests(&self) -> Vec<Value> {
        self.records("requests.jsonl")
    }

    fn last_token(&self) -> String {
        self.records("spawns.jsonl")
            .iter()
            .rev()
            .find_map(|spawn| {
                spawn
                    .pointer("/settings/mcp/calm/environment/NEIGE_MCP_TOKEN")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .expect("managed server received its minted token")
    }

    fn marker_prefix(&self) -> String {
        MarkerInstance::for_data_dir(&self.path().join("data"))
            .unwrap()
            .marker("")
    }

    fn live_processes(&self) -> Vec<(i32, u64)> {
        let prefix = format!("{MARKER_KEY}={}", self.marker_prefix());
        std::fs::read_dir("/proc")
            .unwrap()
            .flatten()
            .filter_map(|entry| {
                let pid = entry.file_name().to_str()?.parse().ok()?;
                let start = read_proc_start_time(pid)?;
                let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
                if stat.rsplit_once(')')?.1.trim_start().starts_with('Z') {
                    return None;
                }
                let env = std::fs::read(format!("/proc/{pid}/environ")).ok()?;
                env.split(|byte| *byte == 0)
                    .any(|value| value.starts_with(prefix.as_bytes()))
                    .then_some((pid, start))
            })
            .collect()
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        for (pid, start) in self.live_processes() {
            sigkill_verified_for_test(pid, start);
        }
    }
}

struct Stack {
    state: AppState,
    app: axum::Router,
}

impl Stack {
    async fn boot(root: &Root) -> Self {
        let mut config = Config::parse_from([
            "calm-server".to_owned(),
            "--opencode-planner-config".to_owned(),
            root.path().join("opencode.json").display().to_string(),
        ]);
        config.db_url = format!(
            "sqlite://{}?mode=rwc",
            root.path().join("calm.db").display()
        );
        config.data_dir = Some(root.path().join("data"));
        config.plugins_dir = Some(root.path().join("plugins"));
        config.plugins_data_dir = Some(root.path().join("plugin-data"));
        config.workspace_root = Some(root.path().join("workspaces"));
        config.codex_bin = root.path().join("absent-codex").display().to_string();
        config.claude_bin = root.path().join("absent-claude").display().to_string();
        let state = AppState::boot(&config).await.expect("production boot");
        assert!(state.mcp_server.is_some(), "production MCP listener booted");
        assert!(!state.shared_codex_appserver.is_running());
        calm_server::recover_harnesses_after_daemon_boot(
            &state,
            Err(calm_server::error::CalmError::Internal(
                "fixture Codex is absent".into(),
            )),
        )
        .await
        .expect("production independent-provider recovery");
        let app = routes::router()
            .layer(axum::middleware::from_fn(
                calm_server::actor::actor_middleware,
            ))
            .layer(axum::middleware::from_fn(owner))
            .with_state(state.clone());
        Self { state, app }
    }

    fn repo(&self) -> &dyn Repo {
        self.state.raw_repo()
    }

    async fn request(&self, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        let builder = Request::builder()
            .method(method)
            .uri(path)
            .header("x-calm-actor", "user");
        let request = match body {
            Some(value) => builder
                .header("content-type", "application/json")
                .body(Body::from(value.to_string())),
            None => builder.body(Body::empty()),
        }
        .unwrap();
        let response = self.app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn create(&self) -> (String, String) {
        let area = self
            .repo()
            .area_create(NewArea {
                name: "OpenCode integration".into(),
                color: "#111111".into(),
                sort: None,
            })
            .await
            .unwrap();
        let (status, body) = self.request("POST", "/api/tracks", Some(json!({
            "planner_provider": "opencode", "area_id": area.id,
            "title": "offline OpenCode Planner", "model": "fixture/model-b", "reasoning_effort": "large",
            "theme": {"fg": [216,219,226], "bg": [15,20,24]},
        }))).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let track = body["id"].as_str().unwrap().to_owned();
        let card = self
            .repo()
            .cards_by_track(&track)
            .await
            .unwrap()
            .into_iter()
            .find(|card| self.state.write().verify_role(&card.id) == Some(CardRole::Planner))
            .expect("Planner card");
        assert_eq!(card.kind, "codex", "shared Planner card contract");
        assert_eq!(card.payload["planner_provider"], "opencode");
        (track, card.id.to_string())
    }

    async fn runtime(&self, card: &str) -> WorkerSessionProjection {
        self.repo()
            .session_projection_active_for_card(&card.to_owned())
            .await
            .unwrap()
            .unwrap()
    }

    async fn input(&self, card: &str, text: &str) {
        let (status, body) = self
            .request(
                "POST",
                &format!("/api/cards/{card}/planner/input"),
                Some(json!({"text": text})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(
            body["entry_id"].is_string(),
            "stable queued input identity: {body}"
        );
    }

    async fn run(&self, card: &str) -> Value {
        let (status, body) = self
            .request("GET", &format!("/api/cards/{card}/planner/run"), None)
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }

    async fn rows(&self, card: &str) -> Vec<Value> {
        let (status, body) = self
            .request(
                "GET",
                &format!("/api/cards/{card}/harness/items?limit=500"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body.as_array().unwrap().clone()
    }

    async fn transcript(&self, card: &str) -> Vec<Value> {
        self.rows(card)
            .await
            .iter()
            .map(|row| serde_json::from_str(row["params"].as_str().unwrap()).unwrap())
            .collect()
    }

    async fn outcomes(&self, card: &str) -> Vec<Value> {
        self.rows(card)
            .await
            .iter()
            .filter(|row| row["method"] == "turn/completed")
            .map(|row| serde_json::from_str(row["params"].as_str().unwrap()).unwrap())
            .collect()
    }

    async fn unknown(&self, card: &str) -> bool {
        let pool = self.repo().sqlite_pool().unwrap();
        calm_server::db::sqlite::opencode_submission_get_unresolved_by_card(&pool, card)
            .await
            .unwrap()
            .is_some_and(|submission| {
                submission.state
                    == calm_truth::opencode_submission::OpenCodeSubmissionState::Unknown
            })
    }

    async fn completed(&self, card: &str, count: usize) {
        wait("native outcome projected and harness settled", || async {
            self.outcomes(card).await.len() >= count
                && self.run(card).await["phase"] == "turn_completed"
        })
        .await;
    }

    async fn reset(&self, card: &str) -> (StatusCode, Value) {
        self.request(
            "POST",
            &format!("/api/cards/{card}/planner/reset"),
            Some(json!({})),
        )
        .await
    }

    async fn authenticates(&self, token: &str) -> bool {
        let socket = self.state.opencode_planner_wiring().host.mcp_socket.clone();
        let mut stream = tokio::net::UnixStream::connect(socket).await.unwrap();
        let request = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
            "protocolVersion":"2024-11-05","capabilities":{},
            "clientInfo":{"name":"fixture-verifier","version":"0"},
            "_meta":{"dev.neige/auth":{"token":token}}
        }});
        stream
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let mut reply = String::new();
        BufReader::new(stream).read_line(&mut reply).await.unwrap();
        let reply: Value = serde_json::from_str(&reply).unwrap();
        reply.get("result").is_some() && reply.get("error").is_none()
    }

    async fn shutdown(self) {
        for harness in self.state.harness.drain_all_for_dev() {
            harness.shutdown().await.expect("quiesce owned harness");
        }
        self.state
            .mcp_server
            .as_ref()
            .unwrap()
            .stop_listener_for_test()
            .await;
    }
}

async fn owner(
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    request.extensions_mut().insert(Principal {
        user_id: "owner".into(),
        display_name: "owner".into(),
        role: "owner".into(),
        session_id: "opencode-rest-fixture".into(),
    });
    next.run(request).await
}

async fn wait<F, Fut>(description: &str, mut ready: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + BUDGET;
    while !ready().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out: {description}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opencode_rest_catalog_turns_recovery_reset_and_delete() {
    let root = Root::new("complete");
    let stack = Stack::boot(&root).await;
    let (status, providers) = stack.request("GET", "/api/agent-providers", None).await;
    assert_eq!(status, StatusCode::OK);
    let providers = providers.as_array().unwrap();
    assert_eq!(providers.len(), 3);
    assert_eq!(
        providers
            .iter()
            .find(|p| p["provider"] == "opencode")
            .unwrap()["status"],
        "ready"
    );
    assert_eq!(
        providers.iter().find(|p| p["provider"] == "codex").unwrap()["status"],
        "unavailable"
    );
    let (status, catalog) = stack
        .request("GET", "/api/models?provider=opencode", None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(catalog["source"], "live");
    assert_eq!(catalog["default_source"], "opencode_config");
    assert_eq!(catalog["default"]["model"], "fixture/model-a");
    assert_eq!(catalog["models"].as_array().unwrap().len(), 2);
    let (track, card) = stack.create().await;
    let before = stack.runtime(&card).await;
    assert_eq!(before.agent_provider, Some(AgentProvider::OpenCode));
    assert!(before.session_id.is_none(), "native session is lazy");
    stack.input(&card, "host memory please").await;
    stack.completed(&card, 1).await;
    let first = root.requests();
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0]["payload"]["model"],
        json!({"providerID":"fixture","modelID":"model-b"})
    );
    assert_eq!(first[0]["payload"]["variant"], "large");
    assert!(
        first[0]["payload"]["system"]
            .as_str()
            .is_some_and(|s| !s.is_empty())
    );
    let native = first[0]["session"].clone();
    let rows = stack.transcript(&card).await;
    assert!(
        rows.iter().any(|row| row["item"]["type"] == "agentMessage"
            && row["item"]["text"] == "memory fixture answer"),
        "{rows:?}"
    );
    assert!(
        rows.iter()
            .any(|row| row["item"]["type"] == "commandExecution"
                && row["item"]["command"] == "free -b"
                && row["item"]["aggregatedOutput"] == "memory fixture output"),
        "{rows:?}"
    );
    assert!(
        rows.iter().any(|row| row["item"]["type"] == "mcpToolCall"
            && row["item"]["tool"] == "calm.user.notify"
            && row["item"]["arguments"]["text"] == "native MCP notification"),
        "{rows:?}"
    );
    assert_eq!(
        root.records("mcp.jsonl").len(),
        2,
        "real listener accepted initialize and tools/call"
    );
    let old_token = root.last_token();
    assert!(stack.authenticates(&old_token).await);

    let (status, model) = stack
        .request(
            "PUT",
            &format!("/api/cards/{card}/planner/model"),
            Some(json!({"model":null,"reasoning_effort":null})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{model}");
    stack
        .input(&card, "second turn using installation default")
        .await;
    stack.completed(&card, 2).await;
    let requests = root.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1]["session"], native);
    assert_ne!(
        requests[1]["payload"]["messageID"],
        requests[0]["payload"]["messageID"]
    );
    assert_eq!(
        requests[1]["payload"]["model"],
        json!({"providerID":"fixture","modelID":"model-a"})
    );

    // A fresh boot's REST reader gets the same persisted output before another input.
    let persisted = stack.rows(&card).await;
    let old_token = root.last_token();
    assert!(stack.authenticates(&old_token).await);
    stack.shutdown().await;
    let stack = Stack::boot(&root).await;
    assert!(
        !stack.authenticates(&old_token).await,
        "boot revoked old credential"
    );
    assert_eq!(
        stack.rows(&card).await,
        persisted,
        "refresh preserves transcript"
    );
    assert!(
        stack
            .transcript(&card)
            .await
            .iter()
            .any(|row| row["item"]["text"] == "memory fixture answer")
    );
    stack.input(&card, "after restart").await;
    stack.completed(&card, 3).await;
    assert_eq!(root.requests()[2]["session"], native);
    let old_runtime = stack.runtime(&card).await;
    let retired_token = root.last_token();
    let (status, body) = stack.reset(&card).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        !stack.authenticates(&retired_token).await,
        "reset revoked predecessor"
    );
    let replacement = stack.runtime(&card).await;
    assert_ne!(replacement.id, old_runtime.id);
    assert_ne!(replacement.thread_id, old_runtime.thread_id);
    stack.input(&card, "new native session after reset").await;
    stack.completed(&card, 1).await;
    assert_eq!(root.requests().len(), 4);
    assert_ne!(root.requests()[3]["session"], native);
    let token = root.last_token();
    assert!(stack.authenticates(&token).await);
    let (status, body) = stack
        .request("DELETE", &format!("/api/tracks/{track}"), None)
        .await;
    assert!(status.is_success(), "{status} {body}");
    assert!(
        !stack.authenticates(&token).await,
        "delete revoked credential"
    );
    wait("delete quiesced owned processes", || async {
        root.live_processes().is_empty()
    })
    .await;
    assert!(
        stack
            .repo()
            .session_projection_active_for_card(&card)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!stack.state.shared_codex_appserver.is_running());
    stack.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opencode_rest_busy_reset_is_refused_then_stop_allows_reset() {
    let root = Root::new("busy");
    let stack = Stack::boot(&root).await;
    let (track, card) = stack.create().await;
    stack.input(&card, "keep running").await;
    wait("running native turn", || async {
        root.requests().len() == 1 && stack.run(&card).await["phase"] == "turn_running"
    })
    .await;
    let original = stack.runtime(&card).await;
    let (status, body) = stack.reset(&card).await;
    assert_eq!(status, StatusCode::CONFLICT, "busy reset must fail: {body}");
    assert_eq!(stack.runtime(&card).await.id, original.id);
    assert_eq!(root.requests().len(), 1);
    let (status, body) = stack
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/interrupt"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["stopped"], true);
    stack.completed(&card, 1).await;
    assert_eq!(stack.outcomes(&card).await[0]["status"], "interrupted");
    wait("stop quiesced native tools", || async {
        root.live_processes().is_empty()
    })
    .await;
    assert!(
        !root.records("children.jsonl").is_empty(),
        "busy native tool had a real marked child"
    );
    let (status, body) = stack.reset(&card).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_ne!(stack.runtime(&card).await.id, original.id);
    assert_eq!(root.requests().len(), 1, "reset itself sends no prompt");
    let (status, body) = stack
        .request("DELETE", &format!("/api/tracks/{track}"), None)
        .await;
    assert!(status.is_success(), "{status} {body}");
    stack.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opencode_rest_response_loss_reboot_and_retry_never_repeat_post() {
    let root = Root::new("loss");
    let stack = Stack::boot(&root).await;
    let (track, card) = stack.create().await;
    stack.input(&card, "ambiguous operation").await;
    wait("lost response retained as unknown", || async {
        stack.unknown(&card).await && stack.run(&card).await["blocked_reason"].is_string()
    })
    .await;
    assert_eq!(root.requests().len(), 1);
    assert!(stack.outcomes(&card).await.is_empty());
    let original = stack.runtime(&card).await;
    let (status, body) = stack.reset(&card).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "unknown reset must fail: {body}"
    );
    assert_eq!(stack.runtime(&card).await.id, original.id);
    let token = root.last_token();
    stack.shutdown().await;
    let stack = Stack::boot(&root).await;
    assert!(!stack.authenticates(&token).await);
    wait("recovered unknown notice", || async {
        stack.unknown(&card).await && stack.run(&card).await["blocked_reason"].is_string()
    })
    .await;
    stack.input(&card, "ambiguous operation").await;
    let (status, body) = stack.reset(&card).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "recovery cannot bypass unknown: {body}"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        root.requests().len(),
        1,
        "restart, retry and reset must not replay POST"
    );
    assert!(
        stack.outcomes(&card).await.is_empty(),
        "no native terminal evidence"
    );
    assert_eq!(
        stack.run(&card).await["pending"].as_array().unwrap().len(),
        1,
        "new input stays queued"
    );
    let (status, body) = stack
        .request("DELETE", &format!("/api/tracks/{track}"), None)
        .await;
    assert!(status.is_success(), "{status} {body}");
    wait("delete stopped unknown process", || async {
        root.live_processes().is_empty()
    })
    .await;
    assert_eq!(root.requests().len(), 1);
    stack.shutdown().await;
}
