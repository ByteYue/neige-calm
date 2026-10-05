//! Existing-session acceptance through production boot and HTTP entry points. The
//! native loopback fixture records all effects independently of Neige projections.
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use calm_server::{
    auth::Principal,
    config::Config,
    model::{NewArea, NewTrack},
    routes,
    state::AppState,
};
use clap::Parser;
use http_body_util::BodyExt;
use native_fixture::{ordered_messages, persisted_user};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tower::ServiceExt;
const SESSION: &str = "ses_existing";
#[derive(Clone)]
struct Native(Arc<Mutex<NativeState>>);
struct NativeState {
    directory: PathBuf,
    messages: Vec<Value>,
    busy: bool,
    lost: bool,
    hold: bool,
    invalid_cursor: bool,
    reply: Option<returns_tests::Reply>,
    release: Arc<tokio::sync::Notify>,
    version: &'static str,
    requests: Vec<Value>,
}
struct Fixture {
    root: tempfile::TempDir,
    native: Native,
    port: u16,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn user(id: &str, text: &str, at: i64) -> Value {
    json!({"info":{"id":id,"sessionID":SESSION,"role":"user","agent":"audit","model":{"providerID":"fixture","modelID":"original-model","variant":"max"},"time":{"created":at}},"parts":[{"id":format!("prt_{id}"),"type":"text","text":text}]})
}
fn assistant(id: &str, parent: &str, text: &str, at: i64, done: bool) -> Value {
    let mut message = json!({"info":{"id":id,"sessionID":SESSION,"parentID":parent,"role":"assistant","providerID":"fixture","modelID":"original-model","time":{"created":at},"tokens":{"input":3,"output":4}},"parts":[{"id":format!("prt_{id}"),"type":"text","text":text,"time":{"start":at}}]});
    if done {
        message["info"]["time"]["completed"] = json!(at + 1);
        message["info"]["finish"] = json!("stop");
        message["parts"][0]["time"]["end"] = json!(at + 1);
    }
    message
}
impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("original-workspace");
        std::fs::create_dir_all(&directory).unwrap();
        let native = Native(Arc::new(Mutex::new(NativeState {
            directory,
            messages: vec![
                user("msg_original", "old request", 1),
                assistant("msg_reply", "msg_original", "original progress", 2, true),
            ],
            busy: false,
            lost: false,
            hold: false,
            invalid_cursor: false,
            reply: None,
            release: Arc::new(tokio::sync::Notify::new()),
            version: "1.18.34",
            requests: vec![],
        })));
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let app = Router::new()
            .route("/{*path}", any(native_request))
            .with_state(native.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let fixture = Self {
            root,
            native,
            port,
            task,
        };
        fixture.write_config(1);
        fixture
    }
    fn path(&self) -> &Path {
        self.root.path()
    }
    fn write_config(&self, generation: u64) {
        std::fs::write(self.path().join("password"), "fixture-password\n").unwrap();
        std::fs::write(self.path().join("connections.json"),json!({"connections":[{"id":"original","label":"Original operations","generation":generation,"directory":self.native.0.lock().unwrap().directory,"port":self.port,"password_file":self.path().join("password")}]}).to_string()).unwrap();
    }
    fn requests(&self) -> Vec<Value> {
        self.native.0.lock().unwrap().requests.clone()
    }
    fn posts(&self) -> Vec<Value> {
        self.requests()
            .into_iter()
            .filter(|request| request["method"] != "GET")
            .collect()
    }
}
async fn native_request(State(native): State<Native>, request: Request) -> Response {
    let method = request.method().to_string();
    let uri = request.uri().clone();
    let path = uri.path().to_string();
    let authorized = request
        .headers()
        .get("authorization")
        .and_then(|header| header.to_str().ok())
        == Some("Basic b3BlbmNvZGU6Zml4dHVyZS1wYXNzd29yZA==");
    let bytes = to_bytes(request.into_body(), 1024 * 1024).await.unwrap();
    let input: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    let query: std::collections::HashMap<_, _> =
        url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
    if method == "POST" && path == format!("/session/{SESSION}/message") {
        let reply = native.0.lock().unwrap().reply;
        if let Some(reply) = reply {
            return returns_tests::reply(native, reply, input, authorized, query).await;
        }
    }
    let mut state = native.0.lock().unwrap();
    state.requests.push(
        json!({"method":method,"path":path,"input":input,"directory":query.get("directory")}),
    );
    if !authorized || query.get("directory").map(String::as_str) != state.directory.to_str() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let not_found = || {
        (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"error":"native target missing"})),
        )
            .into_response()
    };
    if path == "/global/health" {
        return axum::Json(json!({"healthy":true,"version":state.version})).into_response();
    }
    if path == "/session/status" {
        return axum::Json(if state.busy {
            json!({(SESSION):{"type":"busy"}})
        } else {
            json!({})
        })
        .into_response();
    }
    if path == format!("/session/{SESSION}") {
        return axum::Json(json!({"id":SESSION,"directory":state.directory,"title":"Existing audit","agent":"audit","model":{"id":"original-model","providerID":"fixture","variant":"max"}})).into_response();
    }
    if path == "/provider" {
        return axum::Json(json!({"connected":["fixture"],"all":[{"id":"fixture","models":{"original-model":{"name":"Original","variants":{"max":{}}},"wrong-default":{"name":"Different"}}}]})).into_response();
    }
    if path == "/config" {
        return axum::Json(json!({"model":"fixture/wrong-default","default_agent":"different","agent":{"audit":{"model":"fixture/wrong-default"}}})).into_response();
    }
    if path == format!("/session/{SESSION}/message") && method == "POST" {
        let id = input["messageID"].as_str().unwrap();
        let at = state.messages.len() as i64 + 10;
        if state.lost {
            state.busy = true;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error":"response lost after execution"})),
            )
                .into_response();
        }
        state.messages.push(persisted_user(&input, at));
        let result = assistant(
            &format!("msg_reply_{}", state.messages.len()),
            id,
            "fresh audit progress",
            at + 1,
            !state.hold,
        );
        if state.hold {
            state.busy = true;
        }
        state.messages.push(result.clone());
        return axum::Json(result).into_response();
    }
    if path == format!("/session/{SESSION}/message") {
        let messages = ordered_messages(&state.messages);
        if state.invalid_cursor {
            let mut headers = HeaderMap::new();
            headers.insert("x-next-cursor", "same".parse().unwrap());
            return (headers, axum::Json(messages.clone())).into_response();
        }
        if messages.len() > 2 && !query.contains_key("before") {
            let mut headers = HeaderMap::new();
            headers.insert("x-next-cursor", "older".parse().unwrap());
            return (headers, axum::Json(messages[2..].to_vec())).into_response();
        }
        return axum::Json(if query.contains_key("before") {
            messages[..2].to_vec()
        } else {
            messages.clone()
        })
        .into_response();
    }
    if let Some(id) = path.strip_prefix(&format!("/session/{SESSION}/message/")) {
        return state
            .messages
            .iter()
            .find(|message| message["info"]["id"].as_str() == Some(id))
            .cloned()
            .map(|message| axum::Json(message).into_response())
            .unwrap_or_else(not_found);
    }
    not_found()
}
struct Stack {
    state: AppState,
    app: Router,
}
impl Stack {
    async fn boot(fixture: &Fixture) -> Self {
        let mut cfg = Config::parse_from([
            "calm-server",
            "--opencode-connections-config",
            fixture.path().join("connections.json").to_str().unwrap(),
        ]);
        cfg.db_url = format!(
            "sqlite://{}?mode=rwc",
            fixture.path().join("neige.db").display()
        );
        cfg.data_dir = Some(fixture.path().join("data"));
        cfg.workspace_root = Some(fixture.path().join("workspaces"));
        cfg.plugins_dir = Some(fixture.path().join("plugins"));
        cfg.plugins_data_dir = Some(fixture.path().join("plugin-data"));
        cfg.codex_bin = fixture.path().join("absent-codex").display().to_string();
        let state = AppState::boot(&cfg).await.unwrap();
        assert!(!state.shared_codex_appserver.is_running());
        calm_server::recover_harnesses_after_daemon_boot(
            &state,
            Err(calm_server::error::CalmError::Internal(
                "offline Codex fixture".into(),
            )),
        )
        .await
        .unwrap();
        let app = routes::router()
            .layer(axum::middleware::from_fn(
                calm_server::actor::actor_middleware,
            ))
            .layer(axum::middleware::from_fn(owner))
            .with_state(state.clone());
        Self { state, app }
    }
    async fn track(&self, fixture: &Fixture) -> String {
        let workspace = fixture
            .path()
            .join(format!("neige-track-{}", calm_server::model::new_id()));
        std::fs::create_dir_all(&workspace).unwrap();
        let area = self
            .state
            .raw_repo()
            .area_create(NewArea {
                name: "Operations".into(),
                color: "#111111".into(),
                sort: None,
            })
            .await
            .unwrap();
        let track = self
            .state
            .raw_repo()
            .track_create(NewTrack {
                area_id: area.id,
                title: "Operations".into(),
                sort: None,
                cwd: workspace.display().to_string(),
                template_id: None,
                plugin_scope: None,
                template_input: None,
                attach_folder: true,
                theme: calm_server::routes::theme::RequestTheme::default_dark(),
            })
            .await
            .unwrap();
        track.id.to_string()
    }
    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        key: Option<&str>,
    ) -> (StatusCode, Value) {
        let mut builder = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header("x-calm-actor", "user");
        if let Some(key) = key {
            builder = builder.header("idempotency-key", key);
        }
        let request = if let Some(body) = body {
            builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        } else {
            builder.body(Body::empty()).unwrap()
        };
        let response = self.app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }
    async fn attach(&self, track: &str, key: &str) -> String {
        let (status, body) = self
            .request(
                "POST",
                &format!("/api/tracks/{track}/opencode-conversations"),
                Some(json!({"connection_id":"original","session_id":SESSION})),
                Some(key),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["kind"], "track-opencode");
        body["id"].as_str().unwrap().to_owned()
    }
    async fn run(&self, card: &str) -> Value {
        let (status, value) = self
            .request("GET", &format!("/api/cards/{card}/planner/run"), None, None)
            .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        value
    }
    async fn items(&self, card: &str) -> Value {
        let (status, value) = self
            .request(
                "GET",
                &format!("/api/cards/{card}/harness/items?limit=500"),
                None,
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        value
    }
    async fn wait_text(&self, card: &str, text: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if self.items(card).await.to_string().contains(text) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Missing history {text}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    async fn wait_submit(&self, card: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if self.run(card).await["attached_session"]["can_submit"] == true {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Attachment cannot submit: {}",
                self.run(card).await
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    async fn journals(&self) -> Vec<(String, String)> {
        sqlx::query_as(
            "SELECT native_session_id,state FROM opencode_submissions ORDER BY created_at_ms,id",
        )
        .fetch_all(&self.state.raw_repo().sqlite_pool().unwrap())
        .await
        .unwrap()
    }
    async fn shutdown(self) {
        for handle in self.state.harness.drain_all_for_dev() {
            handle.shutdown().await.unwrap();
        }
        self.state
            .mcp_server
            .as_ref()
            .unwrap()
            .stop_listener_for_test()
            .await;
        drop(self);
    }
}
async fn owner(mut request: Request, next: axum::middleware::Next) -> Response {
    request.extensions_mut().insert(Principal {
        user_id: "owner".into(),
        display_name: "owner".into(),
        role: "owner".into(),
        session_id: "existing-opencode-fixture".into(),
    });
    next.run(request).await
}
#[tokio::test]
async fn attach_imports_history_tracks_changes_and_never_submits() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let mut events = stack.state.events.subscribe();
    let card = stack.attach(&track, "attach").await;
    stack.wait_text(&card, "original progress").await;
    stack.wait_submit(&card).await;
    let run = stack.run(&card).await;
    assert_eq!(run["supports_steer"], false);
    assert_eq!(run["attached_session"]["can_stop"], false);
    assert_eq!(run["attached_session"]["session_id"], SESSION);
    assert_eq!(run["attached_session"]["model"], "fixture/original-model");
    assert!(stack.journals().await.is_empty());
    assert!(fixture.posts().is_empty());
    assert_eq!(stack.attach(&track, "attach").await, card);
    let (left, right) = tokio::join!(
        stack.attach(&track, "concurrent-left"),
        stack.attach(&track, "concurrent-right")
    );
    assert_eq!(left, card);
    assert_eq!(right, card);
    assert_eq!(stack.attach(&track, "another-key").await, card);
    let count = stack.items(&card).await.as_array().unwrap().len();
    tokio::time::sleep(Duration::from_millis(700)).await;
    let rows = stack.items(&card).await;
    assert_eq!(rows.as_array().unwrap().len(), count);
    transcript_assertions::assert_item_announcements(
        &mut events,
        &rows,
        &card,
        &track,
        transcript_assertions::ItemEventScope::BoundCard,
        transcript_assertions::ItemSelection::All,
    );
    assert_eq!(
        stack.run(&card).await["phase"],
        "idle",
        "passive history must not adopt a native turn into the Harness FSM"
    );
    fixture.native.0.lock().unwrap().messages[1]["parts"][0]["text"] =
        json!("progress changed externally");
    stack.wait_text(&card, "progress changed externally").await;
    let (status, list) = stack
        .request(
            "GET",
            &format!("/api/tracks/{track}/conversations"),
            None,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list[0]["id"], card);
    assert_eq!(list[0]["kind"], "track-opencode");
    assert!(fixture.posts().is_empty());
    stack.shutdown().await;
    assert!(fixture.posts().is_empty());
}
#[tokio::test]
async fn native_agent_model_variant_and_identity_survive_neige_restart() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "attach").await;
    stack.wait_submit(&card).await;
    for round in 0..2 {
        let (status, body) = stack
            .request(
                "POST",
                &format!("/api/cards/{card}/planner/input"),
                Some(json!({"text":format!("check progress {round}")})),
                Some(&format!("progress-{round}")),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while stack.journals().await != vec![(SESSION.into(), "completed".into()); round + 1] {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Submission did not settle: receipts={:?} posts={:?} run={}",
                stack.journals().await,
                fixture.posts(),
                stack.run(&card).await
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        stack.wait_text(&card, "fresh audit progress").await;
        stack.wait_submit(&card).await;
    }
    let posts = fixture.posts();
    assert_eq!(posts.len(), 2, "{posts:?}");
    for post in &posts {
        assert_eq!(post["path"], format!("/session/{SESSION}/message"));
        assert_eq!(post["input"]["agent"], "audit");
        assert_eq!(
            post["input"]["model"],
            json!({"providerID":"fixture","modelID":"original-model"})
        );
        assert_eq!(post["input"]["variant"], "max");
        assert!(post["input"].get("system").is_none());
    }
    assert_eq!(
        stack.journals().await,
        vec![(SESSION.into(), "completed".into()); 2]
    );
    stack.shutdown().await;
    let reboot = Stack::boot(&fixture).await;
    reboot.wait_text(&card, "original progress").await;
    reboot.wait_submit(&card).await;
    assert_eq!(
        reboot.run(&card).await["attached_session"]["session_id"],
        SESSION
    );
    assert_eq!(fixture.posts().len(), 2);
    let (status, body) = reboot
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/input"),
            Some(json!({"text":"check after reboot"})),
            Some("progress-after-reboot"),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while reboot.journals().await != vec![(SESSION.into(), "completed".into()); 3] {
        assert!(
            tokio::time::Instant::now() < deadline,
            "receipts={:?} posts={:?} run={}",
            reboot.journals().await,
            fixture.posts(),
            reboot.run(&card).await
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    reboot.wait_submit(&card).await;
    assert_eq!(fixture.posts().len(), 3);
    assert_eq!(
        reboot.journals().await,
        vec![(SESSION.into(), "completed".into()); 3]
    );
    reboot.shutdown().await;
}
#[tokio::test]
async fn running_foreign_turn_is_observed_but_never_stopped_or_sent_into() {
    let fixture = Fixture::new().await;
    {
        let mut native = fixture.native.0.lock().unwrap();
        native.busy = true;
        native.messages[1] = assistant(
            "msg_reply",
            "msg_original",
            "foreign partial output",
            2,
            false,
        );
    }
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "attach").await;
    stack.wait_text(&card, "foreign partial output").await;
    let run = stack.run(&card).await;
    assert_eq!(run["attached_session"]["status"], "running");
    assert_eq!(run["attached_session"]["can_submit"], false);
    let (status, _) = stack
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/input"),
            Some(json!({"text":"do another ETL"})),
            Some("foreign-busy-input"),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    for path in [
        format!("/api/cards/{card}/planner/interrupt"),
        format!("/api/cards/{card}/planner/reset"),
    ] {
        let (status, _) = stack.request("POST", &path, Some(json!({})), None).await;
        assert_eq!(status, StatusCode::CONFLICT, "{path}");
    }
    assert!(stack.journals().await.is_empty());
    assert!(fixture.posts().is_empty());
    let (status, body) = stack
        .request("DELETE", &format!("/api/cards/{card}"), None, None)
        .await;
    assert!(status.is_success(), "{status} {body}");
    assert!(fixture.native.0.lock().unwrap().busy);
    assert!(fixture.posts().is_empty());
    stack.shutdown().await;
    assert!(fixture.native.0.lock().unwrap().busy);
    assert!(fixture.posts().is_empty());
}
#[tokio::test]
async fn unresolved_external_submission_is_fenced_and_never_resent_on_reboot() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "attach").await;
    stack.wait_submit(&card).await;
    fixture.native.0.lock().unwrap().lost = true;
    let (status, body) = stack
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/input"),
            Some(json!({"text":"one operational side effect"})),
            Some("unresolved-intent"),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let receipts = stack.journals().await;
        if receipts.len() == 1 && receipts[0].1 == "unknown" {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{receipts:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(fixture.posts().len(), 1);
    stack.shutdown().await;
    assert_eq!(fixture.posts().len(), 1);
    let reboot = Stack::boot(&fixture).await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        reboot.run(&card).await["attached_session"]["can_submit"],
        false
    );
    let (status, _) = reboot
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/input"),
            Some(json!({"text":"try it again"})),
            Some("new-intent-after-unknown"),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(fixture.posts().len(), 1);
    let (deleted, _) = reboot
        .request("DELETE", &format!("/api/cards/{card}"), None, None)
        .await;
    assert!(deleted.is_success());
    reboot.shutdown().await;
    fixture.write_config(2);
    let path = fixture.path().join("connections.json");
    let mut repointed: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    repointed["connections"][0]["port"] = json!(if fixture.port == 65535 {
        1
    } else {
        fixture.port + 1
    });
    std::fs::write(path, repointed.to_string()).unwrap();
    let changed = Stack::boot(&fixture).await;
    let (status, _) = changed
        .request(
            "POST",
            &format!("/api/tracks/{track}/opencode-conversations"),
            Some(json!({"connection_id":"original","session_id":SESSION})),
            Some("orphan-rebind"),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "unknown receipt outlives card and generation"
    );
    assert_eq!(fixture.posts().len(), 1);
    changed.shutdown().await;
}
#[tokio::test]
async fn configuration_generation_change_retains_history_and_blocks_repoint() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "attach").await;
    stack.wait_text(&card, "original progress").await;
    stack.shutdown().await;
    fixture.write_config(2);
    let reboot = Stack::boot(&fixture).await;
    assert_eq!(
        reboot.run(&card).await["attached_session"]["can_submit"],
        false
    );
    assert!(
        reboot
            .items(&card)
            .await
            .to_string()
            .contains("original progress")
    );
    let (status, body) = reboot
        .request(
            "POST",
            &format!("/api/tracks/{track}/opencode-conversations"),
            Some(json!({"connection_id":"original","session_id":SESSION})),
            Some("new-generation"),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let (status, _) = reboot
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/input"),
            Some(json!({"text":"do not repoint"})),
            Some("changed-registration-input"),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(fixture.posts().is_empty());
    reboot.shutdown().await;
}
#[tokio::test]
async fn partial_paginated_history_cannot_claim_ready_or_repeat_native_writes() {
    let fixture = Fixture::new().await;
    {
        let mut state = fixture.native.0.lock().unwrap();
        state.messages.extend([
            user("msg_second", "second old request", 3),
            assistant(
                "msg_second_reply",
                "msg_second",
                "second historical reply",
                4,
                true,
            ),
        ]);
    }
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "attach").await;
    stack.wait_text(&card, "old request").await;
    stack.wait_text(&card, "second historical reply").await;
    stack.wait_submit(&card).await;
    fixture.native.0.lock().unwrap().invalid_cursor = true;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if stack.run(&card).await["attached_session"]["status"] == "unavailable" {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        stack.run(&card).await["attached_session"]["can_submit"],
        false
    );
    assert!(
        stack
            .items(&card)
            .await
            .to_string()
            .contains("second historical reply")
    );
    let (status, _) = stack
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/input"),
            Some(json!({"text":"do not submit with incomplete history"})),
            Some("partial-history-input"),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(stack.journals().await.is_empty());
    assert!(fixture.posts().is_empty());
    stack.shutdown().await;
}
#[path = "support/opencode_existing_capabilities.rs"]
mod capability_tests;
#[path = "support/opencode_existing_native.rs"]
mod native_fixture;
#[path = "support/opencode_existing_persistence_race.rs"]
mod persistence_race_tests;
#[path = "support/opencode_existing_recovery_checkpoint.rs"]
mod recovery_checkpoint_tests;
#[path = "support/opencode_existing_registry.rs"]
mod registry_tests;
#[path = "support/opencode_existing_returns.rs"]
mod returns_tests;
#[path = "support/opencode_existing_transcript.rs"]
mod transcript_assertions;
