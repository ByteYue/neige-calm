//! Production admission/recovery fixtures. Only the native peer is fake; the session,
//! journal CAS, attribution, MCP mint/revoke and process cleanup are the real entry points.
use super::{
    config::{OpenCodePlannerConfig, OpenCodePlannerHost, PINNED_VERSION},
    session::{OpenCodePlannerSession, OpenCodePlannerSessionParams},
};
use crate::{
    codex_appserver::{InputItem, Notification},
    db::{Repo, sqlite::*},
    model::{CardRole, NewArea, NewCard, NewTrack, RequestTheme},
    planner_model::TurnModelSelection,
    planner_submission::TurnAdmission,
    session_projection_repo::{AgentProvider, ThreadAttribution, WorkerSessionInit},
    shared_codex_appserver::SharedCodexAppServer,
};
use calm_truth::opencode_submission::{OpenCodeSubmissionIntent, OpenCodeSubmissionState};
use calm_types::worker::WorkerSessionState;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};

const PEER: &str = r#"#!/usr/bin/python3
import base64,http.server,json,os,sys,threading
if '--version' in sys.argv: print('1.18.34'); raise SystemExit(0)
port=int(sys.argv[sys.argv.index('--port')+1]); lock=threading.Lock(); store=os.path.join(os.environ['HOME'],'fixture-native.json')
with open('serve-starts','a') as started:started.write(str(os.getpid())+'\n')
mode=open('mode').read().strip() if os.path.exists('mode') else 'complete'
def load():
    return json.load(open(store)) if os.path.exists(store) else {'messages':[],'posts':[]}
def save(state):
    with open(store,'w') as file: json.dump(state,file)
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self,*args): pass
    def answer(self,value,status=200,headers={}):
        data=json.dumps(value).encode();self.send_response(status)
        for k,v in headers.items():self.send_header(k,v)
        self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def do_GET(self):
        if self.headers.get('Authorization')!='Basic '+base64.b64encode(('opencode:'+os.environ['OPENCODE_SERVER_PASSWORD']).encode()).decode(): return self.answer({},401)
        from urllib.parse import urlparse,parse_qs
        url=urlparse(self.path);path=url.path;query=parse_qs(url.query)
        if path=='/global/health': return self.answer({'healthy':True,'version':'1.18.34'})
        if path=='/provider': return self.answer({'all':[{'id':'fixture','models':{'model':{'name':'Default'},'other':{'name':'Other'}}}],'connected':['fixture']})
        if path=='/config':return self.answer({'model':'fixture/model'})
        if path in ['/permission','/question']:return self.answer([])
        if path=='/session/ses_owned': return self.answer({'id':'ses_owned','directory':os.getcwd()},404 if mode=='identity-error' else 200)
        state=load()
        if path.startswith('/session/ses_owned/message/'):
            matches=[m for m in state['messages'] if m['info']['id']==path.rsplit('/',1)[1]]
            return self.answer(matches[0] if matches else {},200 if matches else 404)
        if path=='/session/ses_owned/message':
            stop=int(query.get('before',[str(len(state['messages']))])[0]); start=max(0,stop-256)
            return self.answer(state['messages'][start:stop],headers={'X-Next-Cursor':str(start)} if start else {})
        return self.answer({},404)
    def do_POST(self):
        path=self.path.split('?')[0];body=json.loads(self.rfile.read(int(self.headers.get('Content-Length',0))) or b'{}')
        if path=='/session':return self.answer({'id':'ses_owned','directory':os.getcwd()})
        if path=='/session/ses_owned/abort':return self.answer(True)
        if path=='/session/ses_owned/message':
            with lock:
                state=load();state['posts'].append(body)
                user={'info':{'id':body['messageID'],'sessionID':'ses_owned','role':'user','time':{'created':len(state['messages'])+1}},'parts':body['parts']};state['messages'].append(user)
                if mode=='complete':
                    state['messages'].append({'info':{'id':'msg_answer_'+str(len(state['posts'])),'sessionID':'ses_owned','role':'assistant','parentID':body['messageID'],'finish':'stop','time':{'created':len(state['messages'])+1,'completed':3},'tokens':{'input':1,'output':2}},'parts':[{'id':'prt_answer_'+str(len(state['posts'])),'type':'text','text':'answer','time':{'end':3}}]})
                save(state)
            if mode=='loss':self.close_connection=True;return
            return self.answer(state['messages'][-1])
        return self.answer({},404)
server=http.server.ThreadingHTTPServer(('127.0.0.1',port),Handler)
print('opencode server listening on http://127.0.0.1:'+str(server.server_address[1]),flush=True)
server.serve_forever()
"#;

struct Fixture {
    _root: tempfile::TempDir,
    host: Arc<OpenCodePlannerHost>,
    repo: Arc<SqlxRepo>,
    card: String,
    track: String,
    cwd: PathBuf,
}
impl Fixture {
    async fn new(mode: &str) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let cwd = root.path().join("workspace");
        std::fs::create_dir(&cwd).unwrap();
        std::fs::write(cwd.join("mode"), mode).unwrap();
        let binary = root.path().join("fake-opencode");
        std::fs::write(&binary, PEER).unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        let host = Arc::new(
            OpenCodePlannerHost::new(
                Some(OpenCodePlannerConfig {
                    opencode_binary: binary,
                    opencode_version: PINNED_VERSION.into(),
                    config_dir: root.path().join("profile"),
                }),
                root.path(),
                "/fixture/shim".into(),
                root.path().join("mcp.sock"),
            )
            .unwrap(),
        );
        let repo = Arc::new(SqlxRepo::open("sqlite::memory:").await.unwrap());
        let mut tx = repo.pool().begin().await.unwrap();
        let area = area_create_tx(
            &mut tx,
            NewArea {
                name: "fixture".into(),
                color: "#000000".into(),
                sort: None,
            },
        )
        .await
        .unwrap();
        let track = track_create_tx(
            &mut tx,
            NewTrack {
                area_id: area.id,
                title: "fixture".into(),
                sort: None,
                cwd: cwd.display().to_string(),
                template_id: None,
                plugin_scope: None,
                template_input: None,
                attach_folder: false,
                theme: RequestTheme::default_dark(),
            },
            None,
            &TrackWorkspacePlan::AttachedFromCwd,
            None,
            repo.track_area_cache(),
        )
        .await
        .unwrap();
        let card = card_create_with_id_tx(
            &mut tx,
            "card".into(),
            NewCard {
                track_id: track.id.clone(),
                kind: "codex".into(),
                sort: None,
                payload: json!({"planner_provider":"opencode"}),
                title: None,
            },
            CardRole::Planner,
            true,
            repo.card_role_cache(),
        )
        .await
        .unwrap();
        session_start_runtime_tx(
            &mut tx,
            WorkerSessionInit::shared_planner(
                "worker".into(),
                card.id.to_string(),
                AgentProvider::OpenCode,
                WorkerSessionState::Idle,
                Some("thread".into()),
                json!({"mode":"harness"}),
                1,
            ),
        )
        .await
        .unwrap();
        session_bind_attribution_tx(
            &mut tx,
            &"worker".into(),
            ThreadAttribution {
                worker_session_id: "worker".into(),
                provider: AgentProvider::OpenCode,
                thread_id: Some("thread".into()),
                session_id: Some("ses_owned".into()),
                active_turn_id: None,
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        Self {
            _root: root,
            host,
            repo,
            card: card.id.to_string(),
            track: track.id.to_string(),
            cwd,
        }
    }
    async fn session(&self) -> OpenCodePlannerSession {
        self.session_for("worker").await
    }
    async fn session_for(&self, worker: &str) -> OpenCodePlannerSession {
        let repo: Arc<dyn Repo> = self.repo.clone();
        OpenCodePlannerSession::open(OpenCodePlannerSessionParams {
            host: self.host.clone(),
            worker_session_id: worker.into(),
            card_id: self.card.clone(),
            track_id: self.track.clone(),
            cwd: self.cwd.clone(),
            instructions: "workspace instructions".into(),
            proxy: vec![],
            prior_total_tokens: 10,
            seals: SharedCodexAppServer::new_stub(repo.clone()),
            repo,
        })
        .await
        .unwrap()
    }
    async fn intent(&self, sending: bool) -> String {
        let intent = OpenCodeSubmissionIntent {
            id: "intent".into(),
            worker_session_id: "worker".into(),
            card_id: self.card.clone(),
            scope_id: self.host.scope_id.clone(),
            generation: 0,
            thread_id: "thread".into(),
            native_session_id: "ses_owned".into(),
            client_id: "client".into(),
            native_message_id: "msg_user".into(),
            input_json: json!({"messageID":"msg_user","parts":[{"type":"text","text":"operation"}],"system":"workspace instructions","model":{"providerID":"fixture","modelID":"model"}}),
            created_at_ms: 1,
        };
        opencode_submission_prepare(self.repo.pool(), &intent)
            .await
            .unwrap();
        if sending {
            assert!(
                opencode_submission_claim_prepared(self.repo.pool(), &intent.id, 2)
                    .await
                    .unwrap()
            );
        }
        intent.id
    }
    fn state(&self) -> Value {
        std::fs::read(
            self.host
                .configured()
                .unwrap()
                .config_dir
                .join("fixture-native.json"),
        )
        .ok()
        .map(|s| serde_json::from_slice(&s).unwrap())
        .unwrap_or(json!({"posts":[],"messages":[]}))
    }
}
async fn terminal(
    notifications: &mut tokio::sync::broadcast::Receiver<Notification>,
) -> (Value, i64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut total = 0;
        loop {
            match notifications.recv().await.unwrap() {
                Notification::Other { method, params } if method == "thread/tokenUsage/updated" => {
                    total = params["tokenUsage"]["total"]["totalTokens"]
                        .as_i64()
                        .unwrap()
                }
                Notification::TurnCompleted { turn, .. } => return (turn, total),
                _ => {}
            }
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn opencode_production_prepared_recovery_is_visible_failure_without_spawn_or_post() {
    let f = Fixture::new("complete").await;
    let id = f.intent(false).await;
    let session = f.session().await;
    let mut notifications = session.subscribe_notifications();
    session.mark_installed();
    let (outcome, total) = terminal(&mut notifications).await;
    assert_eq!(outcome["status"], "failed");
    assert!(
        outcome["error"]["message"]
            .as_str()
            .unwrap()
            .contains("never sent")
    );
    assert_eq!(total, 10);
    assert!(!session.has_unresolved_submission().await.unwrap());
    let terminal_receipt = session
        .recovery_submission(Some("client"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal_receipt.state, OpenCodeSubmissionState::Failed);
    assert!(f.state()["posts"].as_array().unwrap().is_empty());
    assert_eq!(
        opencode_submission_get_by_client(f.repo.pool(), &f.host.scope_id, "ses_owned", "client")
            .await
            .unwrap()
            .unwrap()
            .id,
        id
    );
    assert!(session.shared.process.lock().await.is_none());
    assert!(!f.cwd.join("serve-starts").exists());
    session.shutdown().await.unwrap();
}
#[tokio::test]
async fn opencode_production_shutdown_barrier_forbids_delayed_recovery_mint_and_spawn() {
    let f = Fixture::new("loss").await;
    f.intent(true).await;
    let session = Arc::new(f.session().await);
    let guard = session.shared.process.lock().await;
    session.mark_installed();
    let owned = session.clone();
    let shutdown = tokio::spawn(async move { owned.shutdown().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !session.shared.state().shutting_down {
            tokio::task::yield_now().await
        }
    })
    .await
    .unwrap();
    drop(guard);
    shutdown.await.unwrap().unwrap();
    tokio::task::yield_now().await;
    assert!(session.shared.process.lock().await.is_none());
    assert!(
        !f.host
            .configured()
            .unwrap()
            .config_dir
            .join("fixture-native.json")
            .exists()
    );
    let hash: Option<String> =
        sqlx::query_scalar("SELECT mcp_token_hash FROM worker_sessions WHERE id='worker'")
            .fetch_one(f.repo.pool())
            .await
            .unwrap();
    assert!(hash.is_none());
    assert!(!f.cwd.join("serve-starts").exists());
    assert!(session.has_unresolved_submission().await.unwrap());
}
#[tokio::test]
async fn opencode_production_second_turn_accumulates_usage_and_resets_to_profile_default() {
    let f = Fixture::new("complete").await;
    let session = f.session().await;
    let mut notifications = session.subscribe_notifications();
    session.mark_installed();
    let explicit = TurnModelSelection {
        model: Some("fixture/other".into()),
        effort: None,
    };
    assert!(matches!(
        session
            .turn_start(
                "thread",
                vec![InputItem::Text {
                    text: "first".into()
                }],
                &explicit,
                "one"
            )
            .await
            .unwrap(),
        TurnAdmission::Accepted { .. }
    ));
    let (first, total) = terminal(&mut notifications).await;
    assert_eq!(first["status"], "completed");
    assert_eq!(total, 13);
    sqlx::query("UPDATE worker_sessions SET agent_session_id=NULL WHERE id='worker'")
        .execute(f.repo.pool())
        .await
        .unwrap();
    let default = TurnModelSelection {
        model: None,
        effort: None,
    };
    assert!(matches!(
        session
            .turn_start(
                "thread",
                vec![InputItem::Text {
                    text: "second".into()
                }],
                &default,
                "two"
            )
            .await
            .unwrap(),
        TurnAdmission::Accepted { .. }
    ));
    let (second, total) = terminal(&mut notifications).await;
    assert_eq!(second["status"], "completed");
    assert_eq!(total, 16);
    let state = f.state();
    let posts = state["posts"].as_array().unwrap();
    assert_eq!(posts.len(), 2);
    assert_eq!(posts[0]["model"]["modelID"], "other");
    assert_eq!(posts[1]["model"]["modelID"], "model");
    assert_ne!(posts[0]["messageID"], posts[1]["messageID"]);
    session.shutdown().await.unwrap();
}
#[tokio::test]
async fn opencode_production_recovery_identity_error_cleans_up_and_keeps_unknown_fence() {
    let f = Fixture::new("identity-error").await;
    f.intent(true).await;
    let session = f.session().await;
    let mut notifications = session.subscribe_notifications();
    session.mark_installed();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Notification::Other { method, .. } = notifications.recv().await.unwrap() {
                if method == "opencode/submission/unknown" {
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    assert!(session.shared.process.lock().await.is_none());
    assert!(session.has_unresolved_submission().await.unwrap());
    assert!(f.state()["posts"].as_array().unwrap().is_empty());
    session.turn_interrupt("thread", "intent").await.unwrap();
    session.shutdown().await.unwrap();
    assert_eq!(
        opencode_submission_get_unresolved_by_card(f.repo.pool(), &f.card)
            .await
            .unwrap()
            .unwrap()
            .state,
        OpenCodeSubmissionState::Unknown
    );
}

#[tokio::test]
async fn opencode_production_paged_history_retains_complete_current_turn_and_trims_old_turns() {
    let f = Fixture::new("loss").await;
    f.intent(true).await;
    let mut messages = Vec::new();
    for i in 0..500 {
        messages.push(json!({"info":{"id":format!("msg_old_{i}"),"sessionID":"ses_owned","role":"user","time":{"created":i}},"parts":[]}));
    }
    messages.push(json!({"info":{"id":"msg_user","sessionID":"ses_owned","role":"user","time":{"created":500}},"parts":[]}));
    for i in 0..300 {
        messages.push(json!({"info":{"id":format!("msg_tail_{i}"),"sessionID":"ses_owned","role":"assistant","parentID":"msg_user","finish":if i==299 {"stop"} else {"tool-calls"},"time":{"created":501+i,"completed":502+i}},"parts":[]}));
    }
    let profile = &f.host.configured().unwrap().config_dir;
    std::fs::create_dir_all(profile).unwrap();
    std::fs::write(
        profile.join("fixture-native.json"),
        serde_json::to_vec(&json!({"messages":messages,"posts":[]})).unwrap(),
    )
    .unwrap();
    let mut process = super::process::ServerProcess::start(&f.host, "paging", &f.cwd, &[], None)
        .await
        .unwrap();
    let intent = opencode_submission_get_unresolved_by_card(f.repo.pool(), &f.card)
        .await
        .unwrap()
        .unwrap();
    let tail = super::driver::snapshot(&process.client, "ses_owned", &intent)
        .await
        .unwrap();
    assert_eq!(tail.len(), 301);
    assert_eq!(tail[0]["info"]["id"], "msg_user");
    assert_eq!(tail[300]["info"]["id"], "msg_tail_299");
    let projection = super::translate::TurnProjection::new(
        "thread".into(),
        "intent".into(),
        "msg_user".into(),
        "client".into(),
        f.cwd.display().to_string(),
        10,
    )
    .unwrap();
    assert_eq!(
        projection.outcome(&tail),
        Some(super::translate::Outcome::Completed)
    );
    assert!(f.state()["posts"].as_array().unwrap().is_empty());
    process.shutdown(&f.host, "paging").await.unwrap();
}

#[tokio::test]
async fn opencode_production_paging_preserves_the_entire_equal_time_bucket() {
    let f = Fixture::new("loss").await;
    f.intent(true).await;
    let mut messages = vec![
        json!({"info":{"id":"msg_old","sessionID":"ses_owned","role":"user","time":{"created":999}},"parts":[]}),
    ];
    // Pinned native order is (time_created,id). An assistant can sort before its preassigned
    // parent at the same millisecond, even though it was produced by the later prompt loop.
    for i in 0..300 {
        messages.push(json!({"info":{"id":format!("msg_a{i:03}"),"sessionID":"ses_owned","role":"assistant","parentID":"msg_user","finish":"stop","time":{"created":1000,"completed":1001}},"parts":[]}));
    }
    messages.push(json!({"info":{"id":"msg_user","sessionID":"ses_owned","role":"user","time":{"created":1000}},"parts":[]}));
    let profile = &f.host.configured().unwrap().config_dir;
    std::fs::create_dir_all(profile).unwrap();
    std::fs::write(
        profile.join("fixture-native.json"),
        serde_json::to_vec(&json!({"messages":messages,"posts":[]})).unwrap(),
    )
    .unwrap();
    let mut process = super::process::ServerProcess::start(&f.host, "tied", &f.cwd, &[], None)
        .await
        .unwrap();
    let intent = opencode_submission_get_unresolved_by_card(f.repo.pool(), &f.card)
        .await
        .unwrap()
        .unwrap();
    let result = super::driver::snapshot(&process.client, "ses_owned", &intent).await;
    process.shutdown(&f.host, "tied").await.unwrap();
    let tail = result.unwrap();
    assert_eq!(
        tail.len(),
        301,
        "original parent position cannot truncate its same-millisecond assistant replies"
    );
    assert!(tail.iter().any(|m| m["info"]["id"] == "msg_a000"));
    assert!(tail.iter().any(|m| m["info"]["id"] == "msg_user"));
}

#[tokio::test]
async fn opencode_production_recovery_adopts_original_identity_across_worker_incarnation() {
    let f = Fixture::new("complete").await;
    f.intent(false).await;
    let mut tx = f.repo.pool().begin().await.unwrap();
    session_supersede_active_tx(&mut tx, &"worker".into(), 3)
        .await
        .unwrap();
    session_start_runtime_tx(
        &mut tx,
        WorkerSessionInit::shared_planner(
            "successor".into(),
            f.card.clone(),
            AgentProvider::OpenCode,
            WorkerSessionState::Starting,
            Some("fresh-thread".into()),
            json!({"mode":"harness"}),
            4,
        ),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let session = f.session_for("successor").await;
    let receipt = session
        .recovery_submission(Some("client"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.worker_session_id, "worker");
    let identity: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT thread_id,agent_session_id FROM worker_sessions WHERE id='successor'",
    )
    .fetch_one(f.repo.pool())
    .await
    .unwrap();
    assert_eq!(identity, (Some("thread".into()), Some("ses_owned".into())));
    let mut notifications = session.subscribe_notifications();
    session.mark_installed();
    assert_eq!(terminal(&mut notifications).await.0["status"], "failed");
    assert!(!f.cwd.join("serve-starts").exists());
    session.shutdown().await.unwrap();
}
