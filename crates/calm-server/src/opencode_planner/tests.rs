use super::translate::{Outcome, TurnProjection};
use crate::codex_appserver::Notification;
use serde_json::{Value, json};

fn projection() -> TurnProjection {
    TurnProjection::new(
        "thread".into(),
        "turn".into(),
        "msg_user".into(),
        "client".into(),
        "/workspace".into(),
        10,
    )
    .unwrap()
}
fn assistant(finish: &str, parts: Vec<Value>) -> Value {
    json!({"info":{"id":"msg_assistant","sessionID":"ses_owned","role":"assistant","parentID":"msg_user","finish":finish,"time":{"created":2,"completed":3}},"parts":parts})
}

#[test]
fn opencode_completion_requires_correlated_final_message_and_settled_tools() {
    let p = projection();
    assert_eq!(p.outcome(&[]), None);
    assert_eq!(p.outcome(&[json!({"status":"idle"})]), None);
    assert_eq!(p.outcome(&[assistant("tool-calls", vec![])]), None);
    assert_eq!(p.outcome(&[assistant("unknown", vec![])]), None);
    assert_eq!(
        p.outcome(&[assistant(
            "stop",
            vec![json!({"id":"prt_tool","type":"tool","state":{"status":"running"}})]
        )]),
        None
    );
    let mut foreign = assistant("stop", vec![]);
    foreign["info"]["parentID"] = json!("msg_foreign");
    assert_eq!(p.outcome(&[foreign]), None);
    assert_eq!(
        p.outcome(&[assistant(
            "stop",
            vec![json!({"id":"prt_tool","type":"tool","state":{"status":"completed"}})]
        )]),
        Some(Outcome::Completed)
    );
    assert!(matches!(
        p.outcome(&[assistant("length", vec![])]),
        Some(Outcome::Failed(_))
    ));
}

#[test]
fn opencode_abort_response_is_not_native_interruption_evidence() {
    let p = projection();
    assert_eq!(p.outcome(&[json!(true)]), None);
    let mut aborted = assistant("unknown", vec![]);
    aborted["info"]["error"] = json!({"name":"MessageAbortedError","data":{"message":"aborted"}});
    assert_eq!(p.outcome(&[aborted]), Some(Outcome::Interrupted));
}

#[test]
fn opencode_snapshot_reconciliation_is_stable_and_never_projects_another_turn() {
    let mut p = projection();
    let mut other = assistant(
        "stop",
        vec![json!({"id":"prt_foreign","type":"text","text":"foreign"})],
    );
    other["info"]["parentID"] = json!("msg_foreign");
    let ours = assistant(
        "stop",
        vec![json!({"id":"prt_text","type":"text","text":"answer"})],
    );
    let frames = p.snapshot(&[other.clone(), ours.clone()]);
    assert_eq!(frames.len(), 1);
    assert!(
        matches!(&frames[0],Notification::Item {method,params} if method=="item/completed" && params["item"]["id"]=="prt_text")
    );
    assert!(p.snapshot(&[other, ours]).is_empty());
}

#[test]
fn opencode_native_tool_input_output_and_failure_are_structured() {
    let mut p = projection();
    let frame = assistant(
        "stop",
        vec![
            json!({"id":"prt_tool","type":"tool","tool":"bash","state":{"status":"completed","input":{"command":"free -b"},"output":"memory output","metadata":{"exit":0},"time":{"start":1,"end":2}}}),
        ],
    );
    let frames = p.snapshot(&[frame]);
    assert!(
        matches!(&frames[0],Notification::Item {params,..} if params["item"]["type"]=="commandExecution" && params["item"]["command"]=="free -b" && params["item"]["aggregatedOutput"]=="memory output" && params["item"]["exitCode"]==0)
    );
}

#[tokio::test]
async fn opencode_http_response_loss_never_retries_the_post() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut buf = [0; 1024];
        loop {
            let n = socket.read(&mut buf).await.unwrap();
            bytes.extend_from_slice(&buf[..n]);
            if n == 0 || bytes.windows(4).any(|s| s == b"\r\n\r\n") {
                break;
            }
        }
        assert!(String::from_utf8_lossy(&bytes).starts_with("POST /session/ses_owned/message"));
        socket.shutdown().await.unwrap();
        drop(socket);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let directory = tempfile::tempdir().unwrap();
    let client = super::client::Client::new(port, "private", directory.path());
    assert!(
        client
            .request(
                "POST",
                "/session/ses_owned/message",
                Some(&json!({"parts":[]})),
                std::time::Duration::from_secs(2)
            )
            .await
            .is_err()
    );
    server.await.unwrap();
}

#[test]
fn opencode_private_environment_and_marker_namespace_are_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let config = super::config::OpenCodePlannerConfig {
        opencode_binary: "/private/opencode".into(),
        opencode_version: super::config::PINNED_VERSION.into(),
        config_dir: dir.path().join("profile"),
    };
    let host = super::config::OpenCodePlannerHost::new(
        Some(config),
        dir.path(),
        "/shim".into(),
        dir.path().join("mcp.sock"),
    )
    .unwrap();
    let env = host.environment("worker", &[]).unwrap();
    assert_eq!(
        env.iter().find(|(key, _)| key == "HOME").unwrap().1,
        dir.path().join("profile").into_os_string()
    );
    for forbidden in [
        "NEIGE_MCP_TOKEN",
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "OPENCODE_CONFIG_CONTENT",
        "SSH_AUTH_SOCK",
    ] {
        assert!(!env.iter().any(|(key, _)| key == forbidden));
    }
    let marker = &env
        .iter()
        .find(|(key, _)| key == crate::claude_planner::stop::MARKER_KEY)
        .unwrap()
        .1;
    assert_eq!(
        marker,
        &std::ffi::OsString::from(host.instance.marker("opencode:worker"))
    );
    assert_ne!(
        marker,
        &std::ffi::OsString::from(host.instance.marker("worker"))
    );
}

#[test]
fn opencode_workspace_rules_are_injected_despite_disabled_project_config() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join(".git")).unwrap();
    std::fs::create_dir(root.path().join("nested")).unwrap();
    std::fs::write(root.path().join("AGENTS.md"), "root instructions").unwrap();
    std::fs::write(root.path().join("nested/AGENTS.md"), "nested instructions").unwrap();
    let text = super::wiring::workspace_instructions(&root.path().join("nested")).unwrap();
    assert!(text.find("root instructions").unwrap() < text.find("nested instructions").unwrap());
}
