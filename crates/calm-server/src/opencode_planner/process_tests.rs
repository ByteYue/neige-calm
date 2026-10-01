//! Offline integration of the production spawn, authentication, private config and marker stop.
use super::{
    config::{OpenCodePlannerConfig, OpenCodePlannerHost, PINNED_VERSION},
    process::ServerProcess,
};
use serde_json::Value;
use std::path::Path;

const FAKE_SERVE: &str = r#"#!/usr/bin/python3
import base64,http.server,json,os,subprocess,sys
if '--version' in sys.argv:
    print('1.18.34')
    raise SystemExit(0)
port=int(sys.argv[sys.argv.index('--port')+1])
settings=json.load(open(os.environ['OPENCODE_CONFIG']))
child=subprocess.Popen(['/bin/sleep','300'],start_new_session=True)
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self,*args): pass
    def do_GET(self):
        auth='Basic '+base64.b64encode(('opencode:'+os.environ['OPENCODE_SERVER_PASSWORD']).encode()).decode()
        if self.headers.get('Authorization')!=auth:
            self.send_response(401); self.end_headers(); return
        path=self.path.split('?')[0]
        if path=='/global/health': answer={'healthy':True,'version':'1.18.34'}
        elif path=='/fixture': answer={'settings':settings,'environment':dict(os.environ),'child':child.pid,'cwd':os.getcwd()}
        elif path=='/provider': answer={'all':[{'id':'fixture','models':{'model':{'name':'Fixture model','variants':{'small':{},'large':{}}}}}],'connected':['fixture']}
        elif path=='/config': answer={'model':'fixture/model'}
        else: self.send_response(404); self.end_headers(); return
        body=json.dumps(answer).encode();self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
server=http.server.ThreadingHTTPServer(('127.0.0.1',port),Handler)
print('opencode server listening on http://127.0.0.1:'+str(server.server_address[1]),flush=True)
server.serve_forever()
"#;

fn host(root: &Path) -> OpenCodePlannerHost {
    use std::os::unix::fs::PermissionsExt;
    let executable = root.join("fake-opencode");
    std::fs::write(&executable, FAKE_SERVE).unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    OpenCodePlannerHost::new(
        Some(OpenCodePlannerConfig {
            opencode_binary: executable,
            opencode_version: PINNED_VERSION.into(),
            config_dir: root.join("dedicated-profile"),
        }),
        root,
        "/scope/mcp-shim".into(),
        root.join("mcp.sock"),
    )
    .unwrap()
}

fn no_live_process(pid: u64) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return true;
    };
    stat.rsplit_once(')')
        .is_some_and(|(_, tail)| tail.trim_start().starts_with('Z'))
}

#[tokio::test]
async fn opencode_managed_serve_auth_config_and_stop_are_scope_isolated() {
    let root = tempfile::tempdir().unwrap();
    let host = host(root.path());
    let cwd = root.path().join("workspace");
    std::fs::create_dir(&cwd).unwrap();
    let mut one = ServerProcess::start(&host, "one", &cwd, &[], Some("token-one"))
        .await
        .unwrap();
    let mut two = ServerProcess::start(&host, "two", &cwd, &[], Some("token-two"))
        .await
        .unwrap();
    let first = one.client.get("/fixture").await.unwrap();
    let second = two.client.get("/fixture").await.unwrap();
    assert_eq!(
        first["settings"]["compaction"],
        serde_json::json!({"auto":false,"prune":false})
    );
    assert_eq!(
        first
            .pointer("/settings/mcp/calm/environment/NEIGE_MCP_TOKEN")
            .and_then(Value::as_str),
        Some("token-one")
    );
    assert_eq!(
        second
            .pointer("/settings/mcp/calm/environment/NEIGE_MCP_TOKEN")
            .and_then(Value::as_str),
        Some("token-two")
    );
    let mcp = &first["settings"]["mcp"]["calm"];
    assert!(
        mcp["environment"]
            .as_object()
            .unwrap()
            .values()
            .all(Value::is_string),
        "native MCP configuration requires string environment values"
    );
    let kernel_path = crate::kernel_bin_path::kernel_led_path().unwrap();
    assert_eq!(mcp["environment"]["PATH"], kernel_path.path_utf8().unwrap());
    // This owned definition must override a disabled calm entry in the private profile.
    assert_eq!(mcp["enabled"], true);
    assert_eq!(
        first["environment"]["HOME"].as_str(),
        host.configured().unwrap().config_dir.to_str()
    );
    assert_eq!(first["environment"]["OPENCODE_DISABLE_PROJECT_CONFIG"], "1");
    assert_eq!(first["environment"]["NEIGE_MCP_TOKEN"], "token-one");
    assert_eq!(second["environment"]["NEIGE_MCP_TOKEN"], "token-two");
    assert_eq!(
        first["environment"]["NEIGE_MCP_SOCKET"].as_str(),
        host.mcp_socket.to_str()
    );
    assert_ne!(
        first["environment"]["NEIGE_MCP_TOKEN"],
        "ambient-must-not-leak"
    );
    assert!(first["environment"].get("OPENAI_API_KEY").is_none());
    let child = first["child"].as_u64().unwrap();
    assert!(!no_live_process(child));
    one.shutdown(&host, "one").await.unwrap();
    assert!(no_live_process(child));
    assert_eq!(
        two.client.get("/global/health").await.unwrap()["healthy"],
        true
    );
    let second_child = second["child"].as_u64().unwrap();
    assert!(!no_live_process(second_child));
    two.shutdown(&host, "two").await.unwrap();
    assert!(no_live_process(second_child));
}

#[tokio::test]
async fn opencode_readiness_uses_the_owned_serve_catalog_and_reaps_it() {
    let root = tempfile::tempdir().unwrap();
    let host = host(root.path());
    let catalog = host.catalog().await.unwrap();
    assert_eq!(catalog.default_model.as_deref(), Some("fixture/model"));
    assert_eq!(catalog.models.len(), 1);
    assert_eq!(catalog.models[0].value, "fixture/model");
    assert_eq!(catalog.models[0].effort_levels, ["large", "small"]);
}

#[tokio::test]
async fn opencode_unscoped_catalog_process_never_inherits_an_ambient_neige_token() {
    let root = tempfile::tempdir().unwrap();
    let host = host(root.path());
    let mut process = ServerProcess::start(&host, "no-token", root.path(), &[], None)
        .await
        .unwrap();
    let fixture = process.client.get("/fixture").await.unwrap();
    assert!(fixture["environment"].get("NEIGE_MCP_TOKEN").is_none());
    assert!(fixture["environment"].get("NEIGE_MCP_SOCKET").is_none());
    assert!(fixture["settings"].get("mcp").is_none());
    process.shutdown(&host, "no-token").await.unwrap();
}
