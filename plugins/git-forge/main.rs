use serde_json::{Value, json};
use std::io::{BufRead, BufWriter, Write};

fn main() {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => return,
        };
        if line.trim().is_empty() {
            continue;
        }
        let frame: Value = match serde_json::from_str(&line) {
            Ok(frame) => frame,
            Err(e) => {
                eprintln!("git-forge: bad json: {e}");
                continue;
            }
        };
        let Some(id) = frame.get("id").cloned() else {
            continue;
        };
        let method = frame
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();

        let reply = match method {
            "initialize" => initialize_reply(&frame, id),
            "tools/call" => tools_call_reply(&frame, id),
            _ => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "echo": method }
            }),
        };

        let mut encoded = serde_json::to_string(&reply).expect("reply serializes");
        encoded.push('\n');
        if out.write_all(encoded.as_bytes()).is_err() {
            return;
        }
        if out.flush().is_err() {
            return;
        }
    }
}

fn initialize_reply(frame: &Value, id: Value) -> Value {
    let protocol = frame
        .get("params")
        .and_then(|params| params.get("protocolVersion"))
        .cloned()
        .unwrap_or_else(|| Value::String("2025-11-25".into()));
    let expected = frame
        .pointer("/params/_meta/dev.neige~1auth/expected_echo")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let echoed = std::env::var("NEIGE_PLUGIN_TOKEN")
        .ok()
        .or(expected)
        .unwrap_or_default();
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "protocolVersion": protocol,
            "serverInfo": { "name": "git-forge", "version": "0.1.0" },
            "capabilities": {
                "experimental": {
                    "dev.neige/kernel-callbacks": { "version": 1 }
                }
            },
            "_meta": {
                "dev.neige/auth": { "echoed_token": echoed }
            }
        }
    })
}

fn tools_call_reply(frame: &Value, id: Value) -> Value {
    let tool = frame
        .pointer("/params/name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let args = frame
        .pointer("/params/arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    match lower(tool, &args) {
        Ok(structured) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [],
                "isError": false,
                "structuredContent": structured
            }
        }),
        Err(error) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [{ "type": "text", "text": error }],
                "isError": true,
                "structuredContent": { "error": error }
            }
        }),
    }
}

use calm_server::builtin_plugins::dev::git_actions::lower;
