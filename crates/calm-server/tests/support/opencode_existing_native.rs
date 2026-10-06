use super::*;

pub(super) fn persisted_user(input: &Value, at: i64) -> Value {
    let id = input["messageID"].as_str().unwrap();
    let mut message = user(id, input["parts"][0]["text"].as_str().unwrap(), at);
    message["parts"] = Value::Array(
        input["parts"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(index, part)| {
                let mut part = part.clone();
                if part.get("id").is_none() {
                    part["id"] = json!(format!("prt_native_{id}_{index}"));
                }
                part["messageID"] = json!(id);
                part["sessionID"] = json!(SESSION);
                part
            })
            .collect(),
    );
    message
}
pub(super) fn ordered_messages(messages: &[Value]) -> Vec<Value> {
    let mut messages = messages.to_vec();
    messages.sort_by(|a, b| {
        (
            a["info"]["time"]["created"].as_i64().unwrap(),
            a["info"]["id"].as_str().unwrap(),
        )
            .cmp(&(
                b["info"]["time"]["created"].as_i64().unwrap(),
                b["info"]["id"].as_str().unwrap(),
            ))
    });
    messages
}
