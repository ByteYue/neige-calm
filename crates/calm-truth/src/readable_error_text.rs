//! One reduction of a failed turn's `error.message` to readable text (#1829), shared by the
//! activity projector's planner-down item and the transcript wire's `turn_error_text`.

use serde_json::Value;

/// A failed turn's `error.message` in words a person reads; codex fills the message with the
/// upstream HTTP body. An HTML body keeps only its visible text; an embedded JSON error object gives
/// its inner `error.message` (else `message`), after the words before it and its `status`. Anything
/// else, including JSON that does not parse to such an object, is returned unchanged. No length cap.
pub fn readable_error_text(raw: &str) -> String {
    let lower = raw.to_ascii_lowercase();
    if ["<!doctype", "<html", "<head", "<body", "<title"]
        .iter()
        .any(|tag| lower.contains(tag))
    {
        return html_visible_text(raw, &lower);
    }
    json_error_text(raw).unwrap_or_else(|| raw.to_string())
}

/// `head`, `title`, `style` and `script` elements go whole, every other tag becomes a space, and
/// whitespace collapses. `lower` is `raw` ASCII-lowercased, so its byte offsets are `raw`'s.
fn html_visible_text(raw: &str, lower: &str) -> String {
    let mut out = String::new();
    let mut at = 0;
    // A hidden element whose closing tag is missing from some point on is missing from every later
    // point too: remember it, so an unclosed one costs one search, not one per tag.
    let mut unclosed: Vec<&str> = Vec::new();
    while let Some(open) = lower[at..].find('<').map(|i| at + i) {
        out.push_str(&raw[at..open]);
        let after = &lower[open + 1..];
        let hidden = ["head", "title", "style", "script"]
            .into_iter()
            .find(|name| {
                after.starts_with(name)
                    && after[name.len()..]
                        .starts_with(|c: char| c == '>' || c.is_ascii_whitespace())
            });
        let close = hidden
            .filter(|name| !unclosed.contains(name))
            .and_then(|name| {
                let found = lower[open..].find(&format!("</{name}")).map(|i| open + i);
                if found.is_none() {
                    unclosed.push(name);
                }
                found
            });
        match lower[close.unwrap_or(open)..].find('>') {
            Some(i) => {
                out.push(' ');
                at = close.unwrap_or(open) + i + 1;
            }
            None => {
                at = open;
                break;
            }
        }
    }
    out.push_str(&raw[at..]);
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// `<words>{"status": 400, "error": {"message": "…"}}<rest>` → `<words>: 400: … <rest>`; the status is
/// left out when the words already carry it. `None` unless the first `{` starts a JSON object with a
/// non-empty message.
fn json_error_text(raw: &str) -> Option<String> {
    let start = raw.find('{')?;
    let mut values = serde_json::Deserializer::from_str(&raw[start..]).into_iter::<Value>();
    let value = values.next()?.ok()?;
    let rest = raw[start + values.byte_offset()..].trim();
    let message = value
        .pointer("/error/message")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|m| !m.is_empty())?;
    let words = raw[..start].trim_end().trim_end_matches(':').trim_end();
    let status = value.get("status").and_then(|s| match s {
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
        _ => None,
    });
    let mut parts: Vec<&str> = Vec::new();
    if !words.is_empty() {
        parts.push(words);
    }
    if let Some(status) = status.as_deref().filter(|s| !words.contains(s)) {
        parts.push(status);
    }
    parts.push(message);
    let text = parts.join(": ");
    Some(if rest.is_empty() {
        text
    } else {
        format!("{text} {rest}")
    })
}

#[cfg(test)]
mod tests {
    use super::readable_error_text;

    #[test]
    fn an_html_body_keeps_its_visible_text() {
        let raw = "unexpected status 403 Forbidden: <!doctype html><meta charset=\"utf-8\">\
                   <meta name=viewport content=\"width=device-width, initial-scale=1\">\
                   <title>403</title>403 Forbidden, url: https://sub2api.example.com/v1/responses";
        assert_eq!(
            readable_error_text(raw),
            "unexpected status 403 Forbidden: 403 Forbidden, \
             url: https://sub2api.example.com/v1/responses"
        );
    }

    #[test]
    fn an_embedded_json_error_gives_its_status_and_message() {
        let raw = "{\"type\":\"error\",\"status\":400,\"error\":{\"type\":\"invalid_request_error\",\
                   \"message\":\"The 'gpt-6-astra' model requires a newer version of Codex. \
                   Please upgrade to the latest app or CLI and try again.\"}}";
        assert_eq!(
            readable_error_text(raw),
            "400: The 'gpt-6-astra' model requires a newer version of Codex. \
             Please upgrade to the latest app or CLI and try again."
        );
    }

    #[test]
    fn plain_text_is_unchanged() {
        let raw = "You've hit your usage limit. Visit https://chatgpt.com/codex/settings/usage \
                   to purchase more credits or try again at Sep 19th, 2026 4:22 PM.";
        assert_eq!(readable_error_text(raw), raw);
    }

    #[test]
    fn an_unclosed_hidden_element_is_one_search_not_one_per_tag() {
        // About 1 MB of `<script>` tags that never close: one closing-tag search per tag is quadratic
        // (minutes in a debug build); the remembered miss keeps it linear (well under a second).
        let body = "<script>x</p>".repeat(80_000);
        let raw = format!("unexpected status 502: <!doctype html>{body}tail");
        let started = std::time::Instant::now();
        let text = readable_error_text(&raw);
        let elapsed = started.elapsed();
        assert_eq!(
            text,
            format!("unexpected status 502: {}tail", "x ".repeat(80_000))
        );
        assert!(
            elapsed < std::time::Duration::from_secs(10),
            "reducing 1 MB of unclosed <script> tags took {elapsed:?}: the closing-tag search is \
             quadratic again (the serial projector runs this on every planner-down recompute)"
        );
    }

    #[test]
    fn unparseable_json_falls_back_to_the_raw_text() {
        let raw = "stream error: {\"type\":\"error\",\"status\":502,\"error\":{\"message\":\"up";
        assert_eq!(readable_error_text(raw), raw);
    }
}
