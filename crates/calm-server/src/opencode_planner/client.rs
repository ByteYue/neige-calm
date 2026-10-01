//! A cancellable, bounded loopback HTTP exchange. There are no redirects or POST retries.
use crate::error::{CalmError, Result};
use base64::Engine as _;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::Duration;

const BODY_LIMIT: usize = 8 * 1024 * 1024;
pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct Client {
    port: u16,
    authorization: String,
    directory: PathBuf,
}

impl Client {
    pub(crate) fn new(port: u16, password: &str, directory: &Path) -> Self {
        Self {
            port,
            authorization: format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("opencode:{password}"))
            ),
            directory: directory.to_path_buf(),
        }
    }

    pub(crate) async fn request(
        &self,
        method: &str,
        path: &str,
        payload: Option<&Value>,
        timeout: Duration,
    ) -> Result<Value> {
        if !path.starts_with('/') || path.contains('#') {
            return Err(CalmError::BadRequest(
                "invalid OpenCode request path".into(),
            ));
        }
        let mut url = url::Url::parse(&format!("http://127.0.0.1:{}{path}", self.port))
            .map_err(|e| CalmError::BadRequest(e.to_string()))?;
        url.query_pairs_mut()
            .append_pair("directory", &self.directory.to_string_lossy());
        let body = payload
            .map(serde_json::to_vec)
            .transpose()?
            .unwrap_or_default();
        let request = Request::builder()
            .method(method)
            .uri(url.as_str())
            .header("authorization", &self.authorization)
            .header("content-type", "application/json")
            .header("connection", "close")
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| CalmError::Internal(e.to_string()))?;
        let exchange = async {
            let socket = tokio::time::timeout(
                Duration::from_secs(3),
                tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, self.port)),
            )
            .await
            .map_err(|_| CalmError::Conflict("OpenCode connection timed out".into()))??;
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(socket))
                    .await
                    .map_err(|e| CalmError::Conflict(format!("OpenCode HTTP handshake: {e}")))?;
            let _connection = AbortConnection(tokio::spawn(connection));
            let result = async {
                let response = sender
                    .send_request(request)
                    .await
                    .map_err(|e| CalmError::Conflict(format!("OpenCode response was lost: {e}")))?;
                let status = response.status();
                let mut stream = response.into_body();
                let mut bytes = Vec::new();
                while let Some(frame) = stream.frame().await {
                    let frame = frame
                        .map_err(|e| CalmError::Conflict(format!("OpenCode response body: {e}")))?;
                    if let Some(data) = frame.data_ref() {
                        if bytes.len().saturating_add(data.len()) > BODY_LIMIT {
                            return Err(CalmError::Conflict(
                                "OpenCode response exceeded the byte limit".into(),
                            ));
                        }
                        bytes.extend_from_slice(data);
                    }
                }
                if !status.is_success() {
                    // A native error can follow a durable user message write. Do not classify
                    // an HTTP status as an execution rejection or include credential-bearing text.
                    return Err(CalmError::Conflict(format!(
                        "OpenCode HTTP status {}",
                        status.as_u16()
                    )));
                }
                serde_json::from_slice(&bytes).map_err(|e| {
                    CalmError::Conflict(format!("OpenCode response is invalid JSON: {e}"))
                })
            }
            .await;
            result
        };
        tokio::time::timeout(timeout, exchange).await.map_err(|_| {
            CalmError::Conflict("OpenCode request outcome is unknown after timeout".into())
        })?
    }

    pub(crate) async fn get(&self, path: &str) -> Result<Value> {
        self.request("GET", path, None, READ_TIMEOUT).await
    }
}

struct AbortConnection(tokio::task::JoinHandle<std::result::Result<(), hyper::Error>>);
impl Drop for AbortConnection {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) fn native_id<'a>(id: &'a str, prefix: &str) -> Result<&'a str> {
    if id.starts_with(prefix)
        && id.len() <= 160
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        Ok(id)
    } else {
        Err(CalmError::Conflict(format!(
            "invalid OpenCode {prefix} identity"
        )))
    }
}
