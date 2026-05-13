//! HTTP client for inter-node query forwarding.
//!
//! Workers expose `POST /v1/internal/query` which accepts the same
//! `QueryRequest` shape as the public endpoint plus a `disable_route=true`
//! flag so the receiving node executes locally rather than re-routing.
//! This keeps the wire protocol trivial: it's the public API.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use zen_query::ResultSet;

use crate::node::NodeInfo;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalQueryRequest {
    pub tenant_id: u64,
    pub query: String,
    pub dialect: Option<String>,
    /// True when this request is sent worker→worker. Workers honor this
    /// by disabling further routing — they just execute locally.
    pub disable_route: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalQueryResponse {
    pub result: ResultSet,
}

#[derive(Debug, Error)]
pub enum RemoteError {
    #[error("all replicas failed: last={last}")]
    AllReplicasFailed { last: String },
    #[error("transport: {0}")]
    Transport(String),
    #[error("status {status}: {body}")]
    Status { status: u16, body: String },
    #[error("decode: {0}")]
    Decode(String),
}

#[derive(Clone)]
pub struct RemoteClient {
    http: reqwest::Client,
    /// Optional HMAC signer. When set, every outbound request to a peer's
    /// `/v1/internal/*` endpoint is signed; the receiver's `hmac_layer`
    /// verifies the same secret. When unset, requests go unsigned — only
    /// safe for single-node or fully-trusted-network deployments.
    signer: Option<zen_auth::HmacVerifier>,
}

impl RemoteClient {
    pub fn new(timeout: Duration) -> Self {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .pool_max_idle_per_host(64)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            // Keep the inter-node hop snappy.
            .http2_prior_knowledge()
            .build()
            // .build() can fail if TLS roots can't load. Fall back to a
            // default Client when prior-knowledge HTTP/2 isn't available.
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { http, signer: None }
    }

    /// Builder: enable HMAC signing on outbound inter-node requests. The
    /// secret must match the receiver's `auth.internal_secret`.
    pub fn with_signer(mut self, signer: zen_auth::HmacVerifier) -> Self {
        self.signer = Some(signer);
        self
    }

    /// Send a query to the first reachable target. Tries replicas in order
    /// until one returns 2xx, then returns its `ResultSet`.
    pub async fn forward(
        &self,
        targets: &[NodeInfo],
        req: &InternalQueryRequest,
    ) -> Result<ResultSet, RemoteError> {
        let mut last_err: String = "no targets".into();
        for t in targets {
            match self.try_one(&t.endpoint, req).await {
                Ok(r) => return Ok(r),
                Err(e) => {
                    tracing::warn!(target=%t.endpoint, err=%e, "remote forward failed");
                    last_err = format!("{e}");
                }
            }
        }
        Err(RemoteError::AllReplicasFailed { last: last_err })
    }

    /// Fan-out to all targets in parallel; collect (result, target) pairs.
    /// Targets that fail are logged + skipped — partial results are still
    /// returned so the cluster degrades gracefully under partial outage.
    pub async fn fan_out(
        &self,
        targets: &[NodeInfo],
        req: &InternalQueryRequest,
    ) -> Vec<ResultSet> {
        use futures::stream::{FuturesUnordered, StreamExt};
        let mut futs = FuturesUnordered::new();
        for t in targets {
            let endpoint = t.endpoint.clone();
            let req = req.clone();
            let this = self.clone();
            futs.push(async move {
                match this.try_one(&endpoint, &req).await {
                    Ok(r) => Some(r),
                    Err(e) => {
                        tracing::warn!(target=%endpoint, err=%e, "fan-out failed");
                        None
                    }
                }
            });
        }
        let mut out = Vec::with_capacity(targets.len());
        while let Some(r) = futs.next().await {
            if let Some(rs) = r {
                out.push(rs);
            }
        }
        out
    }

    async fn try_one(
        &self,
        endpoint: &str,
        req: &InternalQueryRequest,
    ) -> Result<ResultSet, RemoteError> {
        // SECURITY: defend against SSRF via a malicious node entry in
        // the cluster registry. A registered peer endpoint that points
        // at link-local metadata services (e.g. AWS 169.254.169.254)
        // could be used to exfiltrate IAM credentials when this
        // coordinator forwards a query. Reject obviously dangerous
        // hosts before issuing the request.
        if !is_endpoint_allowed(endpoint) {
            return Err(RemoteError::Transport(format!(
                "endpoint blocked: {endpoint} resolves to a forbidden host"
            )));
        }
        let url = format!("{}/v1/internal/query", endpoint.trim_end_matches('/'));
        // Pre-serialize the body so we can both sign it and ship it.
        // Doing this once avoids re-encoding inside `reqwest`.
        let body = serde_json::to_vec(req).map_err(|e| RemoteError::Decode(format!("{e}")))?;
        let mut builder = self
            .http
            .post(&url)
            .header("content-type", "application/json");
        if let Some(signer) = &self.signer {
            let ts = chrono::Utc::now().timestamp();
            let sig = signer
                .sign("POST", "/v1/internal/query", &body, ts)
                .map_err(|e| RemoteError::Transport(format!("hmac sign: {e}")))?;
            builder = builder
                .header("X-Zen-Hmac", sig)
                .header("X-Zen-Timestamp", ts.to_string());
        }
        let resp = builder
            .body(body)
            .send()
            .await
            .map_err(|e| RemoteError::Transport(format!("{e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(RemoteError::Status {
                status: status.as_u16(),
                body,
            });
        }
        let parsed: InternalQueryResponse = resp
            .json()
            .await
            .map_err(|e| RemoteError::Decode(format!("{e}")))?;
        Ok(parsed.result)
    }
}

/// Vet a registered peer endpoint before forwarding a query to it. The
/// goal is to block the most common SSRF pivots in cloud environments
/// — link-local metadata (169.254.169.254 / fd00:ec2::254) and any
/// hostname the operator probably didn't intend.
///
/// We DO allow `localhost` / `127.0.0.1` and 10.x / 172.16-31.x /
/// 192.168.x because legitimate clusters live on those ranges. The
/// strictest defence would be "reject all RFC1918 outside an explicit
/// allowlist", but that breaks the common single-VPC deployment. The
/// IdP-style fix (only fetch over HTTPS to allowed hosts) is wired
/// upstream in JWKS; here we focus on blocking known exfiltration
/// targets.
fn is_endpoint_allowed(endpoint: &str) -> bool {
    let parsed = match url::Url::parse(endpoint) {
        Ok(u) => u,
        Err(_) => return false,
    };
    // Only http(s) — `file://`, `gopher://` etc. would defeat the
    // intent of "make an HTTP call to a peer".
    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return false;
    }
    let host = match parsed.host_str() {
        Some(h) => h,
        None => return false,
    };
    // Block IMDS hosts explicitly. Cover the AWS / GCP / Azure
    // canonical addresses + IPv6 link-local.
    const BLOCKED_HOSTS: &[&str] = &[
        "169.254.169.254",
        "metadata.google.internal",
        "metadata",
        "fd00:ec2::254",
        "fe80::a9fe:a9fe",
    ];
    if BLOCKED_HOSTS.contains(&host) {
        return false;
    }
    // Block any host literal-IP starting with 169.254.* or fe80:: (link-local).
    if host.starts_with("169.254.") {
        return false;
    }
    if host.starts_with("fe80:") || host.starts_with("[fe80:") {
        return false;
    }
    true
}

#[cfg(test)]
mod endpoint_tests {
    use super::*;

    #[test]
    fn allows_loopback() {
        assert!(is_endpoint_allowed("http://127.0.0.1:8080"));
        assert!(is_endpoint_allowed("http://localhost:9000"));
    }

    #[test]
    fn allows_rfc1918() {
        assert!(is_endpoint_allowed("https://10.0.0.5:8080"));
        assert!(is_endpoint_allowed("https://192.168.1.1:8080"));
    }

    #[test]
    fn blocks_imds() {
        assert!(!is_endpoint_allowed(
            "http://169.254.169.254/latest/meta-data/"
        ));
        assert!(!is_endpoint_allowed("http://metadata.google.internal"));
        assert!(!is_endpoint_allowed("http://[fe80::1]:80"));
    }

    #[test]
    fn blocks_non_http_schemes() {
        assert!(!is_endpoint_allowed("file:///etc/passwd"));
        assert!(!is_endpoint_allowed("ftp://attacker"));
    }
}
