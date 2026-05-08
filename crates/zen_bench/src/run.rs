//! Benchmark runner: run a fixed suite of queries against a server, record
//! p50/p95/p99 via t-digest.

use std::time::{Duration, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tdigest::TDigest;

#[derive(Clone, Debug)]
pub struct BenchSuite {
    pub queries: Vec<NamedQuery>,
}

#[derive(Clone, Debug)]
pub struct NamedQuery {
    pub name: String,
    pub sql: String,
    pub tenant_id: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BenchResult {
    pub name: String,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub n: usize,
}

#[derive(Serialize)]
struct QueryRequest<'a> {
    tenant_id: u64,
    query: &'a str,
}

pub async fn run_suite(
    target: &str,
    suite: &BenchSuite,
    duration: Duration,
    concurrency: usize,
) -> Result<Vec<BenchResult>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let url = format!("{target}/v1/query");
    let mut out = Vec::new();
    for q in &suite.queries {
        let mut td = TDigest::new_with_size(100);
        let mut samples = Vec::new();
        let deadline = Instant::now() + duration;
        let req = QueryRequest {
            tenant_id: q.tenant_id,
            query: &q.sql,
        };
        // Run sequentially when concurrency=1 so latency is single-shot. Otherwise
        // spawn `concurrency` tasks racing for the deadline.
        if concurrency <= 1 {
            while Instant::now() < deadline {
                let t0 = Instant::now();
                let _ = client.post(&url).json(&req).send().await?.bytes().await?;
                let elapsed = t0.elapsed().as_micros() as f64;
                samples.push(elapsed);
            }
        } else {
            use futures::future::join_all;
            let n_loops = concurrency;
            let mut handles = Vec::new();
            for _ in 0..n_loops {
                let client = client.clone();
                let url = url.clone();
                let q = q.clone();
                let dl = deadline;
                handles.push(tokio::spawn(async move {
                    let mut local = Vec::new();
                    let req = QueryRequest {
                        tenant_id: q.tenant_id,
                        query: &q.sql,
                    };
                    while Instant::now() < dl {
                        let t0 = Instant::now();
                        let _ = client.post(&url).json(&req).send().await;
                        local.push(t0.elapsed().as_micros() as f64);
                    }
                    local
                }));
            }
            for v in join_all(handles).await.into_iter().flatten() {
                samples.extend(v);
            }
        }
        td = td.merge_unsorted(samples.clone());
        let p50 = td.estimate_quantile(0.5);
        let p95 = td.estimate_quantile(0.95);
        let p99 = td.estimate_quantile(0.99);
        out.push(BenchResult {
            name: q.name.clone(),
            p50_us: p50,
            p95_us: p95,
            p99_us: p99,
            n: samples.len(),
        });
    }
    Ok(out)
}

pub fn default_suite() -> BenchSuite {
    BenchSuite {
        queries: vec![
            NamedQuery {
                name: "B2_time_range_attr_filter".into(),
                tenant_id: 0,
                sql: "SELECT span_id, model, duration_ms, prompt FROM spans WHERE model = 'gpt-4o' AND status = 'error' LIMIT 100".into(),
            },
            NamedQuery {
                name: "B3_fts_common_term".into(),
                tenant_id: 0,
                sql: "SELECT span_id, prompt FROM spans WHERE text_match(prompt, 'memory') LIMIT 100".into(),
            },
            NamedQuery {
                name: "B6_jsonpath_indexed".into(),
                tenant_id: 0,
                sql: "SELECT span_id FROM spans WHERE metadata.tier = 'primary' LIMIT 100".into(),
            },
            NamedQuery {
                name: "B8_aggregation_by_model".into(),
                tenant_id: 0,
                sql: "SELECT model, count(*) FROM spans GROUP BY model".into(),
            },
        ],
    }
}
