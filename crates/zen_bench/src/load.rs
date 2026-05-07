//! Concurrent loader: HTTP POST batches of spans to a running server.

use std::time::Duration;

use anyhow::Result;
use serde::Serialize;

use crate::workload::SpanIn;

#[derive(Serialize)]
struct IngestRequest<'a> {
    tenant_id: u64,
    partition_id: u32,
    spans: &'a [SpanIn],
}

pub async fn load_to_server(
    target: &str,
    spans: Vec<SpanIn>,
    batch_size: usize,
    concurrency: usize,
) -> Result<usize> {
    use futures::stream::{self, StreamExt};
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;

    // Group by tenant_id for ingestion.
    use std::collections::HashMap;
    let mut by_tenant: HashMap<u64, Vec<SpanIn>> = HashMap::new();
    for s in spans {
        by_tenant.entry(s.tenant_id).or_default().push(s);
    }

    let url = format!("{target}/v1/ingest");
    let mut tasks = Vec::new();
    for (tenant_id, tspans) in by_tenant {
        for chunk in tspans.chunks(batch_size) {
            tasks.push((tenant_id, chunk.to_vec()));
        }
    }

    let total = tasks.iter().map(|(_, v)| v.len()).sum::<usize>();
    let url_owned = url.clone();
    let total_done: usize = stream::iter(tasks.into_iter())
        .map(|(tenant_id, batch)| {
            let client = client.clone();
            let url = url_owned.clone();
            async move {
                let req = IngestRequest {
                    tenant_id,
                    partition_id: 0,
                    spans: &batch,
                };
                let r = client.post(&url).json(&req).send().await;
                match r {
                    Ok(resp) if resp.status().is_success() => batch.len(),
                    _ => 0,
                }
            }
        })
        .buffer_unordered(concurrency)
        .fold(0usize, |acc, n| async move { acc + n })
        .await;
    Ok(total_done.min(total))
}
