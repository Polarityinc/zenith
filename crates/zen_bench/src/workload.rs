//! `ai-traces-v1` synthetic workload generator.
//!
//! Distributions match `Brainstore March 2025` rough shape: 12 spans/trace
//! lognormal, 8 models 80/20, 50 tools Zipf, 95/4/1 status, 1536-dim
//! embeddings.

use rand::{rngs::StdRng, Rng, SeedableRng};
use rand_distr::{Distribution, LogNormal, WeightedAliasIndex};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    pub rows: usize,
    pub tenants: u32,
    pub primary_tenant_share: f64,
    pub seed: u64,
    pub time_window_ms: i64,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            rows: 4_000_000,
            tenants: 10,
            primary_tenant_share: 0.5,
            seed: 0xc0ff_eec0,
            time_window_ms: 7 * 24 * 3600 * 1000,
        }
    }
}

const MODELS: &[(&str, f64)] = &[
    ("gpt-4o", 0.50),
    ("claude-sonnet-4-7", 0.20),
    ("gpt-5-mini", 0.10),
    ("haiku-4-5", 0.08),
    ("o4-mini", 0.05),
    ("gemini-pro", 0.04),
    ("llama-3-70b", 0.02),
    ("mistral-large", 0.01),
];

const SPAN_TYPES: &[(&str, f64)] = &[
    ("agent_step", 0.10),
    ("llm_call", 0.40),
    ("tool_call", 0.35),
    ("retrieval", 0.10),
    ("eval_score", 0.05),
];

const STATUSES: &[(&str, f64)] = &[("ok", 0.95), ("error", 0.04), ("timeout", 0.01)];

const PROMPT_POOL: &[&str] = &[
    "Summarize the following conversation in 2-3 sentences",
    "What is the time complexity of this algorithm?",
    "Generate a SQL query that selects the top 10 customers by revenue",
    "Compose a polite reply to this customer email",
    "Translate this English paragraph to French",
    "Out of memory error in retrieval cache during compaction",
    "Rate limit exceeded for tier free; please upgrade your plan",
    "Explain the difference between mutexes and rwlocks in Rust",
    "Decode the base64 string and return the JSON payload",
    "Find the bug in this Python function and propose a fix",
    "Analyze the user behaviour log and identify churn signals",
    "Search for recent papers about retrieval-augmented generation",
    "Generate a list of 10 unique product names that follow the brand voice",
    "Walk me through how transformers handle long context windows",
    "Write a one-paragraph executive summary of the attached PDF",
    "Refactor this React component to use the new compiler hooks",
];

const COMPLETION_POOL: &[&str] = &[
    "Sure — based on what you described, the user is asking how to ...",
    "The complexity is O(n log n) due to the sort operation in the inner loop ...",
    "SELECT customer_id, SUM(revenue) AS total FROM orders GROUP BY customer_id ...",
    "Hi — thanks for reaching out. I understand the frustration ...",
    "Voici la traduction en français de ce paragraphe ...",
    "Sorry, the worker exhausted its memory limit while building the segment ...",
    "You've reached the request quota for the free tier; upgrading to ...",
    "A mutex grants exclusive access; a rwlock allows multiple readers ...",
    "Here is the decoded JSON: {\"id\":1234,\"role\":\"admin\"}",
    "The bug is on line 14: the index variable is reused across loops ...",
];

pub fn generate_workload(cfg: &WorkloadConfig) -> Vec<SpanIn> {
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let model_idx = WeightedAliasIndex::new(MODELS.iter().map(|(_, w)| *w).collect()).unwrap();
    let span_type_idx =
        WeightedAliasIndex::new(SPAN_TYPES.iter().map(|(_, w)| *w).collect()).unwrap();
    let status_idx = WeightedAliasIndex::new(STATUSES.iter().map(|(_, w)| *w).collect()).unwrap();
    // 12 spans/trace lognormal; mean 12, sigma 0.6.
    let trace_size = LogNormal::new(2.4f64, 0.6).unwrap();
    let now: i64 = chrono::Utc::now().timestamp_millis();
    let lo = now - cfg.time_window_ms;

    let mut out = Vec::with_capacity(cfg.rows);
    while out.len() < cfg.rows {
        // Pick tenant.
        let tenant = if rng.gen_bool(cfg.primary_tenant_share) {
            0u64
        } else {
            rng.gen_range(1..cfg.tenants as u64)
        };
        let n_spans = trace_size.sample(&mut rng).clamp(1.0, 500.0) as usize;
        let trace_id = ulid::Ulid::new();
        let trace_start = rng.gen_range(lo..now);
        for s in 0..n_spans {
            if out.len() >= cfg.rows {
                break;
            }
            let model = MODELS[model_idx.sample(&mut rng)].0;
            let span_type = SPAN_TYPES[span_type_idx.sample(&mut rng)].0;
            let status = STATUSES[status_idx.sample(&mut rng)].0;
            let span_id = ulid::Ulid::new();
            let parent = if s > 0 {
                Some(ulid::Ulid::new().to_string())
            } else {
                None
            };
            let dur: i64 = (LogNormal::new(6.5f64, 1.0).unwrap().sample(&mut rng) as i64).max(1);
            let st = trace_start + (s as i64) * 100;
            let et = st + dur;
            let prompt = PROMPT_POOL[rng.gen_range(0..PROMPT_POOL.len())].to_string();
            let completion = COMPLETION_POOL[rng.gen_range(0..COMPLETION_POOL.len())].to_string();
            let cost = rng.gen_range(0.00001..0.05);
            let metadata = serde_json::json!({
                "tier": if tenant == 0 { "primary" } else { "secondary" },
                "user_id": format!("u-{}-{}", tenant, rng.gen_range(0..1000)),
                "request_id": format!("r-{}", rng.gen_range(0..1_000_000)),
                "output": {
                    "steps": [
                        {"name": if s % 2 == 0 {"router"} else {"summarize"}}
                    ]
                }
            });

            out.push(SpanIn {
                tenant_id: tenant,
                partition_id: 0,
                trace_id: Some(trace_id.to_string()),
                span_id: Some(span_id.to_string()),
                parent_span_id: parent,
                start_time_ms: st,
                end_time_ms: et,
                duration_ms: Some(dur),
                span_type: Some(span_type.to_string()),
                status: Some(status.to_string()),
                provider: Some("openai".into()),
                model: Some(model.to_string()),
                tool_name: Some(format!("tool-{:02}", rng.gen_range(0..50))),
                prompt: Some(prompt),
                completion: Some(completion),
                prompt_tokens: Some(rng.gen_range(20..2000)),
                completion_tokens: Some(rng.gen_range(20..1500)),
                cost_usd: Some(cost),
                temperature: Some(rng.gen_range(0.0..1.0)),
                top_p: Some(rng.gen_range(0.7..1.0)),
                tool_io_text: None,
                user_id: Some(format!("u-{}-{}", tenant, rng.gen_range(0..1000))),
                session_id: Some(format!("s-{}", rng.gen_range(0..10000))),
                request_id: Some(format!("r-{}", rng.gen_range(0..1_000_000))),
                metadata: Some(metadata),
                embedding: None,
            });
        }
    }
    out.truncate(cfg.rows);
    out
}

/// Mirror of the server's `IngestRequest::SpanIn` — duplicated here to keep
/// the bench crate independent of zen_server.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpanIn {
    pub tenant_id: u64,
    pub partition_id: u32,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub parent_span_id: Option<String>,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub duration_ms: Option<i64>,
    pub span_type: Option<String>,
    pub status: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub tool_name: Option<String>,
    pub prompt: Option<String>,
    pub completion: Option<String>,
    pub prompt_tokens: Option<u32>,
    pub completion_tokens: Option<u32>,
    pub cost_usd: Option<f64>,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub tool_io_text: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub request_id: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub embedding: Option<Vec<f32>>,
}
