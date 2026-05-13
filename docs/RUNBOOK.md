# Zenith — Operator Runbook

This runbook covers what an operator needs to do to deploy, observe, scale,
recover, and decommission a ZenithDB cluster. It does NOT cover developing
the engine; for that, read the per-crate doc comments and `docs/SCALING_1TB_1PB.md`.

## 1. Architecture overview

Zenith is a storage-disaggregated columnar database for AI agent traces:

- **Catalog** (Postgres only, with `MockCatalog` for tests) holds metadata:
  segment manifests, WAL pointers, commit IDs, compaction leases, cluster
  node registry.
- **Object store** (S3 / GCS / Azure / local FS) holds the immutable data:
  segments and WAL files. Every node can read every byte; sharding is purely
  about *who routes the query*, not about *who stores the data*.
- **Compute nodes** run as containers behind a load balancer. Three roles:
  `coordinator`, `worker`, `compactor`, or `all` (the default).

Traffic flow:

```
client ──HTTPS+JWT──→ LB ──→ coordinator (any node) ──HTTP+HMAC──→ worker
                                          │                          │
                                          ▼                          ▼
                                       Postgres                     S3
                                      (catalog)                  (segments)
```

## 2. Deploy

### Helm (Kubernetes — recommended)

```bash
helm install zenithdb deploy/helm/zenithdb \
  --set image.tag=v0.2.0 \
  --set catalog.backend=postgres \
  --set catalog.postgresUrl=$(aws secretsmanager get-secret-value ...) \
  --set storage.backend=s3 \
  --set storage.bucket=$(terraform output -raw bucket) \
  --set podDisruptionBudget.enabled=true \
  --set autoscaling.enabled=true \
  --set autoscaling.minReplicas=3 \
  --set autoscaling.maxReplicas=20
```

The chart:
- Splits liveness (`/v1/healthz`) and readiness (`/v1/readyz`) probes.
- Ships a `PodDisruptionBudget` to keep ≥1 replica during voluntary
  disruption (node drains, rollouts).
- Provides an HPA on CPU + memory.
- Ships a default-deny `NetworkPolicy` (set `networkPolicy.enabled=true`
  to enforce; configure `allowFromNamespaces` and `allowEgressCidrs`).
- Creates a `ServiceAccount` with `automountToken: false` (Zenith doesn't
  talk to the K8s API). Add IRSA / Workload Identity annotations for
  cloud-managed credentials.

### Terraform (AWS)

```bash
cd deploy/terraform/aws
terraform init
terraform apply \
  -var=vpc_id=vpc-... \
  -var='private_subnet_ids=["subnet-...", "subnet-..."]'
```

Provisions:
- KMS key for envelope encryption + bucket SSE.
- S3 bucket (versioned, SSE-KMS, public-access block).
- RDS Postgres in your private subnets, multi-AZ, encrypted, 14-day backup
  retention, deletion protection on, password held in Secrets Manager.
- IAM policy granting the Zenith pod least-privilege S3 + KMS + Secrets
  Manager access. Attach via IRSA.

## 3. Observability

### Metrics

Scrape `GET /v1/metrics` (Prometheus text format). Important series:

| Metric | Type | What it tells you |
|---|---|---|
| `zen_query_duration_seconds` | histogram | p50/p95/p99 query latency by tenant |
| `zen_ingest_duration_seconds` | histogram | Write-flush latency by tenant |
| `zen_wal_flush_duration_seconds` | histogram | WAL durability cost (group commit + fsync) |
| `zen_compaction_duration_seconds` | histogram | Background compaction wall time |
| `zen_queries_total{status}` | counter | Query throughput, errors |
| `zen_ingest_rows_total{status}` | counter | Rows ingested |
| `zen_segments_active` | gauge | Total active segments per tenant |
| `zen_wal_lag_bytes` | gauge | WAL backlog awaiting compaction |

Suggested alerts:

- `histogram_quantile(0.95, rate(zen_query_duration_seconds_bucket[5m])) > 0.5`
  → query p95 > 500 ms; investigate.
- `rate(zen_queries_total{status="error"}[5m]) > 1`
  → sustained errors.
- `zen_wal_lag_bytes > 5e9`
  → 5 GB WAL backlog; compactor is falling behind.

### Tracing

Set `telemetry.otlp_endpoint` in config (or `ZEN_OTLP_ENDPOINT` env var)
to ship spans to Jaeger / Tempo / Honeycomb. Default sample rate is 100%;
operators with high QPS should set `OTEL_TRACES_SAMPLER_ARG=0.01` for 1%.

### Logs

JSON to stdout (`tracing-subscriber` `fmt::layer`). `LOG_FORMAT=json` is
the default in container images. Ship with Vector / Fluent Bit /
Promtail to your sink of choice.

## 4. Auth

### JWT (customer-facing)

Configure `auth.jwks_url` to your IdP's JWKS endpoint. Required claims:

- `tenant_id` (number) — which tenant this token may operate against
- `exp` (unix seconds) — required by ZenithDB

Optional:

- `sub` — used for audit logs only
- `scope` — space-separated permissions: `ingest read admin`. Empty = read-only.

ZenithDB caches verified claims for 5 minutes (capped at 16 K entries).
Rotation: emit a new JWKS document at the same URL with both old and new
keys; ZenithDB picks up the new one within `jwks_ttl` (default 5 min).

### HMAC (inter-node)

Configure `auth.internal_secret` identically on every node. Used to sign
`/v1/internal/*` traffic between cluster nodes. Rotation requires a
rolling restart with the new secret.

## 5. Backup and restore

### Take a backup

```bash
zen admin-backup --config /etc/zenithdb/config.toml --tenant 7 --out /backups/2026-05-08
```

Output:
- `manifest.json` — snapshot timestamp, segment list, commit IDs.
- `segments/<uuid>.zseg` — copies of every active segment for the tenant.

### Restore

```bash
# 1. Stand up a fresh cluster pointing at a fresh catalog + bucket.
# 2. Restore the tenant.
zen admin-restore --config /etc/zenithdb/config.toml \
                  --tenant 7 \
                  --from /backups/2026-05-08
```

Re-registers segments in the catalog and copies the bytes back into the
configured object store. Subsequent queries against tenant 7 see the data
as of the snapshot.

**RPO**: as low as the WAL flush cadence (default 100 ms). Use
`zen admin backup --continuous` (planned) for archive-on-flush.

**RTO**: dominated by data volume. ~5 min per 100 GB on a single restore
worker.

## 6. Scaling

- **Vertical**: bump `requests.cpu` / `requests.memory` in values.yaml.
  ZenithDB is bandwidth-bound on cold reads, CPU-bound on warm queries
  with FTS / aggregations.
- **Horizontal**: increase `replicaCount` or enable `autoscaling`. New
  nodes register themselves in the catalog `nodes` table; the HRW shard
  map adapts within ~5 s.
- **Tenant pinning**: for big tenants, set `shards: "tenant=N"` on the
  pod env so it only handles that tenant's primary routing. Other nodes
  remain wildcard fallbacks.

## 7. Common incidents

### "All my queries are 401"

Either:
- `auth.jwks_url` is unreachable — check egress rules and the IdP's
  health.
- The token is expired — leeway is 5 s.
- `tenant_id` claim is missing or doesn't match the request — common when
  a token from one project is used against another.

### "Read-after-write returns nothing"

The "0 ms write visible" path scans unconsumed WAL on every query. If
the `wal_objects` row was created but the bytes haven't arrived in the
store yet (object-store eventual consistency), the query will miss
those rows. With S3 strong-consistency (now default) this should not
happen; check object_store credentials and IAM.

### "Compactor is way behind"

Symptom: `zen_wal_lag_bytes` climbing. Reasons:
- Compactor pod isn't running (check role).
- Compaction lease is stuck on a dead worker — leases auto-expire after
  `compact.lease_ttl_seconds` (default 300). Wait it out or DELETE the
  row from `compaction_leases`.
- Object store is throttling. Check `zen_storage` error metrics.

### "Disk full"

Typical when running in a single-node `MockCatalog` + local-FS dev deployment
without rotation. Inspect:

- `du -sh data/blobs` for raw size
- `select sum(byte_count) from segments where superseded_at is null` for
  active size

If `superseded_at` rows are accumulating, that's the GC window — the
default 1 h grace makes recently-superseded segments still on disk.

### "Process died on SIGTERM but lost data"

The graceful-shutdown path flushes memtables to WAL before exiting.
Confirm:

- Container received SIGTERM, not SIGKILL (check `terminationGracePeriodSeconds`).
- `ZEN_UNSAFE_FAST` is NOT set; the default fsync ON path durabilizes
  the WAL flush.

## 8. Decommission

```bash
# 1. Remove the tenant from any LB routing rules.
# 2. Take a final backup.
zen admin-backup --tenant N --out /archive/tenantN-final
# 3. Delete catalog rows.
psql -c "UPDATE segments SET superseded_at = now() WHERE tenant_id = N;"
psql -c "DELETE FROM wal_objects WHERE tenant_id = N;"
# 4. Delete object-store data.
aws s3 rm s3://zenithdb-prod/tenants/N/ --recursive
```

## 9. Where to read more

- `docs/SCALING_1TB_1PB.md` — capacity planning and per-shape latency model.
- Per-crate Rustdoc — `cargo doc --workspace --no-deps --open`.
- The `Cargo.toml` workspace deps comments for "why this crate".
