# EDMS Simulator — Session Context

Operational notes for picking up work mid-stream. Captures architecture deltas
not yet in `docs/ARCHITECTURE.md`, gotchas discovered while building, and a
log of what each phase commit shipped.

---

## Quick start

```bash
# 1. start postgres + redis (ports 5433 + 6380 to avoid host clashes)
docker compose up -d postgres redis

# 2. python deps (one venv, locked in requirements.txt)
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt -r requirements-dev.txt

# 3. env (copy + fill ANTHROPIC_API_KEY if you want chat/image/email body)
cp .env.example .env

# 4. run the API. .env vars must be in the *process* env — the app uses
#    os.getenv() directly (no python-dotenv autoload). On bash:
set -a; source <(grep -v '^#' .env | grep -v '^$' | sed 's/^/export /'); set +a
.venv/Scripts/python -m uvicorn api.main:app --port 8001

# 5. exercise the full pipeline end-to-end
python scripts/simulate_local.py
```

If you skip step 4's env-source step, the app connects to `localhost:5432`
and Redis defaults — both wrong. The Postgres compose port is **5433**, the
Redis port is **6380**.

---

## Repo state at end of last session

- Branch: `main`, all committed (push pending). Origin: `https://github.com/krishnamami/edms-simulator`.
- Tests: **245 passing, 2 skipped** (live-API tests gated on `ANTHROPIC_API_KEY`).
- `simulate_local.py` still runs end-to-end with exit 0; new `scripts/watch_pipeline.py --full` drives the complete mortgage scenario (borrower + property + AUS + context). New `scripts/simulate_s3_edms.py` exercises the incremental indexer.
- **Production ECS service** still live + DB-backed at `http://edms-simulator-alb-1374683374.us-east-1.elb.amazonaws.com`. The Phases B → indexing changes have **not been deployed to prod yet** — only Phase 0/0.5/A are running there. Local docker-compose has every phase applied.
- The pipeline now has three full layers in code:
  1. **Borrower** — universal ingestion + raw storage + identity + income/credit assembly + document graph
  2. **Property** — properties / property_profiles + URAR / HOI / flood / tax / title generators + extractors + PITI math
  3. **Vendor** — DU / LP / Socure / TWN / SSA / OFAC adapters lands as `document_category='vendor'`
- **One-call read shape** — Decision OS hits `GET /application/{id}/context` and gets borrower + co-borrower + property + vendor_checks + DTI/LTV/LTV-ready flags + missing_items + graph summary, all assembled lazily and cached under `context:{application_id}` (TTL 30m). Layer changes invalidate the cache so the next read re-assembles.
- **Webhooks** — Decision OS subscribes via `POST /webhooks` with optional HMAC secret; every assembly fans out a `context_updated` event. Failed deliveries persist with status + error and increment `webhooks.failure_count`.
- **Persona slices** — `/context/{income|credit|property|compliance|fraud}` for the 6 Decision OS personas (each gets only its slice, not the whole context).
- **Audit trail** — every assembly snapshots into `context_versions`; `GET /application/{id}/context/at/{ts}` does point-in-time replay.
- **Observability** — public `GET /dashboard` (HTML, auto-refresh 15s), auth-gated `GET /application/{id}/pipeline-state` (per-borrower docs, raw_ingestion counts, Redis TTLs, graph, vendor checks, readiness, pipeline_complete bool), `GET /application/{id}/timeline` (raw_ingestion + graph edges + context_versions sorted ascending).
- **Incremental indexer** — watermark-driven S3 → document_index → re-assemble. `WatermarkStore` + `S3Scanner` + `BatchIndexer`. Background `AsyncIOScheduler` runs every 15 min when `ENABLE_SCHEDULER=true`. Endpoints: `/indexing/status`, `POST /indexing/run`, `/indexing/runs[/{id}]`, `PUT /indexing/watermark`.

Latest commits (top of `main`):

```
3c631b7  fix(storage): allow document_index upserts to re-attribute applicant/role
07a5bd2  fix(aggregation): hydrate co_applicant_id + cumulative docs on doc upload
736bf97  fix(aggregation): bust graph cache on every doc save + demo cache-bypass probes
8bb789e  docs: refresh context.md with Phases B → incremental indexer
15b5c46  feat(indexing): incremental batch indexer with watermark
5ba24a6  feat(observability): Phase F — dashboard, pipeline-state, timeline, watch_pipeline
fc03352  feat(context): Phase E — persona slices, webhooks, context versioning
22eb278  feat(vendors): Phase D — AUS, fraud, VOE, SSN, OFAC vendor adapters
42e3a28  feat(context): Phase C — application context assembly + single endpoint
1046058  feat(property): Phase B — property layer ingestion + assembly
123f5c5  docs: refresh context.md with Phase A — raw storage layer
ab4b547  fix(ops): apply_schema.py strips comments before splitting on ';'
00a7d26  feat(raw): Phase A — raw storage layer before extraction
2990e98  docs: refresh context.md after Phase 0 + 0.5 prod bootstrap
ab27bf5  fix(los): connectors must compute ssn_hash so applicants don't collide
047aa6d  fix: hydrate XRefStore from Postgres at startup
c5b142a  feat(mismo): Phase 0 — MISMO compatibility + LOS connectors + external IDs
920a15e  fix: extract decision_os_api_key field from secret instead of injecting whole JSON
2d7d548  fix: enable SSL for ElastiCache Redis in production
60c4d68  fix(db): enable SSL on the asyncpg pool when USE_AWS_SECRETS=true
```

(Earlier history — CFN bootstrap, Aurora→RDS pivot, CI fixes, original ingestion phases — preserved in `git log` from `123f5c5` back.)

---

## Phase log

| Phase | Commit | Scope |
|-------|--------|-------|
| **A** | `75e3a46` | Universal ingestion plumbing — `core/ingestion/{events,router,confidence}.py`, `api_adapter`, `_handle_normalized_ingest_event`, `/ingest/*` endpoints stubbed (501-ish). Bundled persistence fixes that closed 3 simulator gaps. |
| **B** | `08ee2e9` | Document **generators** (W2 / paystub / bank stmt / credit report / driver's license JPG) and **extractors** (`pymupdf_extractor`). `claude_extractor` placed as a stub for Phase C wiring. Round-trip tests prove gen→extract→assert. |
| **C** | `0d0b985` | All 7 channel adapters (chat / image / email / pdf / form / csv / xml). Anthropic SDK wired via shared `_claude_client.py` (model `claude-sonnet-4-6`, prompt-caching on system block). Adapters injectable for tests. `/ingest/*` endpoints replaced stubs with real implementations. |
| **D** | `bff35cc` | `scripts/simulate_local.py` rewritten — 7-step walkthrough exercising every channel + verifying golden record / Redis / Postgres / xref. |
| **E** | `e1fa6b6` | Resilience for upstream Claude errors. `email_adapter` body-extract falls back gracefully (attachments still process). `/ingest/{chat,image,email}` map `anthropic.APIStatusError` → HTTP 502 with detail. Simulator distinguishes **failed (live)** vs **skipped (no key)**. |
| **graph** | `d0e11e3` | Document knowledge graph — `core/graph/{models,reconciler,navigator}.py`. Reconciler writes typed edges (confirms / corroborates / contradicts) using the same `NUMERIC_CONFLICT_THRESHOLD` as `ConfidenceResolver`. Navigator answers questions over the graph (Claude with full reasoning_path when key set, rule-based fallback otherwise). 5 new endpoints under `/applicant/{id}/`. 18 new tests. |
| **0**  | `c5b142a` | MISMO 3.4 compatibility + LOS connectors + external IDs. `core/ingestion/{mismo,los_connector}.py` with 55 MISMO + 20 Encompass mappings, `EncompassConnector` + `GenericMISMOConnector`. Schema adds `applicants.external_ids JSONB`, `applications.external_loan_id` + URLA / HMDA / loan-terms columns, `mismo_doc_type_registry` + `los_connectors` tables. New endpoints: `/ingest/los`, `/loans/from-los`, `/resolve/external/{system}/{id}`, `/mismo/doc-types`. 13 new tests. |
| **0.5** | `047aa6d`, `ab27bf5` | Production data-integrity fixes triggered by Phase 0 prod test. (1) `XRefStore.hydrate_from_postgres()` called from `api/main.py` lifespan so applicant-id sequence + SSN lookups survive across restarts (was silently overwriting via `ON CONFLICT DO UPDATE`). (2) LOS connectors must populate `ssn_hash` from the full SSN — empty strings collide on `idx_applicant_ssn`. 7 new tests. |
| **A** (raw) | `00a7d26`, `ab4b547` | Raw storage layer. Every inbound `/ingest/*` payload is now persisted to S3 (`raw/{channel}/{applicant?}/{date}/{uuid}.{ext}`) and tracked in a new `raw_ingestion` table BEFORE extraction. New `IngestionPipeline` (`core/ingestion/pipeline.py`) wraps the existing `IngestRouter` so the 7 channel endpoints all flow through `received → extracting → indexed` (or `failed`). `RawIngestionStore` exposes status transitions; new endpoints `GET /applicant/{id}/raw-ingestion`, `GET /ingest/{id}/raw`, `POST /ingest/{id}/reprocess`, `GET /pipeline/failed`. Reprocess re-reads the original bytes from S3. New `scripts/watch_pipeline.py` walks all storage layers (`--live` for prod). FK constraints on `raw_ingestion.applicant_id` / `application_id` deliberately omitted — raw arrives before parents may exist. 5 new tests. Followup `ab4b547` hardened `apply_schema.py` to strip `--` line comments before splitting on `;` after a `;` inside a comment broke the first prod schema apply. |
| **B** (property) | `1046058` | **Property layer.** Schema adds `properties` + versioned `property_profiles` + `applications.property_id` FK. `core/property/{sources,rules,assembler,extractors}.py` — PropertyDocType enum, PROPERTY_CONFIDENCE ranking, PITIComponents math (`calculate_piti`), per-doc extractors (`extract_appraisal/hoi/flood/tax/hoa`), assembler that works with partial data + lineage hash + `requires_review` on C5/C6 condition or flood-zone insurance. 5 reportlab generators (`appraisal_generator` URAR, `title_generator`, `hoi_generator`, `flood_cert_generator`, `tax_bill_generator`) — all return `(pdf_bytes, metadata)`. pymupdf `core/property/extractors.py` mirrors the borrower extractor pattern. PostgresStore: `save_property`/`get_property`/`save_property_profile`/`get_property_docs`/`update_application_property`. Redis: `TTL_PROPERTY_PROFILE` + `set/get/invalidate_property_profile`. AggregationService gains a `PROPERTY_DOCUMENT_UPLOADED` handler that fans through PropertyAssembler, persists, warms `property:{id}`, and invalidates `context:{application_id}`. 4 endpoints: `POST /properties`, `GET /property/{id}/profile`, `GET /property/{id}/pipeline-state`, `POST /ingest/property-doc`. 23 new tests. |
| **C** (context) | `42e3a28` | **One-call ApplicationContext.** New `core/context/{models,assembler}.py`. ContextAssembler folds borrower (income+credit+identity), property (PITI+LTV), and vendor layers into a single ApplicationContext. Co-borrower income resolves correctly off the primary's nested `co_borrower` section. Front-/back-end DTI + LTV computed inline; readiness flags (`income_verified`, `credit_pulled`, `appraisal_complete`, `insurance_bound`, `flood_cert_received`, `dti_calculable`, `ltv_calculable`, `aus_ready`) drive `missing_items`. Cached under `context:{application_id}` (TTL 30m); the service invalidates after every income or property re-assembly so the next read recomputes. 4 endpoints: `GET /application/{id}/context`, `/readiness`, `POST /refresh-context`, `GET /dti`. PostgresStore gains `get_application` / `get_application_by_applicant` / `update_application_loan_data`. 11 new tests. |
| **D** (vendors) | `22eb278` | **Vendor return adapters.** 5 class-based adapters in `core/ingestion/adapters/`: `VendorAUSAdapter` (Fannie DU + Freddie LP XML, namespace-tolerant), `VendorFraudAdapter` (Socure + LexisNexis JSON, `requires_review` on `medium_risk`/`high_risk`), `VendorVOEAdapter` (TWN status="A" + Equifax "Yes"), `VendorSSNAdapter` + `VendorOFACAdapter`. `vendor_synthetic.py` generators for the demo path. ContextAssembler.`_get_vendor_checks` now reads `document_category='vendor'` rows from `document_index` and surfaces a flat summary (`aus_findings`, `fraud_score`, `fraud_band`, `fraud_requires_review`, `flood_determination`, `employment_verified`, `ssn_valid`, `ofac_clear`). Readiness rewired: `aus_ready` requires real DU/LP `approved=True`; `identity_verified` driven by SSN; `missing_items` adds `ofac_review_required`/`fraud_review_required` when those checks fail. 3 endpoints: `POST /ingest/vendor-return`, `GET /application/{id}/vendor-checks`, `POST /run-vendor-checks` (synthetic). 15 new tests. |
| **E** (persona slices + webhooks) | `fc03352` | **Persona slices, webhooks, context versioning.** 5 slice models (`IncomeSlice`, `CreditSlice`, `PropertySlice`, `ComplianceSlice`, `FraudSlice`) — each Decision OS persona reads exactly its slice. Schema adds `webhooks` + `webhook_deliveries` + `context_versions`. `core/context/webhook_publisher.py` POSTs to subscribers with optional `X-EDMS-Signature: sha256=...` HMAC, persists every delivery attempt, increments `failure_count` on errors; never raises. ContextAssembler now snapshots every assembly into `context_versions` and fans out a `context_updated` event. New endpoints: 5 slices (`/context/{income\|credit\|property\|compliance\|fraud}`), 4 webhooks (POST/GET/DELETE/deliveries), 2 history (`/history`, `/at/{timestamp}`), 1 catalog (`GET /missing-documents` — borrower/property/vendor missing + structured checklist; treats `AUS_LP_FINDINGS` as satisfying the AUS slot). 12 new tests. |
| **F** (observability) | `5ba24a6` | **Pipeline observability.** Public `GET /dashboard` — self-refreshing HTML with traffic-light coding (income/credit/appraisal/AUS/DTI/LTV/conflicts) + stat tiles. Auth-gated `GET /application/{id}/pipeline-state` — per-borrower docs + raw_ingestion counts + Redis TTLs (`income`/`credit`/`status`), property rollup, graph edges, vendor checks, context block (present + ttl_seconds + DTIs + LTV + requires_review), readiness, `pipeline_complete`. `GET /application/{id}/timeline` merges `application_submitted` + raw_ingestion `document_received`/`extraction_complete` + graph edges + context_versions, sorted ascending. PostgresStore: `get_all_applications`, `get_raw_ingestion_for_application` (JOINs against applications so docs that arrived under the applicant_id but pre-date the application_id are still picked up). RedisStore: `key_state(key)` → `{present, ttl_seconds}` (TTL `-2`→missing, `-1`→no expiry). `scripts/watch_pipeline.py` rewritten with 4 modes: default (single-W2), `--full` (POST /loans → W2 → property → all property docs → run-vendor-checks → final context + pipeline-state + timeline), `--application <id>`, `--upload <pdf> --type <DOCTYPE>`. Tracks failures via global `_FAIL_COUNT`; exit 1 if any `[FAIL]`. 6 new tests. |
| **indexer** | `15b5c46` | **Incremental batch indexer.** Schema: `indexing_watermarks` (per source, status idle/running/complete/failed) + `indexing_runs` (per-run audit with watermark_from/to + JSONB error_details) + `idx_indexing_runs_source`. `s3` row seeded at epoch. `core/indexing/{watermark,s3_scanner,batch_indexer}.py`. WatermarkStore wraps PostgresStore for testability. S3Scanner walks `loans/{los_id}/{category}/{filename}`, filters strictly `LastModified > since`, supports both boto3 and local_storage modes. BatchIndexer.run() scans → groups by LOS → looks up application → `s3.get_raw` → routes to W2/paystub/bank/credit/appraisal/HOI/flood/tax extractor → upserts into `document_index` → re-assembles only the layers that changed (income/credit/asset → `agg._run_assembly`; property → `redis.invalidate_property_profile`) → invalidates `context:{application_id}` → advances watermark. Unknown LOS counted as `skipped`, not `error`. MISMOMapper gains `detect_type_from_filename(filename, category)` for path-anchored type detection with category fallback. PostgresStore: `get_watermark`, `upsert_watermark_status/_complete`, `set_watermark_timestamp`, `create/complete_indexing_run`, `get_indexing_runs[_{id}]`. 5 endpoints: `GET /indexing/status`, `POST /indexing/run` (`{source, dry_run}`), `GET /indexing/runs[?source=&limit=]`, `GET /indexing/runs/{id}`, `PUT /indexing/watermark` (`{source, timestamp}` — admin re-index from a point). `AsyncIOScheduler` background job (15-min default) when `ENABLE_SCHEDULER=true` (off by default). `scripts/simulate_s3_edms.py` drops files into local_storage and verifies skip-unchanged semantics; `--dry-run` and `--watch` flags. FakePostgresStore.save_document now upserts on `document_id` to mirror production's `ON CONFLICT DO UPDATE`. 13 new tests. |

---

## AWS production bootstrap (the long Tuesday-night session)

Took the simulator from "runs locally" to "running on Fargate behind an ALB". Multiple false starts; the ones below are the actual fixes that landed.

### What's live in account 621646470377

| Component | What | Where |
|---|---|---|
| ECR repo | `edms-simulator` (created by self-heal step on first GHA push) | `621646470377.dkr.ecr.us-east-1.amazonaws.com/edms-simulator` |
| ECS cluster | `edms-simulator-cluster` | CFN stack `edms-ecs` |
| ECS service | `edms-simulator-service`, Fargate, desired=2, running=2 | inside `edms-simulator-cluster` |
| ALB | `edms-simulator-alb` | `edms-simulator-alb-1374683374.us-east-1.elb.amazonaws.com:80 → :8001` |
| Task role | `edms-simulator-task-role` (4 broad managed policies attached out-of-band) | externally managed; passed to CFN as `TaskRoleArn` parameter |
| Execution role | `edms-simulator-task-execution-role` (default ECS managed + scoped secrets:Get) | created by the `edms-ecs` stack |
| Log group | `/ecs/edms-simulator`, 30-day retention | created by the stack |
| Secrets Manager | `edms/aurora/credentials`, `edms/redis/endpoint`, `edms/api/keys` (with `-tNFwJM`/`-Z3uo92`/`-NLNCtu` suffixes) | pre-existing in account |
| KMS key | `arn:aws:kms:us-east-1:621646470377:key/f61c6a3c-15aa-4e0d-b9dd-8665a8c88d26` | for `edms-simulator-loans` S3 bucket |

**Backing services (admin-provisioned out-of-band, not via this repo's CFN):**

- **RDS Postgres `edms-postgres-rdsinstance-ev3113lmj40h`** — running, private endpoint, `rds.force_ssl=1`, master user `edms_admin`. Schema applied via `scripts/apply_schema.py` one-off ECS task. After Phase A, the live DB has the full set: applicants / applications / xref / income_profiles / credit_profiles / document_index / document_relationships / mismo_doc_type_registry / los_connectors / **raw_ingestion** + indexes. The repo's `infra/cloudformation/rds-postgres.yaml` was never used to deploy the actual instance.
- **ElastiCache Redis** — running with `TransitEncryptionEnabled=True`. `redis_store.py` triggers `ssl=True` when `ENVIRONMENT=production` or `REDIS_SSL=true`. The repo's `infra/cloudformation/elasticache.yaml` was never used.
- **S3 (`edms-simulator-loans`)** — production raw payloads now land at `raw/{channel}/{applicant?}/{YYYY/MM/DD}/{uuid}.{ext}` via Phase A. The original `loans/{application_id}/{category}/...` layout (Phase B generators) still works for assembled documents.
- **Secrets Manager** — `edms/aurora/credentials` (with `username` corrected from `edms` to `edms_admin`), `edms/redis/endpoint`, `edms/api/keys`. All admin-provisioned out-of-band. `task_definition.json` references them by ARN-with-suffix (`-tNFwJM` / `-Z3uo92` / `-NLNCtu`). The `API_KEY` reference uses ECS's JSON-key syntax (`...-NLNCtu:decision_os_api_key::`) so only the field value is injected, not the whole JSON blob.
- **MISMO type registry seeded in production** — `scripts/seed_mismo_registry.py` ran as a one-off ECS task; 75 mappings + 5 LOS connectors loaded.

### Hard-won lessons

1. **`github-cicd-live` is intentionally narrow.** It can:
   - ECR: GetAuthorizationToken, CreateRepository, layer-upload set, PutImage
   - CFN: CreateStack, DescribeStacks, DescribeStackEvents, DeleteStack
   - ECS: most things (cluster/service/task definition lifecycle)
   - EC2: Describe* (read-only, no SG mutations)
   - sts: GetCallerIdentity

   It **cannot**:
   - IAM: any role mutation (`iam:CreateRole`, `iam:DeleteRolePolicy`, etc.)
   - RDS: most write paths (`rds:DescribeDBSubnetGroups` denied → CFN rollback)
   - cloudformation:ListStackResources (so wedged stacks need an admin to inspect)

   So any CFN template that creates an IAM role or RDS resource will fail under this identity. Workaround: take the role/resource as a parameter (externally-managed pattern); template stays declarative for everything else.

2. **`AssignPublicIp: DISABLED` in default-VPC public subnets is broken.** The default VPC has only public subnets (with IGW routes) and no NAT gateway. With `AssignPublicIp: DISABLED`, Fargate tasks have no egress at all — fail immediately with `ResourceInitializationError: connection issue between the task and AWS Secrets Manager`. Fix: `ENABLED`. Trade-off: tasks have public IPs (still firewalled by `TaskSecurityGroup` to `:8001` from ALB only). For a real VPC with private subnets + NAT, flip back to DISABLED.

3. **CFN template + manually-created resource = `AlreadyExists` failure.** An admin had pre-created `edms-simulator-task-role` between sessions with broad managed policies (S3FullAccess, SQSFullAccess, SecretsManagerReadWrite, CloudWatchLogsFullAccess). The original `ecs.yaml` declared it inline — first deploy failed at TaskRole. Refactor: drop inline `TaskRole`, add `TaskRoleArn` parameter pointing at the externally-managed role. CFN no longer owns it (drift detection won't catch policy changes), but conflict goes away.

4. **`amazon-ecs-render-task-definition@v1` only swaps `image`.** Despite the name, it does **not** substitute placeholders elsewhere. The `executionRoleArn` / `taskRoleArn` `ACCOUNT_ID` placeholders had to be substituted by an explicit `sed` step before render: `aws sts get-caller-identity --query Account --output text` → `sed -i s/ACCOUNT_ID/.../g task_definition.json`.

5. **ECR repo doesn't auto-create.** First push to a new account fails with "repository does not exist". `aws-actions/amazon-ecr-login@v2` only does auth, not provisioning. Self-heal step:
   ```yaml
   - name: Ensure ECR repository exists
     run: |
       aws ecr describe-repositories --repository-names "$ECR_REPOSITORY" --region "$AWS_REGION" \
         || aws ecr create-repository --repository-name "$ECR_REPOSITORY" --region "$AWS_REGION" \
              --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
   ```

6. **pip backtracking on `>=` constraints kills Docker builds.** With 16 packages all on `>=`, pip's resolver thrashed for 20+ minutes through Pillow / PyMuPDF / anyio / async_timeout permutations on `python:3.10-slim`. Fix: take a full `pip freeze` of the working venv, replace `requirements.txt` with all `==` pins. **Docker build now: 80s end-to-end, pip phase 20.5s, zero "looking at multiple versions" lines.** Side-effect: the lockfile required `networkx==3.6.1` which needs Python ≥3.11, so the Dockerfile bumped to `python:3.12-slim` and `ci.yaml`'s `setup-python` bumped to `"3.12"` to match.

### Wedged AWS resources (orphans from earlier failed attempts)

| Resource | Cause | Cleanup |
|---|---|---|
| Stack `edms-aurora` | First Aurora deploy: `rds:DescribeDBSubnetGroups` denied; rollback failed because `iam:DeleteRolePolicy` and `ec2:DeleteSecurityGroup` also denied | **Still wedged in `ROLLBACK_FAILED` last we checked.** Two orphan resources: IAM role `edms-aurora-RDSProxyRole-oBsVFktLB9Z3` and security group `sg-0050f77a029b4642f`. Admin needs to: detach + delete the role's inline policies, delete the role, delete the SG, then `aws cloudformation delete-stack --stack-name edms-aurora`. |

The follow-up CFN replacement (`rds-postgres.yaml`) hasn't been deployed yet, so there's no second wedged stack to clean up.

### How the GitHub Actions deploy now flows

```
push to main
   │
   ▼
[CI] (ubuntu-latest, Python 3.12)
   ├── checkout
   ├── set up Python 3.12
   ├── cache pip
   ├── pip install -r requirements.txt + requirements-dev.txt
   ├── psql apply infra/schema.sql
   ├── pytest tests/ --ignore=tests/integration
   └── python scripts/smoke_aggregation.py

[Deploy to AWS] (parallel)
   ├── checkout
   ├── configure AWS credentials       (uses repo secrets)
   ├── login to ECR                    (auth only)
   ├── ensure ECR repo exists          (self-heal — describe || create)
   ├── docker build + tag + push       (image:GITHUB_SHA + image:latest)
   ├── substitute ACCOUNT_ID in task_definition.json   (sts:GetCallerIdentity + sed)
   ├── render task definition          (amazon-ecs-render-task-definition@v1, only swaps `image`)
   └── deploy to ECS                   (RegisterTaskDefinition + UpdateService, waits for stability)
```

Both workflows also accept `workflow_dispatch` for manual reruns from the Actions UI.

---

## Endpoint surface (post-indexer)

Auth: every endpoint except `/health`, `/ready`, and `/dashboard` requires `X-API-Key`. Local key is `edms_dev_key` (override via `EDMS_API_KEY`).

| Surface | Endpoint | Notes |
|---|---|---|
| **Loan / borrower** | `POST /loans` | original ApplicationSubmittedEvent |
| | `POST /loans/document`, `POST /documents/upload` | aliases |
| | `GET /loan/{los_id}/applicant-id` | reverse lookup |
| | `GET /applicant/{id}/income-profile` | Redis → PG fallback |
| | `GET /applicant/{id}/credit-profile` | |
| **Universal ingestion** | `POST /ingest/{pdf,image,email,chat,form,csv,xml}` | Phase C |
| | `GET /applicant/{id}/raw-ingestion` | per-applicant pipeline state |
| | `GET /ingest/{ingest_id}/raw` | one row |
| | `POST /ingest/{ingest_id}/reprocess` | re-extract from S3 bytes |
| | `GET /pipeline/failed?limit=` | system-wide failures |
| **MISMO / LOS** | `POST /loans/from-los`, `POST /ingest/los` | Phase 0 universal LOS |
| | `GET /resolve/external/{system}/{id}` | reverse lookup |
| | `GET /mismo/doc-types` | discoverability |
| **Document graph** | `GET /applicant/{id}/graph[/summary]` | nodes + edges |
| | `GET /applicant/{id}/conflicts` | contradicts edges |
| | `POST /applicant/{id}/navigate` | Q&A over graph |
| | `POST /applicant/{id}/reconcile` | force re-run |
| **Property (Phase B)** | `POST /properties` | create property row |
| | `GET /property/{id}/profile` | versioned PropertyProfile |
| | `GET /property/{id}/pipeline-state` | per-property doc state |
| | `POST /ingest/property-doc` | multipart upload |
| **Application context (Phase C)** | `GET /application/{id}/context` | the Decision OS one-shot |
| | `GET /application/{id}/readiness` | flags only |
| | `POST /application/{id}/refresh-context` | force re-assemble |
| | `GET /application/{id}/dti` | DTI breakdown |
| **Vendor returns (Phase D)** | `POST /ingest/vendor-return` | universal receiver (aus/fraud/voe/ssn/ofac/flood) |
| | `GET /application/{id}/vendor-checks` | flat summary |
| | `POST /application/{id}/run-vendor-checks` | demo: synthetic returns through every adapter |
| **Persona slices (Phase E)** | `GET /application/{id}/context/{income\|credit\|property\|compliance\|fraud}` | one slice per Decision OS persona |
| **Webhooks (Phase E)** | `POST /webhooks` | register `{name, url, secret?, events?}` |
| | `GET /webhooks` | list |
| | `DELETE /webhooks/{id}` | deactivate |
| | `GET /webhooks/{id}/deliveries?limit=` | delivery audit |
| **Versioning (Phase E)** | `GET /application/{id}/context/history?limit=` | versions list |
| | `GET /application/{id}/context/at/{ts}` | point-in-time replay |
| **Checklist (Phase E)** | `GET /application/{id}/missing-documents` | catalog-driven |
| **Observability (Phase F)** | `GET /dashboard` | public HTML, refresh 15s |
| | `GET /application/{id}/pipeline-state` | full machine-readable rollup |
| | `GET /application/{id}/timeline` | sorted event log |
| **Incremental indexer** | `GET /indexing/status?source=s3` | watermark + last run |
| | `POST /indexing/run` | `{source, dry_run}` |
| | `GET /indexing/runs[?source=&limit=]` | history |
| | `GET /indexing/runs/{run_id}` | detail |
| | `PUT /indexing/watermark` | `{source, timestamp}` — admin re-index |

---

## Persistence fixes that landed during Phase A

These were latent bugs surfaced by `simulate_local.py`:

1. **FK violation** on `applications.applicant_id` — service.py never wrote the
   golden record to Postgres before saving the application. Fix: persist
   `gr.identity_xrefs` and `model_dump()` of the golden record before
   `save_application`.
2. **asyncpg date binding** — passing ISO strings to `date` / `timestamptz`
   columns failed even with `::date` casts (cast helps SELECT, not INSERT).
   Added `_to_date` / `_to_ts` helpers in `postgres_store.py`.
3. **Schema unique-constraint bug** — `applicant_identity_xref UNIQUE
   (source_system, source_id)` blocked joint applications (primary +
   co-borrower share one LOS-ID). Changed to `(applicant_id, source_system,
   source_id)`. Live ALTER + schema.sql edit + ON CONFLICT update.
4. **GET response shape mismatch** — `simulate_local.py` reads
   `data["source"]` and `data["data"]`. `IncomeProfileResponse` /
   `CreditProfileResponse` now expose both alongside legacy `profile`/`cached`.
5. **Missing `/loans/document` alias** — simulator step 4 calls a path that
   didn't exist; added as alias for `/documents/upload`.

Also fixed: `S3Client.upload_document` accepts `extension` + `content_type`
so the JPG driver's license gets the right key suffix and mime type.

---

## Universal ingestion architecture (Phase A–E delta on top of existing ARCHITECTURE.md)

```
caller payload
       │
       ▼
IngestRouter.detect_channel()   ── content-based sniffing (PDF magic bytes,
       │                          JFIF/PNG/TIFF headers, XML <?xml,
       ▼                          chat = list[{role,content}], etc.)
ChannelType   ──► IngestRouter.route() ──► adapter
                                              │
                            ┌─────────────────┼─────────────────┐
                            ▼                 ▼                 ▼
                       deterministic     Claude-based       hybrid
                       (api/pdf/form/    (chat/image/       (email = body
                        csv/xml)          email body)        Claude + attachments
                                                             via pdf/image)
                            │
                            ▼
                NormalizedIngestEvent  (shared shape — channel-agnostic)
                            │
                            ▼
                AggregationService.handle()
                    ├── API channel: maps to ApplicationSubmittedEvent (full pipeline)
                    └── all other channels: NotImplementedError (BUILD 12 deferred)
```

`SOURCE_CONFIDENCE_RANKING` (in `core/ingestion/confidence.py`) ranks
sources for `ConfidenceResolver`:

```
IRS_TRANSCRIPT  0.99   PAYROLL_API  0.97   W2_PDF       0.95
PAYSTUB_PDF     0.93   BANK_STMT_PDF 0.90   FORM_1040_PDF 0.90
API_JSON        0.88   WEB_FORM     0.85   CHAT          0.80
EMAIL_BODY      0.75   VERBAL_STATED 0.50
```

Conflicts flagged when numeric values diverge >10% across sources.

### Endpoint map

| Path                       | Channel       | Claude needed |
|----------------------------|---------------|---------------|
| `POST /loans`              | API           | no            |
| `POST /loans/document`     | (alias)       | no            |
| `POST /documents/upload`   | DOCUMENT_UPLOADED | no       |
| `POST /ingest/pdf`         | PDF_UPLOAD    | optional fallback |
| `POST /ingest/image`       | IMAGE_UPLOAD  | yes (vision) |
| `POST /ingest/email`       | EMAIL         | optional (body); attachments don't need it |
| `POST /ingest/chat`        | CHAT          | yes |
| `POST /ingest/form`        | FORM          | no |
| `POST /ingest/csv`         | CSV_BATCH     | no |
| `POST /ingest/xml`         | XML           | no |

Errors:
- `503` — `ClaudeUnavailable` (no API key)
- `502` — Anthropic upstream error (e.g. quota), with the upstream message in `detail`
- `500` — anything else (unhandled)

---

## Open follow-ups

### Resolved in earlier sessions

- ✅ **Leaked AWS key** `AKIAZBPIELTUVVBGFZHN` — **deactivated**.
- ✅ **`XRefStore` in-memory bug** — fixed by Phase 0.5. `hydrate_from_postgres()`
  loads existing applicants on lifespan startup; `next_sequence()` resumes
  past the highest stored id; SSN + source-id indexes rebuilt.
- ✅ **Phase A schema applied to RDS prod**. `raw_ingestion` table + 4 indexes
  live; verified via `scripts/watch_pipeline.py --live`.

### Resolved this session (doc-upload path)

Three latent bugs in the `POST /loans/document` (alias of `/documents/upload`) path, surfaced by `scripts/demo_loan.py --live` reporting `income_verified=false`, `qualifying_monthly=$0`, and `document_count=0` despite four PDFs in S3.

- ✅ **`extracted_fields` nesting in caller payloads** — `IncomeAssembler` reads W2 fields (`box1_wages`, `tax_year`, etc.) directly off the doc dict (`core/income/rules.py:30`), so the demo's `{"extracted_fields": {...}}` envelope buried the values out of reach. Fixed in `scripts/demo_loan.py` by spreading `doc["data"]` at the top level of the `all_documents` payload.

- ✅ **Stale `graph:{applicant_id}` cache** (`736bf97`) — `_persist_and_reconcile_documents` only invalidated on conflict edges, so the very first doc upload left `/graph/summary` serving the pre-insert `document_count`. Added `RedisStore.invalidate_graph_summary` (graph-only — `invalidate_income_profile` would clobber the income/credit caches `_run_assembly` had just warmed) and call it unconditionally after the persist loop. Also added `income_assembly_inputs` / `income_assembly_result` structured logs around the assembler call so `$0` qualifying is debuggable from the field shapes the assembler actually saw.

- ✅ **`_handle_document_uploaded` ignored co-borrower context** (`07a5bd2`) — passed `co_applicant_id=None` and `documents=p.get("all_documents", [])` (request-only) to `_run_assembly`. Two cascading symptoms: (a) co-borrower W2s filed under the primary's `applicant_id` because `_persist_and_reconcile_documents`'s role-routing branch never fired, and (b) every non-W2 upload re-assembled income from a single doc, dropping primary qualifying back to `$0` after STEP 2. Now hydrates `co_applicant_id` + `loan_data` from `get_application` (or `get_application_by_applicant` fallback) and merges the cumulative current doc set from Postgres for both borrowers via the new `_merge_request_with_indexed_docs` helper. The helper lifts `extracted_fields` back to the top level on the way out so the assembler keeps seeing `box1_wages` where it expects it. New docs in the request override existing rows by `document_id` so re-uploads with corrected fields win.

- ✅ **`document_index` upserts couldn't re-attribute** (`3c631b7`) — `save_document`'s `ON CONFLICT (document_id) DO UPDATE SET` excluded `applicant_id` / `application_id` / `borrower_role`. Once a row was inserted with the wrong attribution, no upsert could correct it. Added all three to the SET list. The next `--live` run after deploy migrated `DOC-LOS-DEMO-001-W2_CURRENT-co_borrower` from `APL-00003-P` (3 docs, all `role=primary`) to `APL-00004-C` (1 doc, `role=co_borrower`, `box1_wages=56200`).

Verified end-to-end against prod: `income:APL-00003-P qualifying_monthly: $12,383` (= (92,400 + 56,200) / 12), stable across all 4 doc uploads, served from cache. Per-borrower attribution and graph counts match the data.

`scripts/demo_loan.py` also gained cache-bypass probes (`GET /admin/table-count/{document_index, income_profiles, credit_profiles}` after each step) so future runs can confirm rows are landing even when `/graph/summary` is mid-cache. The endpoint depends on `PostgresStore.get_table_count` (uncommitted as of this session — see follow-up below).

### Resolved this session (Phases B → indexer)

- ✅ **Property layer** — schema (`properties`, `property_profiles`) applied locally; PITI math live; PropertyAssembler + 5 generators + extractors + 4 endpoints + 23 tests.
- ✅ **One-call ApplicationContext** — borrower + property + vendor folded into one cached read shape, with TTL-30m invalidation hooks on every income / property re-assembly.
- ✅ **Vendor return adapters** — DU/LP, Socure/LexisNexis, TWN/Equifax VOE, SSA SSN, Treasury OFAC. Synthetic generators for the demo path.
- ✅ **Persona slices + webhooks** — 5 slices, webhook fan-out with HMAC, context_versions audit trail, point-in-time replay endpoint.
- ✅ **Observability** — `/dashboard` HTML, `/pipeline-state`, `/timeline`. `watch_pipeline.py --full` drives the complete scenario end-to-end.
- ✅ **Incremental indexer** — watermark + S3Scanner + BatchIndexer + AsyncIOScheduler. `simulate_s3_edms.py` validates the skip-unchanged-applicant property.
- ✅ **FakePostgresStore.save_document upserts on document_id** — caught by the indexer test where `_run_assembly` re-saves docs already persisted by the indexer; previously appended duplicates that production's `ON CONFLICT DO UPDATE` would have collapsed.

### Production deploy of Phases B → indexer (NOT YET DEPLOYED)

The local docker-compose has every phase applied. Production ECS still runs Phase 0/0.5/A.

1. **Apply schema deltas to prod RDS.** New tables since the last prod apply: `properties`, `property_profiles`, `webhooks`, `webhook_deliveries`, `context_versions`, `indexing_watermarks`, `indexing_runs`. Plus `applications.property_id` ALTER. Use the same `scripts/apply_schema.py` ECS one-off task pattern that landed Phase A.
2. **Push image with `apscheduler` deps.** `requirements.txt` gained `APScheduler==3.10.4` + `pytz` + `tzdata` + `tzlocal`. Re-pin via `pip freeze` and rebuild before deploy.
3. **Decide whether to run the scheduler in ECS.** `ENABLE_SCHEDULER` is off by default. In ECS, only ONE of the N running tasks should run the scheduler (otherwise N concurrent batch indexers will fight over the watermark). Two clean options:
   - Run one ECS service for the API (desired=N, `ENABLE_SCHEDULER=false`) and a separate ECS service for the scheduler (desired=1, `ENABLE_SCHEDULER=true`).
   - Or: leave `ENABLE_SCHEDULER=false` and trigger `POST /indexing/run` from EventBridge on a 15-minute schedule. Simpler, no service-count fan-out.
4. **`AssignPublicIp: ENABLED`** is acceptable for the default-VPC dev deploy but should flip back to `DISABLED` when this moves to a real VPC with private subnets + NAT gateway. Documented in commit `fbd03d5`.
5. **Wire `infra/cloudformation/secrets.yaml`** so the three `edms/*` secrets are stack-managed — currently their ARN suffixes are hard-coded in `task_definition.json`, so any rotation breaks the deploy.

### Pre-existing AWS / production (still open)

1. **Production data cleanup** (Phase 0/0.5 collateral damage):
   - **James Okafor's data was overwritten.** First Phase 0 prod test triggered
     the (now-fixed) overwrite bug — Maya Patel's data clobbered James's row at
     `APL-00001-P`. Original LOS-PROD-001 application now points at an
     applicant whose person details no longer match. Recovery requires the
     original ingest payload from logs.
   - **Maya's row has `ssn_hash=""`** stored from before the connector fix.
     Won't collide with future real-hash inserts (everyone gets a real hash now)
     but is itself broken — can't be deterministically matched on SSN.
     One-off `aws ecs run-task` with an UPDATE statement to recompute her hash
     would close it out.
2. **Clean up `edms-aurora` wedged stack + 2 orphan resources** — admin to
   delete IAM role `edms-aurora-RDSProxyRole-oBsVFktLB9Z3` (inline policies
   first) and SG `sg-0050f77a029b4642f`, then `aws cloudformation delete-stack
   --stack-name edms-aurora`. (Pre-dates this session.)
3. **CFN drift** — RDS, ElastiCache, and Secrets Manager were all
   admin-provisioned out-of-band, not via the repo's CFN templates. Future
   schema / config changes need to either route through admin-provisioned
   parameter groups or finally take ownership in `infra/cloudformation/`.

### Application

1. **`_handle_normalized_ingest_event` only handles API channel** — for chat /
   pdf / etc., the adapter produces a `NormalizedIngestEvent` but the service
   raises `NotImplementedError`. BUILD 12 (full ConfidenceResolver merge into
   the income profile) was deferred. Today the `/ingest/*` endpoints return
   the event without merging into a profile.
2. **`claude_extractor` body** — Phase B placed the file with the documented
   signature; Phase C extension never replaced the `NotImplementedError` body
   (the pdf_adapter's `claude_fallback` path catches it gracefully). Implement
   when there's a real document type that pymupdf can't handle.
3. **`/ingest/csv` doesn't ingest** — the endpoint returns the report and
   parsed signals but doesn't drive applicants into Postgres. Wire each event
   through the aggregation service when BUILD 12 is done.
4. **`idx_applicant_ssn` is a strict UNIQUE** — if any caller forgets to
   populate `ssn_hash`, the second arrival hits the constraint. Defensive
   alternative: convert to a partial index (`WHERE ssn_hash <> ''`) so empty
   hashes don't collide. Trade-off: hides connector bugs at insert time.
   Current preference: keep strict and rely on connector tests.
5. **Hydration is sequential and load-everything-into-memory.** Fine for
   thousands of applicants, painful at hundreds of thousands. When the row
   count grows, switch to a Postgres-backed `XRefStore` that does point
   lookups instead of pre-loading.
6. **`raw_ingestion.document_id` stays NULL on success** — the FK fires only
   when an actual `document_index` row exists. Phase A persists the raw
   payload but doesn't create the index row itself; the aggregation service
   / `/ingest/los` create the index row but don't backfill
   `raw_ingestion.document_id`. One-line fix when the linkage matters: have
   `service._persist_and_reconcile_documents` and `/ingest/los` look up the
   most recent matching `raw_ingestion` row by `(applicant_id, source_channel)`
   and update its `document_id` after `save_document` succeeds.
7. **`/dashboard` is unauthenticated** — intentional, so a browser tab can sit
   on it. It's a read-only summary; nothing sensitive leaks (just LOS IDs
   and aggregate counts). If product wants this gated later, wrap with
   `Depends(verify_api_key)` and switch to a `?api_key=` query string for the
   browser refresh, or move it to a separate admin port.
8. **Indexer scheduler in ECS needs single-task gating.** With desired-count=N
   and `ENABLE_SCHEDULER=true`, every task fires its own batch indexer on the
   same 15-minute clock and they race on the watermark. Two clean options:
   split the API and scheduler into separate ECS services (scheduler
   desired=1), or trigger `POST /indexing/run` from EventBridge instead of
   APScheduler. See "Production deploy of Phases B → indexer" above.
9. **Incremental indexer doesn't track per-applicant watermarks.** It uses
   one source-level watermark. Means: if applicant A's docs are processed
   in run T and applicant B uploads new ones during T, B's docs don't get
   indexed until run T+1 — fine for a 15-minute cadence but could surprise
   on demos. Multi-watermark per source/applicant is a Phase G item if it
   matters.
10. **Webhook delivery is in-process and synchronous-per-webhook.** Every
    `assemble()` blocks on the round-trip to each subscriber. Fine for a
    handful of webhooks; will need a queue (SQS + worker) at scale.
11. **`/dashboard` does not flush stale Redis context.** It reads whatever
    `redis.get_application_context` returns; if a context was last cached at
    T-29min and an event-driven invalidation didn't fire (e.g. a back-end
    process inserted into `document_index` directly), the dashboard shows
    stale numbers until TTL elapses. Hit `POST /refresh-context` to force.
12. **`/admin/table-count/{table}` returns 500 in prod** — the route at
    `routes.py:2185` calls `pg.get_table_count(table_name)` but the helper
    lives uncommitted on the working tree. Commit + push to unblock the
    cache-bypass probes that `scripts/demo_loan.py` prints after every step.
    Schema is trivial:
    ```python
    async def get_table_count(self, table_name: str) -> int:
        # Caller must whitelist; SQL identifier interpolated unquoted.
        val = await db.fetchval(f"SELECT COUNT(*) FROM {table_name}")
        return int(val or 0)
    ```
13. **Document reconciler produces 0 edges across 4 docs.** Two W2s sharing
    `tax_year=2023`, `borrower_role` semantics, and overlapping fields ought
    to at least CONFIRM. After the doc-upload path fix, `extracted_fields`
    are flat at the top level — but `core/graph/reconciler.py` still produces
    empty `relationships` / `confirmations` / `conflicts` arrays. Most likely
    the field-pair selection logic in the reconciler doesn't match what's
    on the row shape today. Verify by reading the reconciler against the
    current shape of `_persist_and_reconcile_documents`'s `saved_doc`.
14. **Confidence values clamped uniformly to `0.95`.** Demo sends `0.94`
    (W2), `0.95` (credit), `0.97` (appraisal); `document_index` rows all
    show `0.95`. Something between `_persist_and_reconcile_documents` and
    `save_document` overrides the caller-supplied confidence — likely a
    per-doc-type catalog floor analogous to `routes.py:931-934` for
    property docs. Cosmetic for the demo, but corrupts any downstream
    weighting that relies on the value the caller sent.
15. **`/application/{id}/context` reports `combined_qualifying_monthly=0`
    while `/applicant/{id}/income-profile` reports `$12,383`.** The
    `ContextAssembler` isn't reading the same income source as the
    income-profile endpoint after the doc-upload path fix. Check
    `core/context/assembler.py` — it probably reads from a stale field or
    expects the assembler return shape that doesn't match what
    `_handle_document_uploaded` now produces. Manifests as all readiness
    flags showing `✗` even when income and credit are clearly populated.

---

## Common pitfalls (encountered & fixed)

- **Bash sessions don't persist between tool calls** — env vars exported in
  one Bash call are gone in the next. When running multi-step shell flows,
  bundle them in one command or re-source `.env` each time.
- **Windows console uses cp1252** — `simulate_local.py` uses Unicode
  box-drawing chars; set `PYTHONIOENCODING=utf-8` or get
  `UnicodeEncodeError`.
- **reportlab stamps a CreationDate** — same RNG seed → identical content,
  but PDF bytes differ. Compare metadata, not bytes, for determinism tests.
- **`python-multipart` is required** for the `UploadFile` / `Form`
  endpoints. It's pinned in `requirements.txt` now (Phase A).
- **`fakeredis` is required** for the test suite (the `RedisStore` import
  hits `USE_FAKE_REDIS=true` in `tests/conftest.py`). It's in
  `requirements-dev.txt`.
- **Anthropic credit balance** — surfaces as `BadRequestError` with code 400.
  Phase E maps that to HTTP 502 with the upstream message; the email pipeline
  no longer breaks on it.
- **ECS `secrets:` block injects the WHOLE SecretString.** If a secret is
  stored as JSON, the env var ends up containing the JSON blob — not the
  field value. Use ECS's JSON-key syntax (`<arn>:<json-key>::`) to extract
  one field at task start. Bit us on `API_KEY` until commit `920a15e`.
- **Git Bash + AWS CLI path mangling.** `/ecs/edms-simulator` becomes
  `C:\Program Files\Git\ecs\edms-simulator` for AWS CLI args that look like
  Linux paths. Workaround: `MSYS_NO_PATHCONV=1` prefix on the command, or
  double-leading-slash (`//ecs/...`).
- **Python on Windows can't see Bash's `/tmp`.** `curl > /tmp/x.txt`
  (bash) followed by `python -c "open('/tmp/x.txt')"` (Windows Python) fails
  with FileNotFoundError. Either pipe through stdin or use a Windows-style
  path that both shell and Python interpret the same way.
- **RDS Postgres rejects no-SSL connections by default.** Param group has
  `rds.force_ssl=1`. Pass `ssl='require'` to `asyncpg.create_pool` (commit
  `60c4d68`). The error message is misleading — it says "no pg_hba.conf entry"
  with "no encryption" tacked on the end, looks like an auth problem first.
- **ElastiCache TransitEncryptionEnabled requires `ssl=True` on the redis
  client.** Without it, the handshake silently fails. Plus the corresponding
  env var (`REDIS_SSL=true`) needs to be in the task definition.
- **LOS connectors must compute `ssn_hash`.** If they only set `ssn_last4`
  and leave the hash empty, two such inserts collide on `idx_applicant_ssn`.
  All connectors now use `_hash_or_empty(ssn)` (commit `ab27bf5`).
- **A `;` inside a `--` comment breaks naive `split(';')`.** First Phase A
  schema apply against prod failed because the comment block describing
  why `applicant_id` is not a FK contained "system; the audit row". The
  splitter cut the CREATE TABLE in half. `apply_schema.py` now strips
  `--` line comments before splitting (commit `ab4b547`); the comment
  in `infra/schema.sql` was rewritten to drop the inline `;` for good
  measure.
- **FakePostgresStore.save_document used to append, not upsert.** Real
  Postgres has `ON CONFLICT (document_id) DO UPDATE`; the fake just
  appended. Surfaced when the indexer saved a doc and `_run_assembly`
  re-saved it through `_persist_and_reconcile_documents`, producing
  2 rows where prod would have 1. Conftest now upserts by `document_id`.
  Same trap could exist for any other dictionary-row-keyed table — check
  before mirroring.
- **JSONB column auto-decode list in `_row_to_dict`** has to be kept in
  sync each time a new JSONB column is added. Currently includes:
  `address_current`, `identity_xrefs`, `application_ids`, `profile_data`,
  `extracted_fields`, `source_value`, `target_value`, `piti_components`,
  `context_data`, `payload`, `events`, `error_details`. Forgetting to add
  a column means callers see a JSON string instead of a dict.
- **`AUS_DU_FINDINGS` ⟂ `AUS_LP_FINDINGS`.** The `/missing-documents`
  catalog treats either one as satisfying the AUS slot — the indexer's
  filename detection picks DU vs LP from `du_findings` / `lp_findings`
  patterns. If a real LOS sends `aus.xml` with no marker in the name,
  it currently falls back to `UNKNOWN`; the connector path handles that
  via `MISMOMapper.detect_type_from_content`.
- **APScheduler with multiple ECS tasks fights over the watermark.** See
  follow-up #8 in the Application list. Split into a separate scheduler
  service or use EventBridge instead.

---

## Local env (.env keys)

| Var | Where used | Notes |
|-----|------------|-------|
| `ENVIRONMENT` | `local` flag | sometimes branches behavior |
| `DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD` | `core/storage/db.py` | port **5433** locally |
| `REDIS_HOST/REDIS_PORT/REDIS_PASSWORD/REDIS_SSL` | `core/storage/redis_store.py` | port **6380** locally |
| `USE_LOCAL_STORAGE` | `core/storage/s3_client.py` | `true` writes to `local_storage/` |
| `LOCAL_STORAGE_PATH` | `s3_client.py` | default `./local_storage` |
| `USE_AWS_SECRETS` / `USE_AWS_SQS` / `USE_FAKE_REDIS` | various | gates AWS-vs-local backends |
| `AWS_REGION` / `AWS_S3_BUCKET` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | aws clients | unused in dev |
| `API_KEY` | `api/routes.py:verify_api_key` | header `X-API-Key` |
| `DECISION_OS_URL` | downstream call placeholder |  |
| `ANTHROPIC_API_KEY` | `core/ingestion/_claude_client.py` | required for chat / image / email body. Account-level errors are fine — Phase E handles them. |
| `ENABLE_SCHEDULER` | `api/main.py` lifespan | `true` starts APScheduler interval job for the indexer. Off by default — see follow-up #8 before flipping in ECS. |
| `INDEX_INTERVAL_MINUTES` | `api/main.py` lifespan | indexer cadence; default `15`. |
| `EDMS_API_URL` / `EDMS_API_KEY` | `scripts/watch_pipeline.py`, `scripts/simulate_s3_edms.py` | overrides for the dev scripts. Defaults: `http://localhost:8001` and `edms_dev_key`. |

---

## Testing strategy

- **Unit tests** in `tests/core/**` — pure deterministic, no network.
- **Round-trip tests** in `tests/core/documents/` — generator → extractor →
  assert metadata recovered.
- **Mocked Claude tests** in `tests/core/ingestion/test_chat_adapter.py`
  (and friends) — `tests/core/ingestion/_fakes.py::FakeClaudeClient` returns
  canned JSON. Adapters take `client=` so tests inject without env.
- **Live tests** gated on `ANTHROPIC_API_KEY` via `pytest.mark.skipif`.
  Set the key locally (and have credit) to exercise them. Two such tests
  exist: `test_chat_adapter::test_live_chat_extraction_returns_valid_event`
  and `test_claude_extractor::test_extract_phase_c_implementation_pending`.

```bash
# normal run (skips live)
.venv/Scripts/python -m pytest tests/ -q

# include live tests (will spend Anthropic credits)
set -a; source <(grep -v '^#' .env | grep -v '^$' | sed 's/^/export /'); set +a
.venv/Scripts/python -m pytest tests/ -q
```

---

## Helpful one-liners

### Local dev

```bash
# truncate everything for a clean simulate run
docker exec edms-simulator-postgres-1 psql -U edms -d edms -c \
  "TRUNCATE applicants, applicant_identity_xref, applications, income_profiles, credit_profiles, document_index RESTART IDENTITY CASCADE;"
docker exec edms-simulator-redis-1 redis-cli FLUSHDB

# tail the API log
tail -f .logs/uvicorn.log

# inspect a Redis key
docker exec edms-simulator-redis-1 redis-cli GET "income:APL-00001-P"

# list documents written by S3Client local fallback
ls -R local_storage/

# refresh the lockfile after adding a runtime dep
.venv/Scripts/python -m pip install <pkg>
.venv/Scripts/python -m pip freeze | grep -v -E '^(pytest|pytest-asyncio|fakeredis|iniconfig|pluggy|Pygments|sortedcontainers)' > /tmp/freeze
# manually merge /tmp/freeze into requirements.txt preserving the section comments

# walk the full pipeline against local API
.venv/Scripts/python scripts/watch_pipeline.py --full

# drop synthetic files into local_storage and trigger the indexer
.venv/Scripts/python scripts/simulate_s3_edms.py
.venv/Scripts/python scripts/simulate_s3_edms.py --dry-run
.venv/Scripts/python scripts/simulate_s3_edms.py --watch     # 30s loop

# trigger one indexing run + see status
curl -s -X POST http://localhost:8001/indexing/run \
  -H "X-API-Key: edms_dev_key" -H "Content-Type: application/json" \
  -d '{"source":"s3"}' | jq
curl -s http://localhost:8001/indexing/status -H "X-API-Key: edms_dev_key" | jq

# open the dashboard (no auth)
open http://localhost:8001/dashboard
```

### AWS / production observability

```bash
# poll a CFN stack until it reaches a terminal state
until s=$(aws cloudformation describe-stacks --stack-name STACK --query 'Stacks[0].StackStatus' --output text 2>/dev/null) \
  && [[ "$s" != *_IN_PROGRESS && -n "$s" ]]; do sleep 15; done; echo "$s"

# why a stack failed (CREATE_FAILED events with reasons)
aws cloudformation describe-stack-events --stack-name STACK \
  --query 'StackEvents[?ResourceStatus==`CREATE_FAILED`].{Resource:LogicalResourceId,Reason:ResourceStatusReason}' \
  --output table

# what just happened to a fargate task that died
aws ecs list-tasks --cluster edms-simulator-cluster --desired-status STOPPED --max-results 3 \
  --query 'taskArns' --output text \
  | xargs -n1 -I{} aws ecs describe-tasks --cluster edms-simulator-cluster --tasks {} \
    --query 'tasks[0].{StoppedReason:stoppedReason,StopCode:stopCode,Containers:containers[0].reason}'

# tail container logs (after the service is up)
aws logs tail /ecs/edms-simulator --follow --region us-east-1

# hit the live ALB
curl -s http://edms-simulator-alb-1374683374.us-east-1.elb.amazonaws.com/health

# rerun a github action manually (workflow_dispatch is wired on both)
# UI: Actions > [workflow] > Run workflow > main
```

### Bootstrap (when starting fresh in a new account)

```bash
# 1. discover the default VPC + subnets
VPC_ID=$(aws ec2 describe-vpcs --query "Vpcs[?IsDefault==\`true\`].VpcId" --output text)
SUBNETS=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" \
  --query 'Subnets[*].SubnetId' --output text | tr '\t' ',')

# 2. create the three secrets first (or use AWS Console)
for s in edms/aurora/credentials edms/redis/endpoint edms/api/keys; do
  aws secretsmanager create-secret --name "$s" --secret-string '{"placeholder":"replace"}' --region us-east-1
done

# 3. pre-create the task role (with broad managed policies, since CFN no longer manages it)
aws iam create-role --role-name edms-simulator-task-role \
  --assume-role-policy-document file://.aws-bootstrap/trust-policy.json
for p in AmazonS3FullAccess AmazonSQSFullAccess SecretsManagerReadWrite CloudWatchLogsFullAccess; do
  aws iam attach-role-policy --role-name edms-simulator-task-role \
    --policy-arn arn:aws:iam::aws:policy/$p
done

# 4. deploy DB + cache (admin needed for RDS)
aws cloudformation deploy --template-file infra/cloudformation/rds-postgres.yaml --stack-name edms-rds ...
aws cloudformation deploy --template-file infra/cloudformation/elasticache.yaml --stack-name edms-cache ...

# 5. push to main → GHA builds image + deploys ECS service
```
