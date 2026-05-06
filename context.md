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

- Branch: `main`, all committed and pushed to `https://github.com/krishnamami/edms-simulator`
- Tests: **165 passing, 2 skipped** (live-API tests gated on `ANTHROPIC_API_KEY`)
- `simulate_local.py` runs end-to-end with exit 0 on a clean DB
- **Production ECS service is live + DB-backed** at `http://edms-simulator-alb-1374683374.us-east-1.elb.amazonaws.com`. `/health` 200, `/docs` 200, **DB-touching endpoints (`/applicant/{id}/graph/summary`) return 200 with `source: "database"`**.
- **MISMO + LOS endpoints live in prod**: `/loans/from-los`, `/ingest/los`, `/resolve/external/...`, `/mismo/doc-types` all verified end-to-end.
- **XRefStore hydrates from Postgres at startup** so `applicant_id` sequence + SSN / source-id lookups survive across redeploys (Phase 0.5).
- **Raw storage layer live in prod (Phase A)**. Every inbound `/ingest/*` payload is persisted to S3 + `raw_ingestion` BEFORE extraction. `POST /ingest/{ingest_id}/reprocess` re-runs extraction from the stored bytes. Verified end-to-end via `scripts/watch_pipeline.py --live`.

Latest commits (top of `main`):

```
ab4b547  fix(ops): apply_schema.py strips comments before splitting on ';'
00a7d26  feat(raw): Phase A — raw storage layer before extraction
2990e98  docs: refresh context.md after Phase 0 + 0.5 prod bootstrap
ab27bf5  fix(los): connectors must compute ssn_hash so applicants don't collide
047aa6d  fix: hydrate XRefStore from Postgres at startup
c5b142a  feat(mismo): Phase 0 — MISMO compatibility + LOS connectors + external IDs
920a15e  fix: extract decision_os_api_key field from secret instead of injecting whole JSON
2d7d548  fix: enable SSL for ElastiCache Redis in production
60c4d68  fix(db): enable SSL on the asyncpg pool when USE_AWS_SECRETS=true
8c8cc5b  chore(ops): scripts/apply_schema.py — one-off RDS schema bootstrap
a359ce6  chore: track IAM bootstrap policies + gitignore .logs/
cac3b77  docs: update context.md with AWS production bootstrap notes
fbd03d5  fix: enable public IP for ECS tasks in default VPC subnets
1a1002a  infra(ecs): take TaskRole ARN as a parameter, drop inline role
6a66536  ci: add workflow_dispatch trigger to both workflows
e41d244  ci(deploy): substitute ACCOUNT_ID in task_definition.json before render
95c60ce  ci(deploy): self-heal missing ECR repo before push
dc9d18b  ci: bump Python 3.10 -> 3.12 to match pinned requirements
4137095  fix: pin exact package versions to prevent pip backtracking in Docker
ead0fd3  config: production AWS endpoints + full secret ARNs
43f459d  chore: rename aurora.yaml -> rds-postgres.yaml
1eb884d  fix: replace Aurora with simple RDS Postgres for initial deploy
19567d2  fix: aurora engine version 15.4 -> 15.8
ef0d6fa  docs: add context.md (session-resume notes) + docs/PRD.md
e1fa6b6  feat(ingestion): Phase E — resilience for upstream Claude failures
bff35cc  feat(simulator): Phase D — 7-step walkthrough exercising all channels
0d0b985  feat(ingestion): Phase C — chat, image, email, pdf, form, csv, xml adapters
08ee2e9  feat(documents): Phase B — generators + pymupdf extractor (no Claude)
75e3a46  feat(ingestion): Phase A — universal ingestion plumbing + persistence fixes
```

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

### Resolved this session

- ✅ **Leaked AWS key** `AKIAZBPIELTUVVBGFZHN` — **deactivated**. Any new AWS
  CLI from this session needs fresh credentials.
- ✅ **`XRefStore` in-memory bug** — fixed by Phase 0.5. `hydrate_from_postgres()`
  loads existing applicants on lifespan startup; `next_sequence()` resumes
  past the highest stored id; SSN + source-id indexes rebuilt.
- ✅ **Phase A schema applied to RDS prod**. `raw_ingestion` table + 4 indexes
  live; `/ingest/*` endpoints route through `IngestionPipeline` and persist
  raw payloads to S3 + Postgres before extraction. Verified via
  `scripts/watch_pipeline.py --live`.

### AWS / production

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
4. **`AssignPublicIp: ENABLED`** is acceptable for the default-VPC dev deploy
   but should flip back to `DISABLED` when this moves to a real VPC with
   private subnets + NAT gateway. Documented in the commit message of `fbd03d5`.
5. **Wire `infra/cloudformation/secrets.yaml` (or admin-provision) so the
   three `edms/*` secrets are stack-managed** — currently their ARN suffixes
   are hard-coded in `task_definition.json`, so any rotation or recreate
   breaks the deploy.

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
   Current preference: keep strict and rely on connector tests
   (`test_translate_loan_distinct_ssns_produce_distinct_hashes`).
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
