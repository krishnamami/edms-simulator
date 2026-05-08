# EDMS Simulator

EDMS Simulator is a runnable, end-to-end Enterprise Data Management
Simulator for **mortgage loan origination data**. It accepts real-shape
borrower / property / vendor documents through every channel a production
LOS does — JSON API, PDF, image, email, chat, web form, CSV bulk upload,
MISMO 3.4 XML — extracts structured fields, reconciles them across docs,
assembles per-borrower income / credit / asset / identity profiles, folds
the property layer in, runs vendor-return cross-checks (DU/LP/Socure/TWN/
SSA/OFAC), and exposes everything via a single
`GET /application/{id}/context` call.

The simulator mirrors a production AWS deployment (Aurora-Postgres +
ElastiCache Redis + S3 + ECS + SQS) and runs locally on Postgres 15 +
Redis 7 in Docker. **Standalone product** — sells separately from any
downstream Decision OS.

## Quick start

```bash
# 1. Backing services (ports 5433 + 6380 to avoid host clashes)
docker compose up -d postgres redis

# 2. Python deps
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt -r requirements-dev.txt

# 3. Env (copy + fill ANTHROPIC_API_KEY for the AI extractor / chat / image paths)
cp .env.example .env

# 4. Apply schema
psql postgresql://edms:edms_dev@localhost:5433/edms -f infra/schema.sql

# 5. Run the API
.venv/Scripts/python -m uvicorn api.main:app --port 8001

# 6. End-to-end walkthrough
.venv/Scripts/python scripts/simulate_local.py
```

Default API key (local) is `edms_dev_key`. Override via `EDMS_API_KEY`
or set in `.env`.

## Architecture

```
caller payload (PDF / JSON / image / email / chat / form / CSV / XML)
        │
        ▼
IngestRouter.detect_channel()      ── content-based sniffing
        │
        ▼
channel adapter                    ── deterministic / Claude / hybrid
        │
        ▼
NormalizedIngestEvent              ── shared shape, channel-agnostic
        │
        ▼
AggregationService._run_assembly() ── per-applicant Redis advisory lock
        ├─ _persist_and_reconcile_documents → document_index (PG) + graph
        ├─ income_assembler / credit_assembler / property_assembler
        ├─ asset + identity aggregators
        └─ write-through to Redis (income / credit / asset / identity / property / context)
                │
                ▼
        GET /application/{id}/context
        — one read returns borrower / property / vendor_checks /
          loan_terms / readiness / missing_items / conflicts / DTI / LTV
```

**Source-of-truth is Postgres** — `document_index` (per-doc rows + JSONB
extracted_fields), `document_relationships` (graph edges:
confirms / corroborates / contradicts), `income_profiles` (versioned
per-borrower), `credit_profiles`, `properties` + `property_profiles`,
`indexing_watermarks` + `indexing_runs`, `webhooks` +
`webhook_deliveries`, `context_versions`. **Redis is the read cache** —
`income:{aid}`, `credit:{aid}`, `asset:{aid}`, `identity:{aid}` (TTL
4–24h), `property:{id}`, `graph:{aid}` (TTL 1h), `context:{appid}` (TTL
30m). All Redis methods are async (`redis.asyncio.Redis`).

For the data flow + component boundaries see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
For the API surface see [docs/API.md](docs/API.md). For session-resume
notes / gotchas / phase log see [context.md](context.md).

## Pipeline highlights

- **23 deterministic field extractors** covering W2 / paystub / bank /
  credit / appraisal / HOI / flood / tax / IRS transcript / 1040 /
  Schedule C/E / 1099 / K-1 / retirement / brokerage / gift / AVM /
  1004MC / purchase / URLA / rate-lock / offer-letter.
- **AI Vision fallback** — Claude (`claude-sonnet-4-6`) runs when a
  deterministic extractor returns empty. Always-graceful — no key /
  flag-off / failure all return `({}, 0.5)`. Gated on
  `ENABLE_AI_EXTRACTION=true` + `AI_EXTRACTION_MAX_PAGES=3`.
- **43+ cross-doc comparison pairs** in the reconciler:
  IRS↔W2 (5% tight), 1040↔W2 (5%), URLA stated↔documented (10%),
  AVM↔appraisal (15%), purchase↔appraisal (5%), gift↔bank,
  OFFER↔W2/paystub/VOE, K1↔1040, plus the original employment / property /
  asset cross-checks.
- **Concurrency-safe** — per-applicant Redis advisory lock
  (`assembly_lock:{aid}`, 30s TTL) serializes `_run_assembly` for the
  same applicant; bailed contenders persist their docs first so the
  holder picks them up. Indexer processes distinct applicants in
  parallel under `Semaphore(10)`.
- **Comprehensive missing-documents catalog** — 15 required slots
  (with alternates for W2_CURRENT∥W2_PRIOR, AUS_DU∥LP, HOI_BINDER∥
  HOI_DECLARATIONS) + 9 conditional slots (each with the rule that
  triggers it).
- **Webhooks** — Decision OS subscribes via `POST /webhooks` with
  optional HMAC; every assembly fans out a `context_updated` event.

## Running the test suites

**Unit tests** (329 passing, 2 skipped without `ANTHROPIC_API_KEY`):

```bash
make test
# or:
.venv/Scripts/python -m pytest tests/ --ignore=tests/integration -q
```

**Stress test** — 23 checks across 7 tests covering concurrency,
indexer/upload races, cache invalidation correctness, doc-type matrix
coverage, cross-applicant throughput, watermark rewind, webhook fan-out:

```bash
.venv/Scripts/python scripts/stress_test_indexing.py
# Single test:
.venv/Scripts/python scripts/stress_test_indexing.py --test 1
```

**End-to-end synthetic loan file** — drives a 43-document Martinez
joint mortgage application through the API in 4 timed waves and
validates every layer (completeness, income, credit, assets, identity,
property, graph, context, readiness, cross-doc consistency, co-borrower).
Prints a production-readiness report card with PASS/FAIL exit code:

```bash
# Default — 4 waves with realistic timing
.venv/Scripts/python scripts/feed_synthetic_loan.py

# Skip the timing — upload all 43 at once (faster iteration)
.venv/Scripts/python scripts/feed_synthetic_loan.py --no-waves

# Read PDFs from a custom directory if you have generated them
.venv/Scripts/python scripts/feed_synthetic_loan.py --dir path/to/pdfs
```

The script creates a property record before uploading property docs;
the 5 property doc types with reportlab generators (appraisal, title,
HOI, flood, tax) take the multipart `/ingest/property-doc` path so the
PropertyAssembler runs and surfaces appraised_value / PITI / flood_zone.

## API endpoints (one-line each)

Auth: every endpoint except `/health`, `/ready`, `/dashboard` requires
`X-API-Key`.

**Loans / borrowers**

- `POST /loans` — submit an application
- `POST /documents/upload` (alias: `POST /loans/document`) — append docs
- `GET /loan/{los_id}/applicant-id` — reverse lookup
- `GET /applicant/{id}/income-profile` — Redis → PG fallback
- `GET /applicant/{id}/credit-profile` — Redis → PG fallback

**Universal ingestion** — every channel becomes a NormalizedIngestEvent

- `POST /ingest/{pdf,image,email,chat,form,csv,xml}` — channel adapters
- `POST /ingest/los` — MISMO / Encompass connector entry point
- `POST /loans/from-los` — bootstrap loan from LOS payload
- `GET /resolve/external/{system}/{id}` — reverse-lookup external IDs
- `GET /applicant/{id}/raw-ingestion` — per-applicant pipeline state
- `POST /ingest/{ingest_id}/reprocess` — re-extract from S3 bytes

**Document graph**

- `GET /applicant/{id}/graph[/summary]` — nodes + edges
- `GET /applicant/{id}/conflicts` — contradicts edges
- `GET /applicant/{id}/field/{name}` — best-value across all sources
- `POST /applicant/{id}/navigate` — Q&A over the graph
- `POST /applicant/{id}/reconcile` — force re-run

**Property**

- `POST /properties` — create a property + link to application
- `GET /property/{id}/profile` — versioned PropertyProfile
- `POST /ingest/property-doc` — multipart upload → PropertyAssembler

**Application context** — Decision OS one-shot read

- `GET /application/{id}/context` — borrower / property / vendor /
  loan_terms / conflicts / readiness, all in one call
- `GET /application/{id}/readiness` — flags only
- `GET /application/{id}/missing-documents` — required + conditional checklist
- `POST /application/{id}/refresh-context` — force re-assemble
- `GET /application/{id}/dti` — DTI breakdown

**Persona slices** — one per Decision OS persona

- `GET /application/{id}/context/{income|credit|property|compliance|fraud}`

**Vendor returns**

- `POST /ingest/vendor-return` — universal AUS / fraud / VOE / SSN / OFAC / flood
- `GET /application/{id}/vendor-checks` — flat summary
- `POST /application/{id}/run-vendor-checks` — synthetic returns through every adapter

**Webhooks**

- `POST /webhooks` — register `{name, url, secret?, events?}`
- `GET /webhooks` / `DELETE /webhooks/{id}` — list / deactivate
- `GET /webhooks/{id}/deliveries` — delivery audit

**Versioning**

- `GET /application/{id}/context/history` — versions list
- `GET /application/{id}/context/at/{ts}` — point-in-time replay

**Observability**

- `GET /dashboard` — public HTML, refresh 15s
- `GET /application/{id}/pipeline-state` — full machine-readable rollup
- `GET /application/{id}/timeline` — sorted event log

**Incremental indexer**

- `GET /indexing/status?source=s3` — watermark + last run
- `POST /indexing/run` — `{source, dry_run}`
- `GET /indexing/runs[?source=&limit=]` — history
- `PUT /indexing/watermark` — `{source, timestamp}` — admin re-index

## Configuration

See `.env.example`. Key flags:

| Var                       | Default     | Purpose |
|---------------------------|-------------|---------|
| `USE_AWS_SECRETS`         | `false`     | Read DB / Redis / API creds from Secrets Manager |
| `USE_AWS_SQS`             | `false`     | Bind SQS consumers (otherwise direct API calls) |
| `USE_LOCAL_STORAGE`       | `true`      | Local-FS for documents instead of S3 |
| `USE_FAKE_REDIS`          | `false`     | fakeredis (CI / unit tests) |
| `ENABLE_AI_EXTRACTION`    | `true`      | Claude Vision fallback for the indexer / pdf_adapter |
| `AI_EXTRACTION_MAX_PAGES` | `3`         | Max pages sent to Claude per fallback call (cost control) |
| `ENABLE_SCHEDULER`        | `false`     | Run the AsyncIOScheduler batch indexer every 15 min |

## Project layout

```
api/                  FastAPI app + routes + middleware
core/
  aggregation/        AggregationService — central orchestrator
  context/            ContextAssembler + ApplicationContext model
  documents/
    extractors/       pymupdf + 15 income/asset/loan + claude_extractor
    generators/       reportlab W2/paystub/bank/credit + driver's license
  graph/              DocumentReconciler + COMPARISON_MAP + Navigator
  identity/           XRefStore + IdentityResolver + GoldenRecord
  income/             IncomeAssembler + per-source rules
  credit/             CreditAssembler
  property/           PropertyAssembler + extractors + 5 generators
  indexing/           BatchIndexer + S3Scanner + WatermarkStore
  ingestion/          IngestRouter + 7 channel adapters + MISMOMapper
  storage/            PostgresStore + RedisStore + S3Client
  lambda_handlers/    Lambda entry points (production wiring)
infra/
  schema.sql          Postgres schema
  cloudformation/     CFN templates (ECS, RDS, ElastiCache stubs)
docs/                 README / ARCHITECTURE / API / PRD
scripts/              CLI tools — simulate_local, stress_test, watch_pipeline,
                      feed_synthetic_loan, etc.
tests/                Unit tests (329 passing) + integration + smoke
```

## Deploy

GitHub Actions on push to `main` → builds + pushes to ECR → renders
`task_definition.json` → deploys to ECS Fargate. See
[`.github/workflows/aws.yaml`](.github/workflows/aws.yaml). Production
is at `http://edms-simulator-alb-1374683374.us-east-1.elb.amazonaws.com`
(account `621646470377`).

The Phases B → indexer + concurrency hardening + Tier-1/2/3 indexing +
Claude Vision AI fallback commits have NOT been deployed to prod yet —
prod still runs Phase 0/0.5/A. See `context.md` for the deploy checklist.
