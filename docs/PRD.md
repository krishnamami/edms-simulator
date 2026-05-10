# EDMS Simulator — Product Requirements

## 1. Problem & vision

Mortgage origination data flows through a long, brittle chain: borrower
intake → identity resolution → document extraction → income/credit assembly →
decisioning. Most teams test these stages in isolation against
captured-once fixtures. The EDMS Simulator gives engineers a **runnable
end-to-end environment** that mirrors production AWS infrastructure (Aurora,
ElastiCache, S3, ECS, SQS) and accepts real-shape data through every channel
that production does — JSON API, PDF, image, email, chat, web form, CSV bulk
upload, MISMO/IRS XML.

The vision is "one local stack you can throw any kind of data at and watch
it walk the whole pipeline" — useful for integration testing, decisioning
verification, and shape-discovery before wiring real bureau pulls or third
parties.

## 2. Personas

| Persona | What they need from the simulator |
|---------|-----------------------------------|
| **Platform engineer** integrating with Decision OS | A stable mock that returns the same shapes prod will return, plus knobs to inject conflict/edge cases. |
| **ML / extraction engineer** | A way to drop a PDF or chat transcript in and see what the extraction → confidence → assembly pipeline does with it. |
| **Underwriting / credit team** | Confidence scoring + conflict flags so they can audit which source drove a decision. |
| **Architect on call** | A fast local repro for production incidents (FK violations, schema drift, async-pool issues). |

## 3. Capabilities

### Shipped (✓)

| Area | Capability |
|---|---|
| Universal ingestion | `IngestRouter` content-detects payloads (`%PDF`, JFIF/PNG/TIFF, XML, list-of-chat-messages, etc.) and dispatches to the right adapter. |
| Channel adapters | API · PDF · Image · Email · Chat · Form · CSV · XML — all wired through `POST /ingest/*` endpoints. |
| Identity resolution | 3-strategy match (deterministic SSN-hash → probabilistic name+DOB → new record) with persistent xrefs in `applicant_identity_xref`. |
| Income assembly | Per-borrower assembler covering W2 / self-employed / rental / SSA / asset depletion / military, with versioned `income_profiles` (`superseded_by` chain). |
| Credit assembly | Synthetic tri-merge profile generator deterministic by `credit_band`. |
| Document generation | reportlab-rendered W2, paystub, bank statement, credit report; Pillow-rendered driver's license JPG; `package_generator` assembles full doc sets. |
| Document extraction | **23 doc-type extractors** wired into the indexer dispatch — 8 original (W2 / paystub / bank / credit / appraisal / HOI / flood / tax) + 15 new income / asset / property / loan-terms / employment extractors covering IRS / 1040 / Schedule C/E / 1099 / K-1 / retirement / brokerage / gift / AVM / 1004MC / purchase / URLA / rate-lock / offer-letter. All share the same contract: `(pdf_bytes) → (fields, confidence)` returning `({}, 0.5)` on any failure, `base_conf × fraction_populated` on success. |
| AI extraction fallback | Claude Vision (`claude-sonnet-4-6`) fires when a deterministic extractor returns empty (or no extractor exists for a doc type at all). Renders the first N pages as PNG, sends to Claude with a doc-type-specific field-hint prompt, parses JSON. Always-graceful — `({}, 0.5)` on missing key / disabled flag / network error. Gated on `ENABLE_AI_EXTRACTION=true` (default) and `AI_EXTRACTION_MAX_PAGES=3`. Cost-aware logging on every successful call. **Field hints cover 56 doc types** (`_EXPECTED_FIELDS` in `core/documents/extractors/claude_extractor.py`): W-2 + paystub variants, full tri-merge credit reports, bank statements M1/M2/M3 (M1 carries the richer hint set with `nsf_count` / `largest_deposit` / period bounds), URAR + 1073 + update appraisals, title commitment + insurance, HOI + HO6 + flood, VOE / DL / SSN / OFAC / AUS, fixed-income (SSA + pension), condo / HOA / inspection addenda (HOA cert, condo questionnaire, survey, WDO, well-septic, wind-hail), plus lease + divorce decree. Doc types without an entry still extract (Claude falls back to "Extract all visible fields") but accuracy is materially lower without the targeted hint. |
| Confidence ranking | `SOURCE_CONFIDENCE_RANKING` (IRS=0.99 … VERBAL=0.50). `ConfidenceResolver` picks the highest-confidence value across sources and flags >10% numeric divergence as a conflict. Per-pair overrides in `FIELD_CONFLICT_THRESHOLDS` (IRS↔W2 5%, IRS↔1040 agi 2%, URLA stated income 10%, AVM↔appraisal 15%, 1004MC 20%, RATE_LOCK↔URLA 5%). |
| Cross-doc graph | `COMPARISON_MAP` carries 43+ pairs covering IRS↔W2, W2↔paystub, Schedule C/E↔1040, 1099↔1040, K-1↔1040, OFFER↔W2/paystub/VOE, URLA↔W2 (stated-vs-documented via `monthly_income_stated_annual` annualization), URLA↔purchase, RATE_LOCK↔URLA, gift↔bank, AVM↔appraisal, purchase↔appraisal, 1004MC↔appraisal, plus the original employment / property / asset cross-checks. Reconciler emits typed edges (confirms / corroborates / contradicts) into `document_relationships`. **Cross-applicant comparisons gated by an explicit `_CROSS_APPLICANT_PAIRS` allow-list** (currently W2↔W2, paystub↔paystub on `tax_year`) so per-borrower fields (W2 wages, IRS wages, URLA stated income) never produce false-positive contradicts edges between primary and co-borrower data. |
| Extraction provenance | Every `document_index` row carries `extraction_method ∈ {deterministic, caller_supplied, ai_vision, none}`. Priority on upsert: `deterministic > caller_supplied > ai_vision > none` (a doc upserted by the indexer with `deterministic` correctly upgrades from `caller_supplied`; AI Vision doesn't downgrade). Surfaced on `/applicant/{id}/field/{name}` and as `extraction_breakdown: {bucket: count}` on `/applicant/{id}/graph/summary`. |
| LTV / PITI / DTI | ContextAssembler computes LTV (`loan_amount / min(appraised, purchase_price) × 100`), PITI inline (amortization + tax + HOI + HOA when PropertyAssembler's `piti_total` is null), and front/back DTI from PITI + obligations + income. Falls back through `loan_terms` (URLA / RATE_LOCK) → `app` row for the effective loan_amount / interest_rate / term — so the math fires whether the loan terms came in via the `/loans` payload, a URLA upload, or a rate-lock. |
| Chaos-tolerance | API boundary uses `Optional[Any]` on every numeric/bool field in `DocumentSchema` — unparseable values like `box1_wages="one hundred ten thousand"` land in `document_index` instead of being 422'd. Income assemblers use a `_f()` helper that never raises (handles None, bool, currency strings, AND unparseable strings → returns `0.0`); the bad field is silently skipped, the doc stays tracked / counted / graph-visible. Verified by `scripts/feed_chaos_loans.py` (5 scenarios: self-employed / co-borrower / property disaster / data-quality / stale-expired) reporting **VERDICT: ROBUST — 69/69 uploads succeeded, 0 failed, 0 crashes** across deterministic re-runs. |
| Caching | Redis (TTL-keyed, **fully async via `redis.asyncio`**) for status / income / credit / app-lookup / context / property / graph plus Tier-1 entity caches `asset:{aid}` (4h) and `identity:{aid}` (24h). |
| Persistence | Aurora-Postgres (asyncpg pool). FK-safe write order, JSONB columns, idempotent upserts. |
| Concurrency safety | Per-applicant assembly lock (`assembly_lock:{applicant_id}`, 30s TTL) serializes `_run_assembly` for the same applicant; bailed contenders persist their docs first so the holder's inner-merge picks them up. `BatchIndexer` processes distinct applicants in parallel under `Semaphore(10)`. Joint-application doc-merge fans out the primary + co-borrower PG fetches via `asyncio.gather`. |
| Application context | One-call `GET /application/{id}/context` returns nested `borrower` (income / credit / assets / identity / document_count / qualifying_monthly), `co_borrower_aggregation`, `loan_terms` (URLA / RATE_LOCK / PURCHASE_AGREEMENT merged), `conflicts: {count, critical: [...]}` (top contradicts edges), legacy `primary` / `co_borrower` snapshots (kept for backwards compat), property, vendor_checks, DTI/LTV, readiness flags, missing_items. Cached at `context:{application_id}` (TTL 30m). |
| Readiness flags | 19 flags covering: borrower (income_verified, credit_pulled, identity_verified, employment_verified, assets_verified, identity_complete, tax_docs_received), property (appraisal_complete, title_clear, title_received, insurance_bound, flood_cert_received), application (dti_calculable, ltv_calculable, aus_ready), loan terms (loan_application_complete, purchase_agreement_received, rate_locked — date-aware), and a cross-doc fraud signal (no_critical_conflicts). |
| Missing-documents catalog | `GET /application/{id}/missing-documents` returns 15 required slots (with `alternates` for W2_CURRENT∥W2_PRIOR / AUS_DU∥LP / HOI_BINDER∥HOI_DECLARATIONS) + 9 conditional slots (IRS transcript, 1040, Schedule C/E, gift letter, wind/hail, WDO, well/septic, HOA — each with the `reason` clause that triggers it) + `received` + `total_expected` / `total_received` / `completeness_pct`. |
| API | FastAPI app, structured-log middleware, all `/ingest/*` and `/loans*` endpoints. Three Decision-OS-facing API interfaces sit on top of the same data layer: **Application API** (real-time per-entity context — `/loans`, `/documents/upload`, `/application/{id}/context`, `/applicant/{id}/income-profile`, etc.), **Report API** (`/reports/{pipeline,conflicts,completeness,extraction-quality,income-verification}` — paginated cross-loan analytics with 5-min Redis cache), **Bulk Export API** (`/export/{entities,documents,graph,profiles,applications}` — streaming JSONL/CSV with optional `?since=` incremental cutoff and per-consumer watermark CRUD). |
| Auth + multi-tenancy | DB-backed `verify_api_key` resolves the inbound `X-API-Key` against the `api_keys` table (5-min Redis cache at `apikey:{key}`), with a legacy env-var fallback for tests. Every domain row tags `tenant_id` + every read filters on it; Redis keys are prefixed `{tenant_id}:`; reports cache key + every export stream filter on tenant. `Admin` API: `POST /admin/tenants` / `POST /admin/api-keys` (generates `edms_<32-char-token>`) / `GET` listings (api_keys masked) / `DELETE` deactivation, all gated by `Depends(require_admin)`. |
| Rate limiting | One ASGI middleware (`core/middleware/rate_limiter.py`) gates every authenticated request. Three tiers — **application** 1000/min, **reports** 100/min, **export** 10/hour — keyed by raw `X-API-Key` value (per-key, not per-tenant). `X-RateLimit-Limit/Remaining/Reset` on every gated response; `429 + Retry-After` on bust. Bypass list covers `/health`, `/ready`, `/docs`, `/redoc`, `/openapi.json`, `/dashboard`, `/admin/*`. Fail-open on Redis errors. |
| Async webhook outbox | `WebhookPublisher.publish()` writes one `webhook_outbox` row per subscriber (one INSERT each) and returns immediately — uploads no longer block on subscriber availability. A background asyncio task (`core/webhooks/delivery_worker.py`) drains pending rows under `Semaphore(10)`, POSTs with `httpx.AsyncClient(timeout=10)` + HMAC-SHA256, marks `delivered` on 2xx, applies `2^attempts × 30s` backoff (cap 1h) on failure, flips to `status='failed'` after `max_attempts`. `POST /webhooks/{id}/retry-failed` resets failed rows; `/health` reports `{pending, failed, delivered_last_hour, oldest_pending_age_seconds}`. |
| Schema auto-migration | Lifespan applies `infra/schema.sql` against the connected pool right after `db.get_pool()`. Idempotent (every CREATE/ALTER is `IF NOT EXISTS`, every seed `ON CONFLICT DO NOTHING`); `already exists` is `skipped`, anything else is `errors` with the first failing statement logged but startup continues. Off-switch via `AUTO_MIGRATE_ON_STARTUP=false`. Each ECS deploy auto-applies new DDL — no separate `apply_schema.py` task for routine rollouts. |
| Incremental graph backtest | New harness for testing 50-day arrival patterns end-to-end. `scripts/generate_s3_simulation.py` writes 90 docs across 5 loans with realistic patterns. `core/connectors/s3_connector.py` walks date folders incrementally with `(watermark, until]` window. `core/graph/incremental_builder.py` ticks N times/day: pull → save → reconcile → re-assemble → upsert `entity_states` → record `graph_build_runs`. `core/graph/snapshot_scheduler.py` copies live state into `entity_snapshots` (UNIQUE per `(snapshot_date, entity_id)`) for lineage replay. `scripts/run_backtest.py` drives the full 50 days inproc OR via HTTP — `--api-url` + `--api-key` lets ops run against the AWS deployment with no local PG/Redis access. |
| Scale simulation (v4) | `scripts/generate_scale_simulation.py` writes ~338k objects across **22 origination profiles** (Conv/FHA/VA/USDA/Jumbo/NonQM/HELOC/Construction/Reverse/Bridge/Renovation/etc.) × **9000 loans** × **60-day timeline** with ±2-day per-loan jitter into `s3://edms-simulator-loans/s3_scale_simulation/{date}/{loan_origination|post_application/<channel>}/...`. 100 of 9000 loans get rendered binary `.pdf` evidence (the rest stay JSON-only to keep generation tractable). `scripts/verify_scale_results.py` runs **15 production-readiness checks** against any deployed API via `--api-url` + `--api-key`: row counts, watermark progression, edges, snapshots, completeness distribution, `extraction_method` mix, contradicts edges, etc. |
| Bounded connector pull | `core/connectors/s3_connector.py:_pull_sync` is bounded to **one date folder per build** — sorts every date folder, picks only the first strictly after `wm.date()`, returns `[]` when nothing's left. Without the bound, a fresh-from-epoch call against the 9k-loan corpus would list ~338k objects in one shot and ALB-timeout the build before any docs land. The catch-up loop drives this forward day-by-day; existing `documents_pulled == 0 && applications_created == 0` termination handles end-of-corpus. |
| Async scheduler endpoints | `POST /scheduler/trigger` and `POST /scheduler/catch-up` are both fire-and-forget — they validate the job name (still 404 on typos), spawn an asyncio background task, and return immediately with `{job_id, status:'started'}`. State lives at Redis key `catchup:state` so any replica's `GET /scheduler/catch-up/status` reports the same view (in-process dict broke under multi-replica — POST hits A, GET on B used to report `idle`). The status response includes `builds_completed`, `documents_pulled`, `documents_processed`, `applications_created`, `current_watermark`, `last_heartbeat`, `heartbeat_age_s`, `error_count`, `last_error`, `elapsed_seconds`, `max_builds`, `stopped_reason`. |
| Zombie-hardened catch-up | The bg task can crash silently and leave Redis at `status='running'` forever, so three layers guard the operator: **(1)** `last_heartbeat` written before AND after every build; the POST handler treats `running` with `heartbeat>60s` as a zombie and starts a new drain (operator escape hatch via `?force=true` always overrides). **(2)** Per-build try/except — one bad date folder no longer kills the whole drain; `error_count` + `last_error` accumulate and we stop with `stopped_reason='too_many_errors'` after 3 consecutive failures. **(3)** `job_id` self-check inside the bg loop so a force-race loser exits silently rather than stomping the new run's counters. `POST` accepts `max_builds` via JSON body or query param (body wins). `GET` surfaces `heartbeat_age_s` so operators don't subtract timestamps. |
| 15-minute cron | `config/schedule.yaml` collapsed three named cron times into a single `*/15 * * * *` so backlogs drain continuously instead of waiting for the next named slot. Day-boundary EOD snapshots fire inline during the catch-up drain when the watermark crosses a calendar day. |
| `/admin/reset` | `POST /admin/reset` (admin-scoped) nukes prod data for re-runs of the scale corpus. The route lives under `api/admin.py` mounted with `prefix=/admin`, so the decorator path is `/reset` not `/admin/reset` (the double prefix bug is documented). |
| Multi-channel v2 simulation | `scripts/generate_realworld_simulation.py` produces 50 days × 10 diverse loan scenarios (clean salaried, self-employed, joint dual-income, retired fixed-income, first-time-gift, investment rental, refinance with no PA, H1B visa holder, post-divorce alimony, condo-HOA-heavy) × 9 source channels with realistic per-loan arrival schedules + intraday bursts + missing-doc edge cases (LOAN-107 refi has no PA; LOAN-102 has two 1099s in one folder; LOAN-103 has 4 identity docs clustered). `--clean` and `--upload` flags drive `aws s3 sync`. |
| Multi-format PDF rendering | `scripts/pdf_formats.py` defines per-(doc_type, format) renderers so the same field set lands in distinct visual layouts: **W-2** ×3 (ADP red header + 2-col boxes / Paychex Times-Roman horizontal bands / Gusto Helvetica modern centred), **paystub** ×3 (ADP / Paychex / Workday), **bank statement** ×3 multi-page (Chase blue + summary p1 / Wells Fargo red + summary p1 / **BOA navy + ending_balance buried on p3** to stress AI-Vision's `AI_EXTRACTION_MAX_PAGES=3` window), **title** ×2 multi-page (First American formal Schedule A/B per page / Chicago combined schedules), **credit** ×2 multi-page (Equifax tri-merge with score row / Experian summary boxes), **appraisal** ×2 multi-page (URAR boxes / narrative prose). Format pinned per loan via `W2_FORMAT_BY_LOAN` / `PAYSTUB_FORMAT_BY_LOAN` (LOAN-101 ADP, LOAN-103 Gusto + ADP co-borrower, etc.) and per bank-name for statements. Evidence is written as **raw binary `.pdf`** (openable directly when downloaded from S3), with sibling `.pdf` files dropped alongside JSON records in `edms_pull/` and `los_encompass/` so format-renderable doc types still get a rendered PDF (the connector keys on `.json`, ignores the binary). 4 shared-drive scan variants exercise scanner artifacts: 1.5° rotation, landscape orientation, two physical docs on one scan, faded photocopy. |
| Extraction-accuracy harness | `scripts/verify_pdf_extraction.py` deterministically picks one PDF per (doc_type, format) tuple, runs `extract_with_claude_sync`, and reports per-field accuracy against the meta.json ground truth (numeric within 1% / case-insensitive string match). Gated on `ANTHROPIC_API_KEY` so CI / unit tests skip silently. `--max-pdfs N` caps the bill; default 8 covers all family variants for ~$0.25/run. |
| Connector funnel observability | Every S3 list call in `core/connectors/s3_connector.py` emits a CloudWatch-visible INFO line — `s3_list_date_folders_call` / `_complete`, `s3_iter_channels`, `s3_iter_channels_legacy_fallback` (smoking-gun signal that v1 fallback fired), `s3_iter_files`. Per-folder + final logs distinguish `by_channel_seen` (raw yields) from `by_channel_accepted` (post watermark + `received_at` filter), so an operator can attribute funnel collapse to S3 listing, JSON parsing, watermark filtering, or downstream rejection without re-running. All values inlined into the message string (not `extra={}`) so the production stdlib formatter surfaces them. Diagnosed the prod KMS bug in one trigger. |
| Simulator-bucket KMS-safe uploads | `scripts/generate_realworld_simulation.py`'s `s3_sync` always passes `--sse AES256` because the `edms-simulator-loans` bucket's default-encryption KMS key isn't readable by the ECS task role; without the override, `GetObject` returns `AccessDenied` at read time and the connector pulls 0 docs. Same convention used by `scripts/generate_s3_simulation.py`'s `upload_to_s3`. Prod re-encryption recipe: `aws s3 cp s3://bucket/prefix/ s3://bucket/prefix/ --recursive --sse AES256 --metadata-directive REPLACE`. |
| Channel-segmented connector | `core/connectors/s3_connector.py` dispatches per source-channel sub-folder under each date. **v3 layout** (current default): two-stage tree — `{date}/loan_origination/` for the application event, `{date}/post_application/{channel}/` for everything else; `loan_origination/` is yielded FIRST so the builder can create apps before docs need los_id resolution. ~50 known channels across individual JSON (LOS/employer/vendor/IRS/SSA/VA/servicer/compliance/LO-notes), batched JSON arrays (`los_encompass`), CSV row-fanout (`los_bytepro` via DictReader), MISMO 3.4 XML (`mismo_feed` via ET local-name match), `_meta.json` + raw `.pdf` pairs (AMCs, title companies, insurance carriers, closing agent, HOA, condo project, conditions, corrections, attorney_legal, borrower_portal, email_inbox), and raw `.pdf` scans synthesised as `document_type=UNKNOWN, requires_classification=True` for AI Vision (manual-drop variants + `shared_drive`). Application events tagged `event_type='loan_application_submitted'` so the builder routes them through `pg.create_application_from_event` (idempotent on `los_id`) before any document needs an applicant_id. **v2 layout** (single-level channel sub-folders) and **v1 legacy** (recursive `.json` scan) paths preserved. Funnel-stat log inlines per-channel counts. PDF evidence is stored as raw binary so it can be downloaded from S3 and opened directly. |
| `legacy_ids` accumulator | `entity_states.legacy_ids` + `entity_snapshots.legacy_ids` are JSONB columns merged via `\|\|` on every `upsert_entity_state` so the column accumulates IDs over time without overwriting (encompass loan number from the application event + every `source_document_id` from each tick's docs that touched the applicant). Schema additions are strictly additive (`ALTER TABLE … ADD COLUMN IF NOT EXISTS`); a one-time destructive reset lives in `scripts/reset_v3_data.sql` (TRUNCATE recipe with FK ordering) for ops to run when intentionally clearing state. `document_index` gains `source_document_id VARCHAR(200)` + `source_channel VARCHAR(100)` so each row traces back to the originating system's native ID format. |
| `los_id → applicant_id` resolution | `IncrementalGraphBuilder.run_build` resolves `los_id → applicant_id` via `pg.get_application_by_los_id` (per-tick cache) when incoming docs lack an applicant_id — the v2 generators only know the LOS the system minted at `/loans` time. Co-borrower role correctly maps to `co_applicant_id`. Unresolved los_ids (including the synthesised `UNCLASSIFIED` from raw scans) log `unknown_los_id` and skip the persist step, keeping the document_index FK clean. |
| Entity state write-through | Every `/documents/upload` ends with one `entity_states` row per affected entity in the lending tree (borrower / co-borrower / property / loan_terms). State JSONB carries the rich sub-entity views: borrowers `{income, employment, credit, assets, identity, doc_types}` (15-slot completeness), property `{valuation, title, insurance, tax, inspections, doc_types}` (5-bucket completeness), loan_terms `{urla, purchase_agreement, rate_lock, aus_findings, doc_types}` (4-bucket completeness). Redis write-through under `entity:{entity_id}` (1h TTL). 4 new endpoints: `/entity/{id}/state`, `/entity/{id}/timeline`, `/graph/build-runs`, `/graph/watermark`. Wrapped in a top-level try/except so per-entity failures never block uploads. |
| OpenAPI / Swagger | Title `EDMS Knowledge Graph API` v1.0.0 with multi-paragraph description, contact + license, four ordered tag groups (Application / Reports / Export / System / Admin) each with prose blurbs. `custom_openapi()` post-processor classifies every operation by URL prefix and stamps the `ApiKeyAuth` security scheme on every non-public path. `summary` + `responses={200/401/404/422/429}` + per-Query `description=` on every public endpoint. Multi-content-type 200 examples on streaming endpoints. |
| Resilience | Anthropic upstream errors map to HTTP 502 with detail; email body fallback preserves attachment processing. |
| Walkthrough | `scripts/simulate_local.py` runs all 7 ingestion-+aggregation steps end-to-end. `scripts/run_backtest.py` runs a 50-day incremental graph build with EOD snapshots. |

### Pending (⏳)

| Area | What's left |
|---|---|
| `_handle_normalized_ingest_event` for non-API channels | Today, only API events drive the full aggregation pipeline. Chat/PDF/email events are produced but not merged into the profile via `ConfidenceResolver`. (Spec called this BUILD 12.) |
| ~~`claude_extractor.extract` body~~ | **Shipped Tier-3 (commit `1bde27a`).** `extract_with_claude` (async) + `extract_with_claude_sync` use Claude Vision as the indexer / pdf_adapter fallback. Always-graceful, gated on `ENABLE_AI_EXTRACTION` + `ANTHROPIC_API_KEY`. |
| XRefStore startup hydration | After uvicorn restart, in-memory state is empty while Postgres persists. Causes `idx_applicant_ssn` UniqueViolation on re-run with existing SSN. Fix: hydrate from Postgres at startup, or have resolver fall back to a Postgres lookup. |
| `/ingest/csv` ingestion | Endpoint returns parsed signals + report; doesn't push events through the aggregation pipeline. |
| Live Claude extractor for image | Today image_adapter calls Claude vision directly; could route through `claude_extractor` for unified retry/cache. |

### Non-goals

- ~~Production-grade rate limiting / authn / authz beyond the dev `X-API-Key`.~~ **Shipped:** DB-backed multi-tenancy + per-API-key rate limiting across three tiers + admin scope CRUD.
- A UI. The simulator is API + scripts only.
- Real bureau-pull integrations (Experian / Equifax / TransUnion APIs). The
  `CreditAssembler` synthesizes a plausible profile from the requested band.
- A queue / async worker substitute. Local runs are synchronous; the AWS
  Lambda + SQS code lives in `core/lambda_handlers/` and `core/pipelines/`
  for production wiring but isn't exercised locally.

## 4. Functional requirements

| FR | Requirement |
|----|-------------|
| FR-1 | Each channel adapter MUST emit a `NormalizedIngestEvent` matching the schema in `core/ingestion/events.py`. |
| FR-2 | `IngestRouter.detect_channel` MUST classify a payload from content alone — no caller-supplied content-type. |
| FR-3 | Identity resolution MUST persist the golden record to Postgres before any application row that references it (FK ordering). |
| FR-4 | `applicant_identity_xref` MUST allow primary + co-borrower to share a `(source_system, source_id)` pair. |
| FR-5 | The pymupdf extractor MUST recover ≥85% of expected fields for each document type produced by the matching generator (W2, paystub, bank statement, credit report). |
| FR-6 | All chat-extracted values MUST be flagged `requires_verification=True`. |
| FR-7 | When a Claude API call fails with an account-level error, the email pipeline MUST still produce attachment events. |
| FR-8 | Anthropic upstream errors MUST surface to API callers as HTTP 502 with the upstream `detail`, not as opaque 500s. |
| FR-9 | Tests dependent on Claude MUST be skipped (not failed) when `ANTHROPIC_API_KEY` is absent. |
| FR-10 | `simulate_local.py` MUST run to completion (exit 0) without `ANTHROPIC_API_KEY`, exercising the deterministic 5/7 steps. |

## 5. Non-functional requirements

| NFR | Requirement |
|-----|-------------|
| Performance | CSV adapter MUST process 100 rows in < 2 seconds. Local API median latency for `/loans` < 200ms on a clean DB. |
| Reproducibility | Document generators MUST be deterministic given the same seed (content equality; PDF bytes differ because reportlab stamps a CreationDate). |
| Observability | All API requests log a single JSON line via `RequestMiddleware` including `request_id`, `method`, `path`, `status_code`, `elapsed_ms`. |
| Test coverage | New adapters require both a deterministic test (mock or content fixture) and, where applicable, a key-gated live test. |
| Safety | Local docker compose ports differ from defaults (5433 / 6380) so the dev stack can't clash with system Postgres / Redis. |

## 6. Acceptance criteria

The simulator is considered "running correctly" when:

1. `docker compose up -d postgres redis` brings both services healthy.
2. `python -m pytest tests/ --ignore=tests/integration -q` reports
   `356 passed, 2 skipped` (live tests skipped without API key) on a
   fresh checkout. Integration + smoke add 11 more for `367 green`.
   `python scripts/stress_test_indexing.py` reports `23 passed, 0 failed`
   across 7 concurrency / cache / throughput / webhook tests.
3. `python scripts/simulate_local.py` exits 0 and produces:
   - 5 documents in `local_storage/demo/` (≈ 51 KB total)
   - HTTP 200 from `/ingest/pdf` for the W2 with `confidence=1.0`
   - HTTP 200 from `/ingest/email` with both body and attachment events
   - HTTP 200 from `/loans` with a fresh `applicant_id`
   - 2 rows in `applications` and 4 in `applicant_identity_xref` after
     STEP 6 (deterministic match)
4. With `ANTHROPIC_API_KEY` set + sufficient credit, STEP 1 chat extraction
   returns structured borrower / co-borrower / income-source data.
5. With `ANTHROPIC_API_KEY` set but no credit, the simulator surfaces the
   upstream error message clearly and continues; STEP 3 attachment still
   processes at confidence 1.0.

## 7. Dependencies & constraints

- **Runtime**: Python 3.12, Docker Desktop. Tested on Windows 11.
- **Locked Python deps** in `requirements.txt` (runtime) and
  `requirements-dev.txt` (test). Notable additions during build-out:
  - `python-multipart` (for `UploadFile` / `Form`)
  - `reportlab`, `PyMuPDF`, `Pillow` (Phase B)
  - `anthropic>=0.99.0` (Phase C)
- **External services**: optional Anthropic API for chat / image / email body
  / Tier-3 AI extraction fallback. Cost ≈ a few cents per simulate_local run
  when live; the AI extraction fallback fires only when a deterministic
  extractor returns empty, and is gated on `ENABLE_AI_EXTRACTION=true`
  (default) + `AI_EXTRACTION_MAX_PAGES=3`. Flip the flag off for zero token
  cost.
- **AWS**: production code targets Aurora + ElastiCache + S3 + ECS + SQS.
  Local stack uses Postgres 15 + Redis 7 in containers and `local_storage/`
  for documents.

## 8. Risks

| Risk | Mitigation |
|------|-----------|
| Anthropic API key leaks via logs | Adapters never log the key; `_claude_client` reads env each call (no print). `.env` is gitignored. |
| Schema drift between `infra/schema.sql` and a running DB | The lifespan now auto-applies `infra/schema.sql` on every API startup (`core/storage/migrations.py`); idempotent CREATE/ALTER `IF NOT EXISTS` so re-runs are no-ops. Each ECS deploy picks up new DDL automatically. |
| Confidence ranking diverges from real underwriting truth | The numbers in `SOURCE_CONFIDENCE_RANKING` are spec-driven; underwriting can override per-deployment by editing the dict — it's the only source. |
| `XRefStore` in-memory state diverges from Postgres | Documented in `context.md`. Workaround: truncate before each fresh demo. Real fix: hydrate from Postgres at startup. |
| Bucket-default KMS encryption blocks the connector | `edms-simulator-loans` default-encrypts new objects with a KMS key the ECS task role can't `kms:Decrypt`, so any upload missing `--sse AES256` is invisible to `GetObject` and the connector reads 0 docs. Mitigation: every uploader script in this repo (`generate_s3_simulation.py`, `generate_realworld_simulation.py`) hard-codes the AES256 override; remediate stale objects with an in-place server-side copy + `--metadata-directive REPLACE`. |

## 9. References

- Local architecture: `docs/ARCHITECTURE.md`
- API surface: `docs/API.md`
- Session-resume notes & gotchas: `context.md`
- AWS deployment: `infra/cfn-template.yaml` + `.github/workflows/aws.yaml`
