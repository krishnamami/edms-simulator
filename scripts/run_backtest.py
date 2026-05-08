"""Backtest engine: simulate N days of incremental graph building.

Walks ``local_storage/s3_simulation`` (or an S3 bucket) day-by-day,
runs ``BUILDS_PER_DAY`` incremental builds per simulated day, takes
EOD snapshots, and prints a report card.

Two execution modes:

**Inproc** (default) — talks to PG + Redis directly through the
service layer. Used for local development; doesn't require the API
to be running.

**API** — set ``--api-url`` (and ``--api-key``) to drive the backtest
via the production HTTP surface instead. Bootstraps applications via
``POST /loans``, posts each window's docs via ``POST /documents/upload``,
then reads state back via ``GET /entity/{id}/state``,
``GET /graph/build-runs``, ``GET /graph/watermark`` (best-effort —
older deployments without the entity-state surface still get a useful
report card based on upload counts).

Usage:
    # Inproc, full 50-day, 2 builds / day
    python scripts/run_backtest.py

    # Single-day re-run
    python scripts/run_backtest.py --day 2026-01-15

    # Against a remote EDMS deployment
    python scripts/run_backtest.py \\
        --api-url http://edms-simulator-alb-1374683374.us-east-1.elb.amazonaws.com \\
        --api-key edms-prod-key-2026

The HTTP endpoints in ``api/routes.py`` read the same tables and
surface inproc results back to operators / dashboards.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Force test/local fakes off — backtest uses real PG + Redis.
os.environ.setdefault("USE_AWS_SECRETS", "false")
os.environ.setdefault("USE_AWS_SQS", "false")
os.environ.setdefault("USE_LOCAL_STORAGE", "true")
os.environ.setdefault("USE_FAKE_REDIS", "false")

from core.connectors.s3_connector import S3EDMSConnector  # noqa: E402
from core.credit.assembler import CreditAssembler         # noqa: E402
from core.graph.incremental_builder import IncrementalGraphBuilder  # noqa: E402
from core.graph.reconciler import DocumentReconciler       # noqa: E402
from core.graph.snapshot_scheduler import SnapshotScheduler  # noqa: E402
from core.identity.xref_store import XRefStore             # noqa: E402
from core.income.assembler import IncomeAssembler          # noqa: E402
from core.storage import db                                # noqa: E402
from core.storage.postgres_store import PostgresStore      # noqa: E402
from core.storage.redis_store import RedisStore            # noqa: E402
from scripts.generate_s3_simulation import LOAN_PROFILES   # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(message)s")
logger = logging.getLogger("backtest")


DEFAULT_START   = "2026-01-01"
DEFAULT_DAYS    = 50
DEFAULT_BUILDS  = 2
DEFAULT_S3_PATH = "local_storage/s3_simulation"
TENANT_ID       = os.getenv("BACKTEST_TENANT_ID", "default")


# Build clocks per day, hour-of-day. 2-build cadence: noon + 17:00.
# 3-build cadence: 09:00, 13:00, 17:00. N-build: spread across 09→17.
def _build_clocks(builds_per_day: int) -> list[int]:
    if builds_per_day <= 1:
        return [17]
    if builds_per_day == 2:
        return [12, 17]
    if builds_per_day == 3:
        return [9, 13, 17]
    # General: even spread across the 9-17 work day
    span = 8
    step = max(1, span // (builds_per_day - 1))
    return [9 + step * i for i in range(builds_per_day)]


# ---------------------------------------------------------------------------
# Bootstrap — create applicant + application rows for each loan in the
# `applicants` / `applications` tables. Document FK constraints need
# parents to exist before save_document fires.
# ---------------------------------------------------------------------------


async def bootstrap_loans(pg: PostgresStore) -> dict:
    """Idempotently create one applicant per loan + per co-borrower
    + one application row per loan. Uses the placeholder applicant_ids
    the connector docs reference (``APL-LOS-XXX-P`` / ``-C``).

    Returns ``{los_id: application_id}`` so the runner can log mapped IDs.
    """
    out: dict = {}
    for los_id, prof in LOAN_PROFILES.items():
        primary_id     = f"APL-{los_id}-P"
        co_id          = f"APL-{los_id}-C" if prof.get("co_name") else None
        application_id = f"APP-{los_id}"

        await pg.save_golden_record({
            "applicant_id":   primary_id,
            "full_name":      prof["primary_name"],
            "first_name":     prof["primary_name"].split()[0],
            "last_name":      prof["primary_name"].split()[-1],
            "dob":            prof["primary_dob"],
            "ssn_hash":       f"hash_{los_id}_P",
            "ssn_last4":      prof["primary_ssn4"],
            "status":         "active",
            "identity_xrefs": [],
            "application_ids": [application_id],
        }, tenant_id=TENANT_ID)

        if co_id:
            await pg.save_golden_record({
                "applicant_id":   co_id,
                "full_name":      prof["co_name"],
                "first_name":     prof["co_name"].split()[0],
                "last_name":      prof["co_name"].split()[-1],
                "dob":            prof["co_dob"],
                "ssn_hash":       f"hash_{los_id}_C",
                "ssn_last4":      prof["co_ssn4"],
                "status":         "active",
                "identity_xrefs": [],
                "application_ids": [application_id],
            }, tenant_id=TENANT_ID)

        await pg.save_application({
            "application_id":  application_id,
            "applicant_id":    primary_id,
            "co_applicant_id": co_id,
            "los_id":          los_id,
            "status":          "active",
        }, tenant_id=TENANT_ID)
        out[los_id] = application_id
    return out


# ---------------------------------------------------------------------------
# Main backtest loop
# ---------------------------------------------------------------------------


async def reset_watermark(pg: PostgresStore) -> None:
    """Roll the connector watermark back to the epoch so every run is
    deterministic."""
    await pg.set_watermark_timestamp(
        "s3_edms_connector", "2025-12-31T00:00:00+00:00",
    )


async def reset_run_artifacts(pg: PostgresStore) -> None:
    """Wipe entity_states / entity_snapshots / graph_build_runs for a
    clean re-run. Keeps applicants / applications / document_index
    intact — those persist between runs (idempotent saves)."""
    await db.execute("DELETE FROM entity_snapshots")
    await db.execute("DELETE FROM entity_states")
    await db.execute("DELETE FROM graph_build_runs")
    await db.execute("DELETE FROM document_relationships WHERE created_by = 'reconciler'")


async def run_one_day(
    builder: IncrementalGraphBuilder,
    snapshotter: SnapshotScheduler,
    day: date,
    clocks: list[int],
    tenant_id: str,
) -> dict:
    """Run all clocks for a single day, then take EOD snapshot.
    Returns a per-day summary dict."""
    per_build: list[dict] = []
    for build_number, hour in enumerate(clocks, 1):
        until = datetime.combine(
            day, datetime.min.time().replace(hour=hour, minute=59, second=59),
            tzinfo=timezone.utc,
        ).isoformat()
        stats = await builder.run_build(
            build_date=day,
            build_number=build_number,
            until=until,
            tenant_id=tenant_id,
        )
        per_build.append(stats)
    snapshot_count = await snapshotter.take_daily_snapshot(day, tenant_id=tenant_id)
    return {
        "day":            str(day),
        "builds":         per_build,
        "snapshot_count": snapshot_count,
    }


def _format_day_line(day: date, builds: list[dict], snap_count: int) -> str:
    parts = []
    for i, b in enumerate(builds, 1):
        parts.append(
            f"Build {i}: +{b['documents_new']:>2} docs, "
            f"{b['entities_updated']:>2} ents"
        )
    return f"Day {day} | " + " | ".join(parts) + f" | snap: {snap_count} entities"


async def final_report(pg: PostgresStore, start: date, end: date) -> None:
    runs = await pg.get_graph_build_runs(start, end, tenant_id=TENANT_ID, limit=1000)

    total_builds   = len(runs)
    total_pulled   = sum(r.get("documents_pulled") or 0 for r in runs)
    total_new      = sum(r.get("documents_new")    or 0 for r in runs)
    total_edges    = sum(r.get("edges_created")    or 0 for r in runs)
    # Count entity_states irrespective of last_updated — that column is
    # real wall-clock, not the simulated build date, and the backtest
    # always stamps NOW() on the upsert.
    total_entities = await db.fetchval(
        "SELECT COUNT(*) FROM entity_states WHERE tenant_id = $1",
        TENANT_ID,
    )
    snap_count = await db.fetchval(
        "SELECT COUNT(*) FROM entity_snapshots WHERE tenant_id = $1",
        TENANT_ID,
    )

    bar = "═" * 60
    print(f"\n{bar}")
    print(f"  BACKTEST COMPLETE — {(end - start).days + 1} days simulated")
    print(bar)
    print(f"  Total builds:        {total_builds}")
    print(f"  Total docs pulled:   {total_pulled}")
    print(f"  Total docs ingested: {total_new}")
    print(f"  Total entities:      {total_entities}")
    print(f"  Total edges:         {total_edges}")
    print(f"  Total snapshots:     {snap_count}")
    print()
    print("  Per-loan summary:")
    for los_id, prof in LOAN_PROFILES.items():
        primary_id = f"APL-{los_id}-P"
        st = await pg.get_entity_state(primary_id, tenant_id=TENANT_ID)
        if not st:
            print(f"    {los_id}: (no state)")
            continue
        state = st.get("state") or {}
        if isinstance(state, str):
            import json as _json
            try:
                state = _json.loads(state)
            except Exception:
                state = {}
        completeness = state.get("completeness_pct") or st.get("completeness_pct") or 0
        doc_count    = st.get("document_count") or 0
        edges        = st.get("graph_edge_count") or 0
        confl        = st.get("conflict_count") or 0
        last_doc     = state.get("last_doc_received_at") or "—"
        verdict = "complete" if completeness >= 100 else f"{completeness:.0f}% complete"
        print(f"    {los_id} ({prof['primary_name']:<20}): "
              f"{doc_count:>3} docs, {verdict:<14} "
              f"edges={edges:<3} conflicts={confl:<2} "
              f"last={last_doc[:10]}")
    print()
    print("  Watermark trail (first 10):")
    for r in runs[:10]:
        wm_from = r.get("watermark_from")
        wm_to   = r.get("watermark_to")
        print(f"    {r['build_date']} #{r['build_number']}: "
              f"+{r['documents_new']:>2} docs, "
              f"wm {str(wm_from)[:19] if wm_from else '—':<19} "
              f"→ {str(wm_to)[:19] if wm_to else '—'}")
    print(bar)


async def amain(args) -> int:
    pg     = PostgresStore()
    redis  = RedisStore()
    xref   = XRefStore()

    # The aggregation service is optional — the builder works without
    # it, just doesn't trigger income/credit/asset/identity assembly.
    # Wiring it in gives the report card richer data.
    try:
        from core.aggregation.service import AggregationService
        agg = AggregationService(
            xref_store=xref, golden_record_store=xref,
            income_assembler=IncomeAssembler(),
            credit_assembler=CreditAssembler(),
            redis_store=redis, postgres_store=pg,
        )
    except Exception as exc:
        logger.warning("aggregation_service_unavailable: %s", exc)
        agg = None

    connector   = S3EDMSConnector(args.s3_path, postgres_store=pg)
    reconciler  = DocumentReconciler(postgres_store=pg)
    builder     = IncrementalGraphBuilder(
        connector=connector,
        postgres_store=pg,
        redis_store=redis,
        reconciler=reconciler,
        aggregation_service=agg,
    )
    snapshotter = SnapshotScheduler(postgres_store=pg)

    # Bootstrap loans (idempotent).
    print("Bootstrapping 5 loans …")
    mapped = await bootstrap_loans(pg)
    for los_id, app_id in mapped.items():
        print(f"  {los_id} → {app_id}")

    if args.reset:
        print("Resetting backtest artifacts (entity_states / snapshots / build_runs / watermark) …")
        await reset_run_artifacts(pg)
        await reset_watermark(pg)

    # Determine the date range.
    if args.day:
        start = end = date.fromisoformat(args.day)
    else:
        start = date.fromisoformat(args.start)
        end   = start + timedelta(days=args.days - 1)

    clocks = _build_clocks(args.builds_per_day)
    print(f"Backtest: {start} → {end}, {args.builds_per_day} builds/day at hours {clocks} UTC")

    day = start
    while day <= end:
        summary = await run_one_day(
            builder, snapshotter, day, clocks, TENANT_ID,
        )
        print(_format_day_line(day, summary["builds"], summary["snapshot_count"]))
        day = day + timedelta(days=1)

    await final_report(pg, start, end)

    await db.close_pool()
    return 0


# ===========================================================================
# API mode — drive the backtest against a remote EDMS deployment
# ===========================================================================
#
# Goal: prove the same 50-day arrival pattern lands cleanly through the
# production HTTP surface. The runner reads simulation docs from the
# local filesystem (so it doesn't depend on the remote having an S3
# connector configured), bootstraps applications via POST /loans, then
# posts each window's docs via POST /documents/upload. After each day,
# it tries to read /entity/{id}/state — when the remote doesn't carry
# that endpoint yet (older deploys), the runner degrades gracefully:
# it tracks upload counts locally and prints a useful report card from
# what it observed itself.


import json as _json
from collections import defaultdict


def _walk_simulation_docs(s3_path: Path) -> list[dict]:
    """Read every ``.json`` doc from the local s3_simulation/ tree
    into memory. The 90-doc backtest set fits trivially; for larger
    sets, an iterator would be needed but the current scale is fine."""
    docs: list[dict] = []
    for date_dir in sorted(s3_path.iterdir()) if s3_path.exists() else []:
        if not date_dir.is_dir():
            continue
        for los_dir in sorted(date_dir.iterdir()):
            if not los_dir.is_dir():
                continue
            for f in sorted(los_dir.iterdir()):
                if f.suffix != ".json":
                    continue
                try:
                    with f.open("r", encoding="utf-8") as fh:
                        docs.append(_json.load(fh))
                except Exception as exc:
                    logger.warning("doc_read_failed: %s — %s", f, exc)
    return docs


def _docs_in_window(
    all_docs: list[dict],
    window_start: datetime,
    window_end: datetime,
) -> list[dict]:
    """Filter docs whose ``received_at`` falls in (start, end]."""
    out = []
    for d in all_docs:
        rs = d.get("received_at")
        if not rs:
            continue
        try:
            cleaned = rs[:-1] + "+00:00" if rs.endswith("Z") else rs
            ts = datetime.fromisoformat(cleaned)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if window_start < ts <= window_end:
            out.append(d)
    return out


class _APIClient:
    """Thin httpx wrapper. Closes the underlying client on context exit
    so callers don't leak connections during a 50-day run."""

    def __init__(self, base_url: str, api_key: str, timeout: float = 30.0):
        import httpx
        self.base_url = base_url.rstrip("/")
        self.api_key  = api_key
        self._client  = httpx.AsyncClient(
            base_url=self.base_url,
            headers={"X-API-Key": api_key, "Content-Type": "application/json"},
            timeout=timeout,
        )

    async def post(self, path: str, json: dict):
        return await self._client.post(path, json=json)

    async def get(self, path: str, params: Optional[dict] = None):
        return await self._client.get(path, params=params or {})

    async def aclose(self):
        await self._client.aclose()


async def _api_bootstrap_loans(client: _APIClient) -> dict:
    """Create one application per LOS via POST /loans. Returns
    ``{los_id: (real_applicant_id, real_co_applicant_id_or_None,
    real_application_id)}`` so the doc-upload step can rewrite the
    placeholder applicant_ids the simulation embeds."""
    mapping: dict = {}
    for los_id, prof in LOAN_PROFILES.items():
        body = {
            "los_id": los_id,
            "borrower": {
                "first_name": prof["primary_name"].split()[0],
                "last_name":  prof["primary_name"].split()[-1],
                "dob":        prof["primary_dob"],
                "ssn_hash":   f"hash_api_{los_id}_P",
                "ssn_last4":  prof["primary_ssn4"],
            },
            "loan": {
                "loan_amount":      round(prof["purchase_price"] * 0.8),
                "interest_rate":    6.50,
                "loan_term_months": 360,
            },
            "documents": [],
        }
        if prof.get("co_name"):
            body["co_borrower"] = {
                "first_name": prof["co_name"].split()[0],
                "last_name":  prof["co_name"].split()[-1],
                "dob":        prof["co_dob"],
                "ssn_hash":   f"hash_api_{los_id}_C",
                "ssn_last4":  prof["co_ssn4"],
            }
        resp = await client.post("/loans", body)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"POST /loans failed for {los_id}: "
                f"{resp.status_code} {resp.text[:300]}"
            )
        data = resp.json()
        mapping[los_id] = (
            data["applicant_id"],
            data.get("co_applicant_id"),
            data["application_id"],
        )
    return mapping


def _to_upload_doc(sim_doc: dict, primary_role: bool = True) -> dict:
    """Coerce the simulation doc shape to /documents/upload's
    ``DocumentSchema`` — extracted_fields nested under the key the
    schema expects."""
    return {
        "document_id":       sim_doc["document_id"],
        "document_type":     sim_doc["document_type"],
        "document_category": sim_doc.get("category") or "income",
        "borrower_role":     sim_doc.get("borrower_role", "primary"),
        "status":            "indexed",
        "confidence_score":  0.94,
        "extracted_fields":  sim_doc.get("extracted_fields") or {},
    }


async def _api_upload_window(
    client: _APIClient,
    docs: list[dict],
    los_to_real: dict,
) -> tuple[int, int]:
    """POST a batch of docs grouped per LOS into /documents/upload.
    Returns ``(uploaded_count, failed_count)``."""
    by_los: dict[str, list[dict]] = defaultdict(list)
    for d in docs:
        los = d.get("los_id")
        if not los or los not in los_to_real:
            continue
        by_los[los].append(d)

    uploaded = failed = 0
    for los, batch in by_los.items():
        primary_aid, _co_aid, app_id = los_to_real[los]
        body = {
            "applicant_id":   primary_aid,
            "application_id": app_id,
            "all_documents":  [_to_upload_doc(d) for d in batch],
        }
        resp = await client.post("/documents/upload", body)
        if resp.status_code in (200, 201):
            uploaded += len(batch)
        else:
            failed += len(batch)
            logger.warning(
                "upload_failed los=%s status=%s body=%s",
                los, resp.status_code, resp.text[:200],
            )
    return uploaded, failed


async def _api_read_entity_state(
    client: _APIClient, applicant_id: str,
) -> Optional[dict]:
    resp = await client.get(f"/entity/{applicant_id}/state")
    if resp.status_code == 200:
        return resp.json()
    return None


async def _api_read_context(
    client: _APIClient, application_id: str,
) -> Optional[dict]:
    """Fallback when /entity/{id}/state isn't deployed — the older
    /application/{id}/context surface still exists everywhere."""
    resp = await client.get(f"/application/{application_id}/context")
    if resp.status_code == 200:
        return resp.json()
    return None


async def _api_final_report(
    client: _APIClient, los_to_real: dict, totals: dict,
) -> None:
    bar = "═" * 60
    print(f"\n{bar}")
    print(f"  BACKTEST COMPLETE — {totals['days']} days, API mode")
    print(bar)
    print(f"  Total upload calls:  {totals['windows']}")
    print(f"  Total docs uploaded: {totals['uploaded']}")
    print(f"  Total docs failed:   {totals['failed']}")
    print()
    print("  Per-loan summary (read back from API):")
    for los_id, (primary_aid, co_aid, app_id) in los_to_real.items():
        st = await _api_read_entity_state(client, primary_aid)
        if st:
            state = st.get("state") or {}
            if isinstance(state, str):
                try:
                    state = _json.loads(state)
                except Exception:
                    state = {}
            cp = state.get("completeness_pct") or st.get("completeness_pct") or 0
            doc_count = st.get("document_count") or 0
            edges     = st.get("graph_edge_count") or 0
            confl     = st.get("conflict_count") or 0
            verdict = "complete" if cp >= 100 else f"{cp:.0f}% complete"
            print(f"    {los_id} → {primary_aid}: "
                  f"{doc_count:>3} docs, {verdict:<14} "
                  f"edges={edges:<3} conflicts={confl:<2} (entity_states)")
            continue

        # Fallback to /application/{id}/context — older deployments.
        ctx_resp = await _api_read_context(client, app_id)
        if ctx_resp:
            data = (ctx_resp or {}).get("data") or {}
            readiness = data.get("readiness") or {}
            ready_n = sum(1 for v in readiness.values() if v is True)
            ready_d = sum(1 for v in readiness.values() if isinstance(v, bool))
            gs = data.get("graph_summary") or {}
            print(f"    {los_id} → {primary_aid}: "
                  f"docs={gs.get('document_count', '—')} "
                  f"edges={gs.get('relationship_count', '—')} "
                  f"conflicts={gs.get('conflict_count', '—')} "
                  f"readiness={ready_n}/{ready_d} (context)")
        else:
            print(f"    {los_id} → {primary_aid}: (state unavailable)")

    # /graph/build-runs — best-effort. Skipping noise on 404.
    print()
    runs_resp = await client.get(
        "/graph/build-runs",
        params={"date_from": "2026-01-01", "date_to": "2026-02-19", "limit": 5},
    )
    if runs_resp.status_code == 200:
        rows = runs_resp.json().get("build_runs") or []
        if rows:
            print("  Watermark trail (first 5 from /graph/build-runs):")
            for r in rows[:5]:
                print(f"    {r.get('build_date')} #{r.get('build_number')}: "
                      f"+{r.get('documents_new')} docs, "
                      f"wm {str(r.get('watermark_from'))[:19]} "
                      f"→ {str(r.get('watermark_to'))[:19]}")
        else:
            print("  /graph/build-runs returned no rows "
                  "(builder hasn't fired remotely — expected for API mode).")
    else:
        print(f"  /graph/build-runs unavailable (HTTP {runs_resp.status_code}) — "
              "expected on older deploys.")

    wm_resp = await client.get("/graph/watermark")
    if wm_resp.status_code == 200:
        print(f"  /graph/watermark: {wm_resp.json()}")
    print(bar)


async def amain_api(args) -> int:
    """API-mode driver — bootstraps loans + uploads windows + reads
    state back via HTTP. Doesn't touch local PG / Redis at all."""
    if not args.api_key:
        print("ERROR: --api-key is required when --api-url is set.", file=sys.stderr)
        return 2

    client = _APIClient(args.api_url, args.api_key)

    # 1: bootstrap loans
    print(f"Bootstrapping 5 loans against {args.api_url} …")
    try:
        los_to_real = await _api_bootstrap_loans(client)
    except Exception as exc:
        print(f"ERROR: bootstrap failed — {exc}", file=sys.stderr)
        await client.aclose()
        return 1
    for los_id, (primary_aid, co_aid, app_id) in los_to_real.items():
        print(f"  {los_id} → {primary_aid}{' / ' + co_aid if co_aid else ''} "
              f"({app_id})")

    # 2: read every doc once + sort by received_at so the build-window
    # filter is cheap.
    s3_path = Path(args.s3_path)
    if not s3_path.exists():
        print(f"ERROR: simulation path not found: {s3_path}", file=sys.stderr)
        await client.aclose()
        return 1
    all_docs = _walk_simulation_docs(s3_path)
    all_docs.sort(key=lambda d: d.get("received_at") or "")
    print(f"Loaded {len(all_docs)} docs from {s3_path}")

    # 3: determine date range + clocks
    if args.day:
        start = end = date.fromisoformat(args.day)
    else:
        start = date.fromisoformat(args.start)
        end   = start + timedelta(days=args.days - 1)
    clocks = _build_clocks(args.builds_per_day)
    print(f"Backtest: {start} → {end}, {args.builds_per_day} builds/day at {clocks} UTC")

    totals = {"days": (end - start).days + 1, "windows": 0,
              "uploaded": 0, "failed": 0}
    last_window_end = datetime(2025, 12, 31, tzinfo=timezone.utc)

    day = start
    while day <= end:
        per_build_uploaded: list[int] = []
        for build_number, hour in enumerate(clocks, 1):
            window_end = datetime.combine(
                day, datetime.min.time().replace(hour=hour, minute=59, second=59),
                tzinfo=timezone.utc,
            )
            window_docs = _docs_in_window(all_docs, last_window_end, window_end)
            uploaded, failed = await _api_upload_window(client, window_docs, los_to_real)
            totals["windows"]  += 1
            totals["uploaded"] += uploaded
            totals["failed"]   += failed
            per_build_uploaded.append(uploaded)
            last_window_end = window_end

        parts = [f"Build {i+1}: +{n} docs" for i, n in enumerate(per_build_uploaded)]
        print(f"Day {day} | " + " | ".join(parts))
        day = day + timedelta(days=1)

    await _api_final_report(client, los_to_real, totals)
    await client.aclose()
    return 0


# ===========================================================================
# Entry point
# ===========================================================================


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--days",  type=int, default=DEFAULT_DAYS)
    ap.add_argument("--day",   default=None,
                    help="single-day re-run; overrides --start/--days")
    ap.add_argument("--builds-per-day", type=int, default=DEFAULT_BUILDS)
    ap.add_argument("--s3-path", default=DEFAULT_S3_PATH)
    ap.add_argument("--reset", action="store_true",
                    help="(inproc only) wipe entity_states / snapshots / build_runs / watermark")
    ap.add_argument("--api-url", default=None,
                    help="run against a remote EDMS API instead of inproc PG/Redis")
    ap.add_argument("--api-key", default=None,
                    help="X-API-Key for --api-url; required when --api-url is set")
    args = ap.parse_args()

    if args.api_url:
        sys.exit(asyncio.run(amain_api(args)))
    sys.exit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
