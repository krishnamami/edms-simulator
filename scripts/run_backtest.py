"""Backtest engine: simulate N days of incremental graph building.

Walks ``local_storage/s3_simulation`` (or an S3 bucket) day-by-day,
runs ``BUILDS_PER_DAY`` incremental builds per simulated day, takes
EOD snapshots, and prints a report card.

Usage:
    python scripts/run_backtest.py
    python scripts/run_backtest.py --builds-per-day 3
    python scripts/run_backtest.py --start 2026-01-01 --days 50
    python scripts/run_backtest.py --day 2026-01-15  # single-day re-run

The runner is **inproc** — it talks to PG / Redis directly, not via the
HTTP layer, so it doesn't require the API to be running. The HTTP
endpoints in api/routes.py read the same tables and surface the
results back to operators / dashboards.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--days",  type=int, default=DEFAULT_DAYS)
    ap.add_argument("--day",   default=None,
                    help="single-day re-run; overrides --start/--days")
    ap.add_argument("--builds-per-day", type=int, default=DEFAULT_BUILDS)
    ap.add_argument("--s3-path", default=DEFAULT_S3_PATH)
    ap.add_argument("--reset", action="store_true",
                    help="wipe entity_states / snapshots / build_runs / watermark before run")
    args = ap.parse_args()
    sys.exit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
