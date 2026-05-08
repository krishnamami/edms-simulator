#!/usr/bin/env python3
"""Feed 5 chaos loan scenarios through EDMS and report what breaks.

For each scenario:
  1. Create the application
  2. Upload all docs (with messy/malformed fields)
  3. Check what survived: income, credit, assets, identity, property, context
  4. Report: SURVIVED / DEGRADED / CRASHED for each component

Usage:
    python scripts/feed_chaos_loans.py
    python scripts/feed_chaos_loans.py --scenario 4    # run one scenario
    python scripts/feed_chaos_loans.py -v              # verbose
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import traceback
import uuid
from typing import Optional

import httpx

BASE_URL = os.getenv("EDMS_API_URL", "http://localhost:8001")
API_KEY = os.getenv("EDMS_API_KEY", "edms_dev_key")
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}
CHAOS_DIR = os.getenv("CHAOS_DIR",
    os.path.join(os.path.dirname(__file__), "chaos_loan_files"))

logger = logging.getLogger("chaos_feed")


class ScenarioResult:
    def __init__(self, name: str):
        self.name = name
        self.uploads_attempted = 0
        self.uploads_succeeded = 0
        self.uploads_failed = 0
        self.upload_errors: list[str] = []
        self.components: dict[str, str] = {}  # component → SURVIVED/DEGRADED/CRASHED
        self.component_details: dict[str, str] = {}
        self.warnings: list[str] = []
        self.crashes: list[str] = []

    def component(self, name: str, status: str, detail: str = ""):
        self.components[name] = status
        if detail:
            self.component_details[name] = detail
        if status == "CRASHED":
            self.crashes.append(f"{name}: {detail}")

    def warn(self, msg: str):
        self.warnings.append(msg)


async def post(client, path, body):
    return await client.post(f"{BASE_URL}{path}", json=body, headers=HEADERS, timeout=30)


async def get(client, path):
    return await client.get(f"{BASE_URL}{path}", headers=HEADERS, timeout=30)


async def create_application(client, manifest) -> dict:
    borrower = manifest["borrower"]
    uid = uuid.uuid4().hex[:6].upper()
    payload = {
        "los_id": f"{manifest['los_id']}-{uid}",
        "borrower": {
            "first_name": borrower["first"],
            "last_name": borrower["last"],
            "dob": "1985-06-20",
            "ssn_hash": f"hash_{uid}",
            "ssn_last4": borrower["ssn_last4"],
            "email": f"chaos_{uid}@test.com",
        },
        "loan": manifest.get("loan") or {
            "loan_amount": 350000, "interest_rate": 6.5, "loan_term_months": 360,
        },
        "documents": [],
    }
    co = manifest.get("co_borrower")
    if co:
        payload["co_borrower"] = {
            "first_name": co["first"],
            "last_name": co["last"],
            "dob": "1987-09-10",
            "ssn_hash": f"cohash_{uid}",
            "ssn_last4": co["ssn_last4"],
        }
    resp = await post(client, "/loans", payload)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Create app failed: {resp.status_code} {resp.text[:200]}")
    return resp.json()


async def upload_doc(client, applicant_id, application_id, doc_entry,
                     los_id, co_applicant_id=None) -> tuple[bool, str]:
    """Upload a single doc. Returns (success, error_msg)."""
    target_id = co_applicant_id if doc_entry.get("role") == "co_borrower" and co_applicant_id else applicant_id

    doc_id = doc_entry.get("force_doc_id") or f"DOC-{los_id}-{doc_entry['doc_type']}-{uuid.uuid4().hex[:4]}"

    doc = {
        "document_id": doc_id,
        "document_type": doc_entry["doc_type"],
        "document_category": doc_entry["category"],
        "borrower_role": doc_entry.get("role", "primary"),
        "status": "indexed",
        "confidence_score": 0.90,
    }
    # Merge fields — including potentially malformed ones
    fields = doc_entry.get("fields", {})
    if fields:
        doc.update(fields)

    payload = {
        "applicant_id": target_id,
        "application_id": application_id,
        "all_documents": [doc],
    }

    try:
        resp = await post(client, "/documents/upload", payload)
        if resp.status_code in (200, 201):
            return True, ""
        else:
            return False, f"HTTP {resp.status_code}: {resp.text[:150]}"
    except Exception as exc:
        return False, f"Exception: {str(exc)[:150]}"


async def check_component(client, name, url, checks) -> tuple[str, str]:
    """Hit an endpoint and run checks. Returns (status, detail).

    Auto-unwraps the standard ``{"source": ..., "data": {...}}``
    envelope used by /context, /graph/summary, /income-profile,
    /credit-profile, etc. so check lambdas only need to know the
    inner payload's shape.
    """
    try:
        resp = await get(client, url)
        if resp.status_code != 200:
            return "CRASHED", f"HTTP {resp.status_code}"

        body = resp.json()
        # Unwrap {"source": ..., "data": {...}} when present.
        if (
            isinstance(body, dict)
            and "data" in body
            and isinstance(body["data"], dict)
        ):
            data = body["data"]
        else:
            data = body
        issues = []

        for check_name, check_fn in checks:
            try:
                result = check_fn(data)
                if result is not None:
                    issues.append(f"{check_name}: {result}")
            except Exception as exc:
                issues.append(f"{check_name}: EXCEPTION {str(exc)[:80]}")

        if not issues:
            return "SURVIVED", "all checks passed"
        else:
            return "DEGRADED", "; ".join(issues[:3])

    except Exception as exc:
        return "CRASHED", f"Exception: {str(exc)[:150]}"


async def run_scenario(client, scenario_dir: str, result: ScenarioResult):
    """Run a single chaos scenario."""
    manifest_path = os.path.join(scenario_dir, "manifest.json")
    if not os.path.exists(manifest_path):
        result.component("setup", "CRASHED", f"No manifest.json in {scenario_dir}")
        return

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    # Create application
    try:
        app = await create_application(client, manifest)
        applicant_id = app["applicant_id"]
        application_id = app["application_id"]
        co_applicant_id = app.get("co_applicant_id")
        los_id = manifest["los_id"]
        result.component("app_creation", "SURVIVED", f"app={application_id}")
    except Exception as exc:
        result.component("app_creation", "CRASHED", str(exc)[:200])
        return

    # Upload all docs
    for doc_entry in manifest["documents"]:
        result.uploads_attempted += 1
        success, error = await upload_doc(
            client, applicant_id, application_id, doc_entry,
            los_id, co_applicant_id
        )
        if success:
            result.uploads_succeeded += 1
        else:
            result.uploads_failed += 1
            result.upload_errors.append(
                f"{doc_entry['doc_type']}: {error}"
            )

    # Wait for assembly
    await asyncio.sleep(2.0)

    # All endpoints below return the standard
    # {"source": ..., "data": {...}} envelope that check_component
    # auto-unwraps. The lambdas read the inner payload directly.

    # Check income
    status, detail = await check_component(
        client, "income",
        f"/applicant/{applicant_id}/income-profile",
        [
            ("qualifying > 0", lambda d: None if (d.get("combined_qualifying_monthly") or 0) > 0 else "zero income"),
        ]
    )
    result.component("income", status, detail)

    # Check credit (response shape uses ``mid_score`` at top level of
    # the credit profile dict; ``primary_mid_score`` only appears in
    # the slice envelope)
    status, detail = await check_component(
        client, "credit",
        f"/applicant/{applicant_id}/credit-profile",
        [
            ("has_score", lambda d: None if ((d.get("mid_score") or d.get("primary_mid_score") or 0)) > 0 else "no score"),
        ]
    )
    result.component("credit", status, detail)

    # Check assets + identity via context (borrower nests
    # income / credit / assets / identity / document_count / qualifying_monthly)
    status, detail = await check_component(
        client, "context",
        f"/application/{application_id}/context",
        [
            ("has_borrower", lambda d: None if d.get("borrower") else "no borrower section"),
            ("has_property", lambda d: None if d.get("property") or d.get("loan_terms") else "no property/loan_terms"),
            ("income_in_ctx", lambda d: None if ((d.get("borrower") or {}).get("qualifying_monthly") or d.get("combined_qualifying_monthly") or 0) > 0 else "zero income in context"),
            ("assets_section", lambda d: None if (d.get("borrower") or {}).get("assets") else "no assets section"),
            ("identity_section", lambda d: None if (d.get("borrower") or {}).get("identity") else "no identity section"),
            ("conflicts", lambda d: f"conflicts={(d.get('conflicts') or {}).get('count', '?')}" if (d.get("conflicts") or {}).get("count", 0) > 0 else None),
        ]
    )
    result.component("context", status, detail)

    # Check graph
    status, detail = await check_component(
        client, "graph",
        f"/applicant/{applicant_id}/graph/summary",
        [
            ("has_docs", lambda d: None if (d.get("document_count") or 0) > 0 else "zero docs in graph"),
            ("has_edges", lambda d: None if (d.get("relationship_count") or d.get("edge_count") or 0) >= 0 else "negative edges"),
        ]
    )
    result.component("graph", status, detail)

    # Check readiness
    status, detail = await check_component(
        client, "readiness",
        f"/application/{application_id}/readiness",
        [
            ("responds", lambda d: None),
            ("has_flags", lambda d: None if isinstance(d, dict) and len(d) > 0 else "empty readiness"),
        ]
    )
    result.component("readiness", status, detail)

    # Check missing docs
    status, detail = await check_component(
        client, "missing_docs",
        f"/application/{application_id}/missing-documents",
        [
            ("responds", lambda d: None),
        ]
    )
    result.component("missing_docs", status, detail)


def print_scenario_report(result: ScenarioResult):
    """Print a detailed report for one scenario."""
    print(f"\n{'━'*70}")
    print(f"  {result.name}")
    print(f"{'━'*70}")

    # Uploads
    upload_status = "ALL" if result.uploads_failed == 0 else f"{result.uploads_succeeded}/{result.uploads_attempted}"
    print(f"  Uploads: {upload_status} succeeded", end="")
    if result.uploads_failed > 0:
        print(f" ({result.uploads_failed} FAILED)")
        for err in result.upload_errors[:5]:
            print(f"    ✗ {err}")
    else:
        print()

    # Components
    print()
    for comp, status in result.components.items():
        icon = {"SURVIVED": "✓", "DEGRADED": "△", "CRASHED": "✗"}.get(status, "?")
        detail = result.component_details.get(comp, "")
        color_status = status
        print(f"    {icon} {comp:20s} {color_status:10s} {detail}")

    if result.warnings:
        print(f"\n  Warnings:")
        for w in result.warnings:
            print(f"    ⚠ {w}")

    survived = sum(1 for s in result.components.values() if s == "SURVIVED")
    degraded = sum(1 for s in result.components.values() if s == "DEGRADED")
    crashed = sum(1 for s in result.components.values() if s == "CRASHED")
    total = len(result.components)
    print(f"\n  Score: {survived} survived, {degraded} degraded, {crashed} crashed / {total} components")


def print_summary(results: list[ScenarioResult]):
    """Print the overall chaos test summary."""
    print(f"\n{'═'*70}")
    print(f"  CHAOS TEST SUMMARY")
    print(f"{'═'*70}")

    total_uploads = sum(r.uploads_attempted for r in results)
    total_succeeded = sum(r.uploads_succeeded for r in results)
    total_failed = sum(r.uploads_failed for r in results)

    total_components = sum(len(r.components) for r in results)
    total_survived = sum(sum(1 for s in r.components.values() if s == "SURVIVED") for r in results)
    total_degraded = sum(sum(1 for s in r.components.values() if s == "DEGRADED") for r in results)
    total_crashed = sum(sum(1 for s in r.components.values() if s == "CRASHED") for r in results)

    print(f"\n  Scenarios:    {len(results)}")
    print(f"  Uploads:      {total_succeeded}/{total_uploads} succeeded ({total_failed} failed)")
    print(f"  Components:   {total_survived} survived, {total_degraded} degraded, {total_crashed} crashed")
    print(f"  Crash rate:   {total_crashed}/{total_components} ({total_crashed/max(total_components,1)*100:.0f}%)")

    print(f"\n  Per-scenario breakdown:")
    for r in results:
        survived = sum(1 for s in r.components.values() if s == "SURVIVED")
        degraded = sum(1 for s in r.components.values() if s == "DEGRADED")
        crashed = sum(1 for s in r.components.values() if s == "CRASHED")
        total = len(r.components)
        bar = ("█" * survived) + ("▒" * degraded) + ("░" * crashed)
        print(f"    {r.name:40s} {bar} {survived}/{total}")

    # Collect all crashes across scenarios
    all_crashes = []
    for r in results:
        all_crashes.extend(r.crashes)

    all_upload_errors = []
    for r in results:
        all_upload_errors.extend(r.upload_errors)

    if all_crashes:
        print(f"\n  CRASHES (components that completely failed):")
        for c in all_crashes:
            print(f"    ✗ {c}")

    if all_upload_errors:
        print(f"\n  UPLOAD FAILURES ({len(all_upload_errors)} total):")
        for e in all_upload_errors[:10]:
            print(f"    ✗ {e}")
        if len(all_upload_errors) > 10:
            print(f"    ... and {len(all_upload_errors) - 10} more")

    print(f"\n{'═'*70}")
    if total_crashed == 0 and total_failed == 0:
        print(f"  VERDICT: ROBUST — no crashes, no upload failures")
        print(f"           {total_degraded} degraded components = expected for messy data")
    elif total_crashed == 0:
        print(f"  VERDICT: RESILIENT — no crashes, {total_failed} upload failures handled gracefully")
    else:
        print(f"  VERDICT: FRAGILE — {total_crashed} crashes need fixing before production")
    print(f"{'═'*70}")

    return total_crashed


async def main(scenarios_to_run: Optional[list[int]] = None):
    async with httpx.AsyncClient() as client:
        # Health check
        try:
            resp = await get(client, "/health")
            if resp.status_code != 200:
                print(f"API not healthy: {resp.status_code}")
                sys.exit(1)
        except Exception as exc:
            print(f"Cannot reach API at {BASE_URL}: {exc}")
            sys.exit(1)

        scenario_dirs = sorted([
            d for d in os.listdir(CHAOS_DIR)
            if os.path.isdir(os.path.join(CHAOS_DIR, d)) and d.startswith("scenario_")
        ])

        if not scenario_dirs:
            print(f"No scenario directories found in {CHAOS_DIR}")
            print(f"Run generate_chaos_loans.py first to create them.")
            sys.exit(1)

        results: list[ScenarioResult] = []

        for i, scenario_name in enumerate(scenario_dirs, 1):
            if scenarios_to_run and i not in scenarios_to_run:
                continue

            scenario_path = os.path.join(CHAOS_DIR, scenario_name)
            result = ScenarioResult(scenario_name)

            logger.info(f"Running {scenario_name}...")
            try:
                await run_scenario(client, scenario_path, result)
            except Exception as exc:
                result.component("scenario_runner", "CRASHED",
                                 f"{str(exc)[:150]}\n{traceback.format_exc()[-200:]}")

            print_scenario_report(result)
            results.append(result)

        crashes = print_summary(results)
        sys.exit(1 if crashes > 0 else 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EDMS Chaos Test")
    parser.add_argument("--scenario", "-s", type=int, action="append",
                        help="Run specific scenario(s) by number (1-5)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)-5s %(message)s",
                        datefmt="%H:%M:%S")
    # Suppress httpx info logs unless verbose
    if not args.verbose:
        logging.getLogger("httpx").setLevel(logging.WARNING)

    asyncio.run(main(scenarios_to_run=args.scenario))
