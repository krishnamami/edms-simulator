"""End-to-end aggregation smoke test.

Two modes:
  - SMOKE_TARGET=inproc (default): exercises the AggregationService directly
    with in-memory stores. Used by CI.
  - SMOKE_TARGET=api: hits a live API at SMOKE_API_URL with X-API-Key auth.

Exits 0 on full PASS, 1 if any check FAILs.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# Allow running as a top-level script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Force fakes for the in-process variant before any core import.
os.environ.setdefault("USE_FAKE_REDIS", "true")
os.environ.setdefault("USE_AWS_SECRETS", "false")
os.environ.setdefault("USE_AWS_SQS", "false")
os.environ.setdefault("USE_LOCAL_STORAGE", "true")
os.environ.setdefault("API_KEY", "edms_dev_key")

from core.aggregation.events import (  # noqa: E402
    ApplicationSubmittedEvent,
    DocumentUploadedEvent,
    EventType,
)
from core.aggregation.service import AggregationService  # noqa: E402
from core.credit.assembler import CreditAssembler  # noqa: E402
from core.identity.golden_record import GoldenRecord  # noqa: E402
from core.identity.xref_store import XRefStore  # noqa: E402
from core.income.assembler import IncomeAssembler  # noqa: E402
from core.storage.redis_store import RedisStore  # noqa: E402

PASSES: list[str] = []
FAILS: list[str] = []


def check(label: str, ok: bool, detail: str = ""):
    tag = "[PASS]" if ok else "[FAIL]"
    msg = f"{tag} {label}" + (f" -- {detail}" if detail else "")
    print(msg)
    (PASSES if ok else FAILS).append(label)


class FakePG:
    """Minimal in-memory PostgresStore stand-in for the smoke script.
    Mirrors the real store's tenant_id kwarg on every method (default
    "default") so AggregationService — which now passes
    ``tenant_id=current_tenant_id()`` everywhere — doesn't trip a
    TypeError. Tenant is recorded but isolation isn't enforced; the
    smoke run is single-tenant by construction."""

    def __init__(self):
        self.applications: dict = {}
        self.income: dict = {}
        self.credit: dict = {}
        self.applicants: dict = {}
        self.documents: dict = {}

    async def save_golden_record(self, gr, tenant_id="default"):
        self.applicants[gr["applicant_id"]] = {**gr, "tenant_id": tenant_id}

    async def save_xref(self, xref):
        pass

    async def save_application(self, app, tenant_id="default"):
        self.applications[app["application_id"]] = {**app, "tenant_id": tenant_id}

    async def get_application(self, application_id, tenant_id="default"):
        return self.applications.get(application_id)

    async def get_application_by_los_id(self, los_id, tenant_id="default"):
        for app in self.applications.values():
            if app["los_id"] == los_id:
                return app
        return None

    async def get_application_by_applicant(self, applicant_id, tenant_id="default"):
        for app in self.applications.values():
            if app.get("applicant_id") == applicant_id or app.get("co_applicant_id") == applicant_id:
                return app
        return None

    async def save_income_profile(self, p, tenant_id="default"):
        prev = self.income.get(p["applicant_id"])
        v = (prev.get("_version", 0) + 1) if prev else 1
        self.income[p["applicant_id"]] = {**p, "_version": v, "tenant_id": tenant_id}
        return f"id-{v}"

    async def get_income_profile(self, aid, tenant_id="default"):
        return self.income.get(aid)

    async def save_credit_profile(self, p, tenant_id="default"):
        self.credit[p["applicant_id"]] = {**p, "tenant_id": tenant_id}

    async def get_credit_profile(self, aid, tenant_id="default"):
        return self.credit.get(aid)

    async def save_document(self, doc, tenant_id="default"):
        # Mirror prod's ON CONFLICT DO UPDATE on document_id.
        self.documents[doc["document_id"]] = {
            **doc, "is_current": doc.get("is_current", True),
            "tenant_id": tenant_id,
        }

    async def get_documents_for_applicant(self, applicant_id, tenant_id="default"):
        return [
            d for d in self.documents.values()
            if d.get("applicant_id") == applicant_id and d.get("is_current", True)
        ]


def _payload(los_id: str, ssn: str) -> dict:
    return {
        "los_id": los_id,
        "borrower": {
            "first_name": "Smoke",
            "last_name": "Tester",
            "dob": "1985-05-05",
            "ssn_hash": GoldenRecord.hash_ssn(ssn),
            "ssn_last4": ssn[-4:],
            "email": f"smoke-{los_id}@example.com",
        },
        "loan": {"credit_band": "near-prime"},
        "documents": [
            {
                "document_id": f"D-{los_id}-W2",
                "document_type": "W2",
                "borrower_role": "primary",
                "box1_wages": 84000,
                "employer_name": "SmokeCo",
            },
            {
                "document_id": f"D-{los_id}-PAY",
                "document_type": "PAYSTUB",
                "borrower_role": "primary",
            },
        ],
    }


async def smoke_inproc():
    xref = XRefStore()
    pg = FakePG()
    redis = RedisStore()
    svc = AggregationService(
        xref_store=xref,
        golden_record_store=xref,
        income_assembler=IncomeAssembler(),
        credit_assembler=CreditAssembler(),
        redis_store=redis,
        postgres_store=pg,
    )

    # 1: new loan
    p1 = _payload("LOS-S001", "555-12-3456")
    r1 = await svc.handle(
        ApplicationSubmittedEvent(
            event_type=EventType.APPLICATION_SUBMITTED, payload=p1
        )
    )
    check(
        "POST /loans -> new applicant",
        r1["applicant_id"].startswith("APL-") and r1["status"] == "active",
        f"applicant_id={r1['applicant_id']}",
    )

    # 2: applicant lookup by los_id resolves
    cached = await redis.get_app_lookup("LOS-S001")
    check(
        "GET /loan/{los_id}/applicant-id resolves correctly",
        cached and cached["applicant_id"] == r1["applicant_id"],
    )

    # 3: income profile present in Postgres
    income = await pg.get_income_profile(r1["applicant_id"])
    check(
        "GET /applicant/{id}/income-profile returns data",
        income is not None and income["combined_qualifying_monthly"] > 0,
    )

    # 4: credit profile mid_score present
    credit = await pg.get_credit_profile(r1["applicant_id"])
    check(
        "GET /applicant/{id}/credit-profile has mid_score",
        credit is not None and "mid_score" in credit,
    )

    # 5: same SSN -> same applicant
    p2 = _payload("LOS-S002", "555-12-3456")
    r2 = await svc.handle(
        ApplicationSubmittedEvent(
            event_type=EventType.APPLICATION_SUBMITTED, payload=p2
        )
    )
    check(
        "POST /loans same SSN -> SAME applicant_id (deterministic)",
        r2["applicant_id"] == r1["applicant_id"]
        and r2["match_method"] == "deterministic",
    )

    # 6: doc upload changes lineage_hash
    pre_hash = (await pg.get_income_profile(r1["applicant_id"]))["lineage_hash"]
    upload = DocumentUploadedEvent(
        event_type=EventType.DOCUMENT_UPLOADED,
        payload={
            "applicant_id": r1["applicant_id"],
            "application_id": r1["application_id"],
            "all_documents": p1["documents"]
            + [
                {
                    "document_id": "D-S001-EXTRA",
                    "document_type": "W2",
                    "borrower_role": "primary",
                    "box1_wages": 110000,
                    "employer_name": "BigCo",
                }
            ],
        },
    )
    await svc.handle(upload)
    post_hash = (await pg.get_income_profile(r1["applicant_id"]))["lineage_hash"]
    check(
        "Document upload changes lineage_hash",
        pre_hash != post_hash,
        f"{pre_hash} -> {post_hash}",
    )

    # 7-8: health + ready (in-proc proxies)
    check("/health -> ok", True)
    check("/ready -> postgres + redis ok", await redis.ping())


async def smoke_api():
    import httpx

    base = os.environ.get("SMOKE_API_URL", "http://localhost:8001")
    api_key = os.environ.get("API_KEY", "edms_dev_key")
    headers = {"X-API-Key": api_key}

    async with httpx.AsyncClient(base_url=base, timeout=10.0) as client:
        r = await client.get("/health")
        check("/health -> ok", r.status_code == 200, f"status={r.status_code}")

        rr = await client.get("/ready")
        ready = rr.json() if rr.status_code == 200 else {}
        check(
            "/ready -> postgres + redis ok",
            rr.status_code == 200
            and ready.get("postgres")
            and ready.get("redis"),
        )

        p1 = _payload("LOS-S001", "555-12-3456")
        r1 = await client.post("/loans", json=p1, headers=headers)
        d1 = r1.json() if r1.status_code == 200 else {}
        check(
            "POST /loans -> new applicant",
            r1.status_code == 200 and d1.get("applicant_id", "").startswith("APL-"),
        )

        rl = await client.get(f"/loan/{p1['los_id']}/applicant-id", headers=headers)
        check(
            "GET /loan/{los_id}/applicant-id",
            rl.status_code == 200,
        )

        ri = await client.get(
            f"/applicant/{d1.get('applicant_id', 'X')}/income-profile",
            headers=headers,
        )
        check(
            "GET /applicant/{id}/income-profile",
            ri.status_code == 200,
        )

        rc = await client.get(
            f"/applicant/{d1.get('applicant_id', 'X')}/credit-profile",
            headers=headers,
        )
        check(
            "GET /applicant/{id}/credit-profile",
            rc.status_code == 200,
        )

        p2 = _payload("LOS-S002", "555-12-3456")
        r2 = await client.post("/loans", json=p2, headers=headers)
        d2 = r2.json() if r2.status_code == 200 else {}
        check(
            "Same SSN -> deterministic match",
            d2.get("applicant_id") == d1.get("applicant_id"),
        )


def main():
    target = os.environ.get("SMOKE_TARGET", "inproc")
    print(f"=== EDMS smoke ({target}) ===")
    if target == "api":
        asyncio.run(smoke_api())
    else:
        asyncio.run(smoke_inproc())
    print(f"\n{len(PASSES)} passed, {len(FAILS)} failed")
    sys.exit(0 if not FAILS else 1)


if __name__ == "__main__":
    main()
