"""HTTP smoke test against a running API instance.

Set SMOKE_API_URL (default http://localhost:8001) and API_KEY.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx  # noqa: E402

from core.identity.golden_record import GoldenRecord  # noqa: E402

BASE = os.getenv("SMOKE_API_URL", "http://localhost:8001")
API_KEY = os.getenv("API_KEY", "edms_dev_key")
HEADERS = {"X-API-Key": API_KEY}


def _payload(los_id: str, ssn: str) -> dict:
    return {
        "los_id": los_id,
        "borrower": {
            "first_name": "Smoke",
            "last_name": "Api",
            "dob": "1985-05-05",
            "ssn_hash": GoldenRecord.hash_ssn(ssn),
            "ssn_last4": ssn[-4:],
            "email": f"smoke-{los_id}@example.com",
        },
        "loan": {"credit_band": "near-prime"},
        "documents": [
            {
                "document_id": f"DOC-{los_id}-W2",
                "document_type": "W2",
                "borrower_role": "primary",
                "box1_wages": 96000,
                "employer_name": "ApiCo",
            },
            {
                "document_id": f"DOC-{los_id}-PAY",
                "document_type": "PAYSTUB",
                "borrower_role": "primary",
            },
        ],
    }


async def main():
    fails = 0
    async with httpx.AsyncClient(base_url=BASE, timeout=10.0) as client:
        h = await client.get("/health")
        ok = h.status_code == 200
        print(("[PASS] " if ok else "[FAIL] ") + f"/health status={h.status_code}")
        fails += 0 if ok else 1

        r = await client.get("/ready")
        body = r.json() if r.status_code == 200 else {}
        ok = r.status_code == 200 and body.get("postgres") and body.get("redis")
        print(("[PASS] " if ok else "[FAIL] ") + f"/ready -> {body}")
        fails += 0 if ok else 1

        body = _payload("LOS-API-001", "555-66-7777")
        r = await client.post("/loans", headers=HEADERS, json=body)
        ok = r.status_code == 200 and r.json().get("applicant_id", "").startswith("APL-")
        print(("[PASS] " if ok else "[FAIL] ") + f"POST /loans -> {r.status_code}")
        fails += 0 if ok else 1
        applicant_id = r.json().get("applicant_id") if r.status_code == 200 else None

        if applicant_id:
            for path in (
                f"/loan/LOS-API-001/applicant-id",
                f"/applicant/{applicant_id}/income-profile",
                f"/applicant/{applicant_id}/credit-profile",
            ):
                rr = await client.get(path, headers=HEADERS)
                ok = rr.status_code == 200
                print(("[PASS] " if ok else "[FAIL] ") + f"GET {path} -> {rr.status_code}")
                fails += 0 if ok else 1

    print(f"\n{('FAIL' if fails else 'PASS')}: {fails} failures")
    sys.exit(0 if not fails else 1)


if __name__ == "__main__":
    asyncio.run(main())
