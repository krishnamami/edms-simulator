"""Identity-resolver smoke test (no I/O)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.identity.golden_record import GoldenRecord
from core.identity.resolver import IdentityResolver, IdentitySignals, MatchMethod
from core.identity.xref_store import XRefStore


def main():
    store = XRefStore()
    resolver = IdentityResolver(store, store)

    s1 = IdentitySignals(
        los_id="LOS-1",
        first_name="Alice",
        last_name="Adams",
        dob="1990-01-01",
        ssn_hash=GoldenRecord.hash_ssn("111-22-3333"),
        ssn_last4="3333",
    )
    r1 = resolver.resolve(s1)
    assert r1.match_method == MatchMethod.NEW_RECORD, r1
    print(f"[PASS] new record -> {r1.applicant_id}")

    s2 = IdentitySignals(
        los_id="LOS-2",
        first_name="Alice",
        last_name="Adams",
        dob="1990-01-01",
        ssn_hash=GoldenRecord.hash_ssn("111-22-3333"),
        ssn_last4="3333",
    )
    r2 = resolver.resolve(s2)
    assert r2.match_method == MatchMethod.DETERMINISTIC, r2
    assert r2.applicant_id == r1.applicant_id
    print(f"[PASS] deterministic SSN match -> {r2.applicant_id}")

    s3 = IdentitySignals(
        los_id="LOS-3",
        first_name="Alice",
        last_name="Adams",
        dob="1990-01-01",
        email="alice@example.com",
    )
    r3 = resolver.resolve(s3)
    assert r3.match_method == MatchMethod.PROBABILISTIC, r3
    print(f"[PASS] probabilistic match score={r3.match_confidence:.2f}")

    print("\nAll identity smoke checks passed.")


if __name__ == "__main__":
    main()
