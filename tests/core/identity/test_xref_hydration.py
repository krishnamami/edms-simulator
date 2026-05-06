"""XRefStore.hydrate_from_postgres — load existing applicants at startup.

These tests guard the data-overwrite bug surfaced in production: without
hydration, a uvicorn restart leaves XRefStore empty, ``next_sequence``
issues ``APL-00001-P`` again, and ``save_golden_record``'s
``ON CONFLICT (applicant_id) DO UPDATE`` silently overwrites the
existing person.
"""
from datetime import date, datetime

import pytest

from core.identity.golden_record import GoldenRecord
from core.identity.xref_store import XRefStore


def _gr_dict(seq: int, ssn_hash: str, full_name: str = "Test Person") -> dict:
    """Postgres-shape applicant row: dob is a date object, identity_xrefs
    is a list of dicts (as JSONB returns)."""
    return {
        "applicant_id":   f"APL-{seq:05d}-P",
        "full_name":      full_name,
        "first_name":     full_name.split()[0],
        "last_name":      full_name.split()[-1],
        "dob":            date(1985, 1, 1),
        "ssn_hash":       ssn_hash,
        "ssn_last4":      "0000",
        "email":          None,
        "phone":          None,
        "address_current": None,
        "status":         "active",
        "identity_xrefs": [
            {
                "xref_id":          f"xref-{seq}",
                "applicant_id":     f"APL-{seq:05d}-P",
                "source_system":    "los",
                "source_id":        f"LOS-{seq:03d}",
                "match_confidence": 1.0,
                "match_method":     "deterministic",
                "added_at":         datetime.utcnow(),
            }
        ],
        "application_ids": [],
        "created_at":     datetime.utcnow(),
        "updated_at":     datetime.utcnow(),
    }


@pytest.mark.asyncio
async def test_hydrate_loads_applicants(postgres_store):
    """Every row from get_all_applicants ends up in the in-memory caches."""
    postgres_store.applicants = {
        "APL-00001-P": _gr_dict(1, "ssn-aaa", "Alice Anderson"),
        "APL-00003-P": _gr_dict(3, "ssn-ccc", "Carol Carter"),
    }
    store = XRefStore()
    loaded, max_seq = await store.hydrate_from_postgres(postgres_store)

    assert loaded == 2
    assert store.find_by_applicant_id("APL-00001-P") is not None
    assert store.find_by_applicant_id("APL-00003-P") is not None
    assert store.find_by_ssn_hash("ssn-aaa").full_name == "Alice Anderson"
    assert store.find_by_ssn_hash("ssn-ccc").full_name == "Carol Carter"


@pytest.mark.asyncio
async def test_hydrate_advances_sequence(postgres_store):
    """next_sequence after hydration must continue past the highest stored id."""
    postgres_store.applicants = {
        "APL-00001-P": _gr_dict(1, "ssn-aaa"),
        "APL-00007-P": _gr_dict(7, "ssn-bbb"),
        "APL-00003-P": _gr_dict(3, "ssn-ccc"),
    }
    store = XRefStore()
    _, max_seq = await store.hydrate_from_postgres(postgres_store)

    assert max_seq == 7
    # First fresh allocation must be 8, not 1
    assert store.next_sequence() == 8


@pytest.mark.asyncio
async def test_hydrate_empty_db_is_noop(postgres_store):
    """No rows -> store stays empty, sequence stays at 0."""
    store = XRefStore()
    loaded, max_seq = await store.hydrate_from_postgres(postgres_store)

    assert loaded == 0
    assert max_seq == 0
    assert store.next_sequence() == 1


@pytest.mark.asyncio
async def test_save_after_hydrate_doesnt_collide(postgres_store):
    """The original production bug: a fresh save after restart must NOT
    re-issue APL-00001-P when it's already taken."""
    postgres_store.applicants = {
        "APL-00001-P": _gr_dict(1, "ssn-existing", "Existing Person"),
    }
    store = XRefStore()
    await store.hydrate_from_postgres(postgres_store)

    # New person arrives — different SSN, no match
    next_id = store.next_sequence()
    new_gr = GoldenRecord(
        applicant_id=GoldenRecord.generate_applicant_id(next_id, "P"),
        full_name="Brand New Person",
        first_name="Brand",
        last_name="New",
        dob="1990-01-01",
        ssn_hash="ssn-fresh",
        ssn_last4="9999",
    )

    assert new_gr.applicant_id == "APL-00002-P"
    assert new_gr.applicant_id != "APL-00001-P"
    # Old person still findable
    assert store.find_by_applicant_id("APL-00001-P").full_name == "Existing Person"


@pytest.mark.asyncio
async def test_hydrate_preserves_xref_index(postgres_store):
    """Identity xrefs reconstruct so deterministic source_system + source_id
    matches still work after a restart."""
    postgres_store.applicants = {
        "APL-00001-P": _gr_dict(1, "ssn-aaa"),
    }
    store = XRefStore()
    await store.hydrate_from_postgres(postgres_store)

    # The seeded gr has an xref to source_system='los', source_id='LOS-001'
    found = store.find_by_source_id("los", "LOS-001")
    assert found is not None
    assert found.applicant_id == "APL-00001-P"
