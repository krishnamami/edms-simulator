"""IdentityResolver match-strategy tests."""
import pytest

from core.identity.golden_record import GoldenRecord
from core.identity.resolver import IdentityResolver, IdentitySignals, MatchMethod
from core.identity.xref_store import XRefStore


@pytest.fixture
def store():
    return XRefStore()


@pytest.fixture
def resolver(store):
    return IdentityResolver(store, store)


def _signals(**kwargs):
    base = dict(
        los_id="LOS-X",
        first_name="Jane",
        last_name="Smith",
        dob="1985-04-22",
        ssn_hash=GoldenRecord.hash_ssn("999-88-7777"),
        ssn_last4="7777",
    )
    base.update(kwargs)
    return IdentitySignals(**base)


def test_first_signal_creates_new_record(resolver):
    result = resolver.resolve(_signals())
    assert result.is_new_record
    assert result.match_method == MatchMethod.NEW_RECORD
    assert result.golden_record.applicant_id.startswith("APL-")


def test_same_ssn_hash_deterministic_match(resolver):
    first = resolver.resolve(_signals(los_id="LOS-A"))
    second = resolver.resolve(_signals(los_id="LOS-B"))
    assert second.match_method == MatchMethod.DETERMINISTIC
    assert second.applicant_id == first.applicant_id
    assert second.is_new_record is False


def test_no_ssn_uses_probabilistic(resolver):
    resolver.resolve(_signals(los_id="LOS-A"))
    second = resolver.resolve(
        _signals(
            los_id="LOS-B",
            ssn_hash=None,
            ssn_last4=None,
            email="jane@example.com",
        )
    )
    # Probabilistic match because name + dob still align.
    assert second.match_method == MatchMethod.PROBABILISTIC
    assert second.match_confidence >= IdentityResolver.PROBABILISTIC_THRESHOLD


def test_distinct_person_creates_new_record(resolver):
    resolver.resolve(_signals(los_id="LOS-A"))
    other = resolver.resolve(
        IdentitySignals(
            los_id="LOS-C",
            first_name="Bob",
            last_name="Different",
            dob="1990-12-12",
            ssn_hash=GoldenRecord.hash_ssn("000-11-2222"),
            ssn_last4="2222",
        )
    )
    assert other.is_new_record
    assert other.applicant_id != "APL-00001-P"  # different person
