"""RedisStore tests against fakeredis."""
from core.storage.redis_store import RedisStore


def test_set_and_get_income_profile():
    store = RedisStore()
    profile = {"applicant_id": "APL-1", "combined_qualifying_monthly": 8000}
    assert store.set_income_profile("APL-1", profile)
    fetched = store.get_income_profile("APL-1")
    assert fetched == profile


def test_get_missing_returns_none():
    store = RedisStore()
    assert store.get_income_profile("DOES-NOT-EXIST") is None


def test_set_and_get_credit_profile():
    store = RedisStore()
    profile = {"applicant_id": "APL-1", "mid_score": 720}
    store.set_credit_profile("APL-1", profile)
    assert store.get_credit_profile("APL-1") == profile


def test_set_and_get_status():
    store = RedisStore()
    store.set_status("APL-1", "active")
    assert store.get_status("APL-1") == "active"


def test_set_and_get_app_lookup():
    store = RedisStore()
    record = {"application_id": "APP-1", "applicant_id": "APL-1"}
    store.set_app_lookup("LOS-1", record)
    assert store.get_app_lookup("LOS-1") == record


def test_ping():
    store = RedisStore()
    assert store.ping()
