"""RedisStore tests against fakeredis (async client)."""
import pytest

from core.storage.redis_store import RedisStore


@pytest.mark.asyncio
async def test_set_and_get_income_profile():
    store = RedisStore()
    profile = {"applicant_id": "APL-1", "combined_qualifying_monthly": 8000}
    assert await store.set_income_profile("APL-1", profile)
    fetched = await store.get_income_profile("APL-1")
    assert fetched == profile


@pytest.mark.asyncio
async def test_get_missing_returns_none():
    store = RedisStore()
    assert await store.get_income_profile("DOES-NOT-EXIST") is None


@pytest.mark.asyncio
async def test_set_and_get_credit_profile():
    store = RedisStore()
    profile = {"applicant_id": "APL-1", "mid_score": 720}
    await store.set_credit_profile("APL-1", profile)
    assert await store.get_credit_profile("APL-1") == profile


@pytest.mark.asyncio
async def test_set_and_get_status():
    store = RedisStore()
    await store.set_status("APL-1", "active")
    assert await store.get_status("APL-1") == "active"


@pytest.mark.asyncio
async def test_set_and_get_app_lookup():
    store = RedisStore()
    record = {"application_id": "APP-1", "applicant_id": "APL-1"}
    await store.set_app_lookup("LOS-1", record)
    assert await store.get_app_lookup("LOS-1") == record


@pytest.mark.asyncio
async def test_ping():
    store = RedisStore()
    assert await store.ping()
