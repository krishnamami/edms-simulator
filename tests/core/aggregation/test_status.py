"""StatusMachine transition tests."""
import pytest

from core.aggregation.status import (
    GoldenRecordStatus,
    StatusMachine,
)


def test_placeholder_to_resolving_allowed():
    assert StatusMachine.transition(
        GoldenRecordStatus.PLACEHOLDER, GoldenRecordStatus.RESOLVING
    ) == GoldenRecordStatus.RESOLVING


def test_resolving_to_active_allowed():
    assert StatusMachine.transition(
        GoldenRecordStatus.RESOLVING, GoldenRecordStatus.ACTIVE
    ) == GoldenRecordStatus.ACTIVE


def test_active_to_stale_allowed():
    assert StatusMachine.transition(
        GoldenRecordStatus.ACTIVE, GoldenRecordStatus.STALE
    ) == GoldenRecordStatus.STALE


def test_stale_back_to_active_allowed():
    assert StatusMachine.transition(
        GoldenRecordStatus.STALE, GoldenRecordStatus.ACTIVE
    ) == GoldenRecordStatus.ACTIVE


def test_placeholder_to_active_disallowed():
    with pytest.raises(ValueError):
        StatusMachine.transition(
            GoldenRecordStatus.PLACEHOLDER, GoldenRecordStatus.ACTIVE
        )


def test_active_to_resolving_disallowed():
    with pytest.raises(ValueError):
        StatusMachine.transition(
            GoldenRecordStatus.ACTIVE, GoldenRecordStatus.RESOLVING
        )


def test_can_transition_helper():
    assert StatusMachine.can_transition(
        GoldenRecordStatus.PLACEHOLDER, GoldenRecordStatus.RESOLVING
    )
    assert not StatusMachine.can_transition(
        GoldenRecordStatus.PLACEHOLDER, GoldenRecordStatus.ACTIVE
    )


def test_is_ready():
    assert StatusMachine.is_ready(GoldenRecordStatus.ACTIVE)
    assert not StatusMachine.is_ready(GoldenRecordStatus.PLACEHOLDER)
    assert not StatusMachine.is_ready(GoldenRecordStatus.STALE)
