"""GoldenRecord status state machine."""
from enum import Enum


class GoldenRecordStatus(str, Enum):
    PLACEHOLDER = "placeholder"
    RESOLVING = "resolving"
    ACTIVE = "active"
    STALE = "stale"
    CONFLICT = "conflict"
    ERROR = "error"


VALID_TRANSITIONS = {
    GoldenRecordStatus.PLACEHOLDER: [
        GoldenRecordStatus.RESOLVING,
        GoldenRecordStatus.ERROR,
    ],
    GoldenRecordStatus.RESOLVING: [
        GoldenRecordStatus.ACTIVE,
        GoldenRecordStatus.CONFLICT,
        GoldenRecordStatus.ERROR,
    ],
    GoldenRecordStatus.ACTIVE: [
        GoldenRecordStatus.STALE,
        GoldenRecordStatus.ERROR,
    ],
    GoldenRecordStatus.STALE: [
        GoldenRecordStatus.ACTIVE,
        GoldenRecordStatus.ERROR,
    ],
    GoldenRecordStatus.CONFLICT: [
        GoldenRecordStatus.ACTIVE,
        GoldenRecordStatus.ERROR,
    ],
    GoldenRecordStatus.ERROR: [
        GoldenRecordStatus.RESOLVING,
    ],
}


class StatusMachine:
    @staticmethod
    def transition(
        current: GoldenRecordStatus, target: GoldenRecordStatus
    ) -> GoldenRecordStatus:
        if target not in VALID_TRANSITIONS.get(current, []):
            raise ValueError(
                f"Invalid transition: {current} -> {target}. "
                f"Allowed: {[s.value for s in VALID_TRANSITIONS.get(current, [])]}"
            )
        return target

    @staticmethod
    def can_transition(
        current: GoldenRecordStatus, target: GoldenRecordStatus
    ) -> bool:
        return target in VALID_TRANSITIONS.get(current, [])

    @staticmethod
    def is_ready(status: GoldenRecordStatus) -> bool:
        return status == GoldenRecordStatus.ACTIVE
