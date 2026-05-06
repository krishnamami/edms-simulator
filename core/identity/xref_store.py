"""In-memory xref store. Used as the default for local dev / unit tests.

Same interface as the Postgres-backed prod store, so callers can swap
implementations without touching call-sites.
"""
from typing import Optional


class XRefStore:
    def __init__(self):
        self._by_ssn_hash: dict = {}
        self._by_applicant_id: dict = {}
        self._by_source: dict = {}
        self._sequence: int = 0

    def save(self, gr) -> None:
        self._by_applicant_id[gr.applicant_id] = gr
        if gr.ssn_hash:
            self._by_ssn_hash[gr.ssn_hash] = gr
        for xref in gr.identity_xrefs:
            self._by_source[(xref.source_system, xref.source_id)] = gr.applicant_id

    def find_by_ssn_hash(self, ssn_hash: str):
        return self._by_ssn_hash.get(ssn_hash)

    def find_by_applicant_id(self, applicant_id: str):
        return self._by_applicant_id.get(applicant_id)

    def find_by_source_id(self, source_system: str, source_id: str):
        aid = self._by_source.get((source_system, source_id))
        return self._by_applicant_id.get(aid) if aid else None

    def find_by_name_dob(
        self, first_name: str, last_name: str, dob: str
    ) -> list:
        return [
            gr
            for gr in self._by_applicant_id.values()
            if gr.dob == dob and gr.last_name.lower() == last_name.lower()
        ]

    def next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    def all_records(self) -> list:
        return list(self._by_applicant_id.values())

    def reset(self) -> None:
        self._by_ssn_hash.clear()
        self._by_applicant_id.clear()
        self._by_source.clear()
        self._sequence = 0

    async def hydrate_from_postgres(self, postgres_store) -> tuple[int, int]:
        """Reload in-memory caches from Postgres at startup.

        Without this, every uvicorn restart leaves XRefStore empty even
        though Postgres still has every applicant. ``next_sequence``
        would issue ``APL-00001-P`` again and ``save_golden_record``'s
        ``ON CONFLICT (applicant_id) DO UPDATE`` would silently
        overwrite the existing applicant.

        Returns ``(loaded_count, max_sequence)``.
        """
        from core.identity.golden_record import GoldenRecord

        rows = await postgres_store.get_all_applicants()
        loaded = 0
        max_seq = self._sequence

        for row in rows:
            data = dict(row)
            # Postgres returns ``dob`` as a date; GoldenRecord wants str.
            dob = data.get("dob")
            if dob is not None and hasattr(dob, "isoformat"):
                data["dob"] = dob.isoformat()

            # Sequence number lives in the middle segment of the id,
            # e.g. ``APL-00042-P`` -> 42.
            try:
                seq = int(str(data.get("applicant_id", "")).split("-")[1])
                if seq > max_seq:
                    max_seq = seq
            except (IndexError, ValueError):
                pass

            try:
                # Pydantic ignores unknown fields by default; pass the
                # whole row and let it pick what it needs.
                gr = GoldenRecord(**{
                    k: v for k, v in data.items()
                    if k in GoldenRecord.model_fields
                })
                self.save(gr)
                loaded += 1
            except Exception:
                # Skip malformed rows rather than block startup.
                continue

        self._sequence = max_seq
        return loaded, max_seq
