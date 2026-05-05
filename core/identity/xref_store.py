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
