"""CSV adapter tests — header flexibility, error collection, perf."""
import time

from core.ingestion.adapters import csv_adapter


def test_processes_clean_csv_with_canonical_headers():
    csv = (
        "first_name,last_name,annual_income,email\n"
        "James,Okafor,92400,james@example.com\n"
        "Sarah,Okafor,56200,sarah@example.com\n"
    ).encode()
    events, report = csv_adapter.adapt(csv)
    assert report["processed"] == 2
    assert report["failed"] == 0
    assert events[0].applicant_signals["first_name"] == "James"
    assert events[0].extracted_fields["annual_income"] == 92400.0


def test_flexible_header_aliases():
    csv = (
        "First Name,LName,INCOME,Email Address\n"
        "James,Okafor,92400,james@example.com\n"
    ).encode()
    events, report = csv_adapter.adapt(csv)
    assert report["processed"] == 1
    assert events[0].applicant_signals["first_name"] == "James"
    assert events[0].applicant_signals["last_name"] == "Okafor"
    assert events[0].extracted_fields["annual_income"] == 92400.0


def test_collects_errors_without_stopping():
    csv = (
        "first_name,last_name,annual_income\n"
        "James,Okafor,92400\n"            # ok
        ",Okafor,55000\n"                  # missing first_name
        "Sarah,,56200\n"                   # missing last_name
        "Pat,Adams,not_a_number\n"         # bad income
        "Casey,Brown,72000\n"              # ok
    ).encode()
    events, report = csv_adapter.adapt(csv)
    assert report["processed"] == 2
    assert report["failed"] == 3
    assert {e["row"] for e in report["errors"]} == {3, 4, 5}


def test_handles_dollar_signs_and_commas_in_income():
    csv = (
        "first_name,last_name,annual_income\n"
        "James,Okafor,\"$92,400\"\n"
    ).encode()
    events, report = csv_adapter.adapt(csv)
    assert report["processed"] == 1
    assert events[0].extracted_fields["annual_income"] == 92400.0


def test_processes_100_rows_under_two_seconds():
    rows = ["first_name,last_name,annual_income"]
    for i in range(100):
        rows.append(f"User{i},Test{i},{50000 + i}")
    csv = ("\n".join(rows) + "\n").encode()

    start = time.perf_counter()
    events, report = csv_adapter.adapt(csv)
    elapsed = time.perf_counter() - start

    assert report["processed"] == 100
    assert elapsed < 2.0
