"""Round-trip: bank statement generator + pymupdf extractor."""
from datetime import date

from core.documents.extractors.pymupdf_extractor import extract_bank_statement
from core.documents.generators.bank_stmt_generator import generate_bank_statement


def test_bank_statement_round_trip():
    pdf_bytes, meta = generate_bank_statement(
        bank_name="Pacific First Bank",
        account_holder="James Okafor",
        account_number="9876543210",
        statement_end_date=date(2024, 3, 31),
        starting_balance=10_000.00,
        seed=11,
    )
    assert pdf_bytes.startswith(b"%PDF")
    assert len(meta["months"]) == 3
    for m in meta["months"]:
        assert 20 <= len(m["transactions"]) <= 30

    fields, confidence = extract_bank_statement(pdf_bytes)

    assert confidence >= 0.80, f"low confidence: {confidence}, fields={fields}"
    assert fields["bank_name"] == "Pacific First Bank"
    assert fields["account_number_masked"] == "****3210"
    assert fields["account_holder"] == "James Okafor"
    assert fields["months_count"] == 3
    # ending_balance is the last month's closing
    assert abs(fields["ending_balance"] - meta["ending_balance"]) < 0.01


def test_bank_statement_seeded_is_deterministic():
    """Same seed -> same transaction stream + balances. PDF bytes differ
    because reportlab stamps a CreationDate; we assert content parity."""
    _, meta_a = generate_bank_statement(
        bank_name="X", account_holder="Y", account_number="1234",
        statement_end_date=date(2024, 3, 31), starting_balance=5000, seed=7,
    )
    _, meta_b = generate_bank_statement(
        bank_name="X", account_holder="Y", account_number="1234",
        statement_end_date=date(2024, 3, 31), starting_balance=5000, seed=7,
    )
    assert meta_a == meta_b
