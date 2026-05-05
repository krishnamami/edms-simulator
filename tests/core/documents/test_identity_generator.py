"""Driver's license image generator — verify well-formed JPG + metadata."""
import io
from datetime import date

from PIL import Image

from core.documents.generators.identity_generator import generate_drivers_license


def test_drivers_license_produces_valid_jpg():
    jpg, meta = generate_drivers_license(
        state="CA",
        full_name="James Okafor",
        dob=date(1982, 7, 14),
        address="100 Main St\nSan Francisco, CA 94105",
        dl_number="D1234567",
        expiry=date(2028, 5, 1),
    )

    img = Image.open(io.BytesIO(jpg))
    assert img.format == "JPEG"
    assert img.size == (1010, 638)

    assert meta["document_type"] == "DRIVERS_LICENSE"
    assert meta["state"] == "CA"
    assert meta["full_name"] == "James Okafor"
    assert meta["dob"] == "1982-07-14"
    assert meta["dl_number"] == "D1234567"
    assert meta["image_format"] == "JPEG"
