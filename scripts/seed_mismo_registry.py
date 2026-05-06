"""Seed the mismo_doc_type_registry table.

Idempotent: ON CONFLICT DO NOTHING on (source_system, external_type), so
re-running does not duplicate. Run after applying schema:

    USE_AWS_SECRETS=true python scripts/seed_mismo_registry.py

Designed to run as a one-off ECS task in production (same pattern as
scripts/apply_schema.py) since RDS is private.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.ingestion.mismo import (  # noqa: E402
    ENCOMPASS_TO_INTERNAL,
    MISMO_TO_INTERNAL,
)
from core.storage import db  # noqa: E402


async def seed() -> int:
    pool = await db.get_pool()
    inserted = 0
    skipped = 0
    async with pool.acquire() as conn:
        async with conn.transaction():
            # MISMO 3.4 standard types
            for external_type, internal_type in MISMO_TO_INTERNAL.items():
                result = await conn.execute(
                    """
                    INSERT INTO mismo_doc_type_registry
                        (source_system, external_type, internal_type, mismo_type)
                    VALUES ($1, $2, $3, $2)
                    ON CONFLICT (source_system, external_type) DO NOTHING
                    """,
                    "mismo_34",
                    external_type,
                    internal_type,
                )
                if result.endswith(" 1"):
                    inserted += 1
                else:
                    skipped += 1

            # Encompass label set
            for external_type, internal_type in ENCOMPASS_TO_INTERNAL.items():
                result = await conn.execute(
                    """
                    INSERT INTO mismo_doc_type_registry
                        (source_system, external_type, internal_type)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (source_system, external_type) DO NOTHING
                    """,
                    "encompass",
                    external_type,
                    internal_type,
                )
                if result.endswith(" 1"):
                    inserted += 1
                else:
                    skipped += 1

            # LOS connectors registry — names match get_connector() factory.
            for name, display_name in [
                ("encompass",    "ICE Encompass"),
                ("mismo_34",     "Generic MISMO 3.4"),
                ("byteprocloud", "Byte Pro Cloud"),
                ("openclose",    "OpenClose"),
                ("meridianlink", "MeridianLink"),
            ]:
                await conn.execute(
                    """
                    INSERT INTO los_connectors (name, display_name)
                    VALUES ($1, $2)
                    ON CONFLICT (name) DO NOTHING
                    """,
                    name,
                    display_name,
                )

    await db.close_pool()
    print(f"Done: {inserted} inserted, {skipped} already present")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(seed()))
