#!/usr/bin/env python3
"""
Transform raw_ingestions → silver_restaurants.
Reads all rows from raw_ingestions, extracts core fields from raw_data,
and upserts into silver_restaurants on conflict of place_id.
"""

import sys
from typing import Any

from repository.db import fetch_all, get_client


def fetch_raw_ingestions(supabase: Any) -> list[dict[str, Any]]:
    return fetch_all("raw_ingestions", "id, slug, place_id, ingested_at, raw_data")


def build_silver_row(raw: dict[str, Any]) -> dict[str, Any] | None:
    data = (raw.get("raw_data") or {}).get("data") or []
    place = data[0] if data else {}

    place_id = raw.get("place_id")
    if not place_id:
        return None

    return {
        "raw_ingestion_id": raw["id"],
        "slug": raw["slug"],
        "place_id": place_id,
        "name": place.get("name"),
        "website": place.get("website"),
        "phone": place.get("phone"),
        "rating": place.get("rating"),
        "review_count": int(place["reviews"]) if place.get("reviews") is not None else None,
        "ingested_at": raw.get("ingested_at"),
    }


def main() -> int:
    try:
        supabase = get_client()
    except RuntimeError as error:
        print(f"❌ {error}")
        return 1

    print("Fetching raw_ingestions...")
    raws = fetch_raw_ingestions(supabase)
    print(f"Found {len(raws)} rows")

    rows = [build_silver_row(r) for r in raws]
    rows = [r for r in rows if r is not None]

    upserted = 0
    errors = 0

    for row in rows:
        try:
            result = (
                supabase.table("silver_restaurants")
                .upsert(row, on_conflict="place_id")
                .execute()
            )
            if getattr(result, "error", None):
                raise RuntimeError(result.error.message)
            upserted += 1
        except Exception as error:
            errors += 1
            print(f"✗ {row.get('slug')} → {error}")

    print("---")
    print(f"Upserted: {upserted} | Errors: {errors}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
