#!/usr/bin/env python3
"""
Transform raw_ingestions → silver_restaurants.
Reads all rows from raw_ingestions, extracts core fields from raw_data,
and upserts into silver_restaurants on conflict of place_id.
"""

import os
import sys
from typing import Any

from dotenv import load_dotenv
from supabase import create_client


def load_environment() -> tuple[str, str]:
    load_dotenv(".env.local")

    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

    missing = []
    if not supabase_url:
        missing.append("SUPABASE_URL")
    if not supabase_key:
        missing.append("SUPABASE_KEY")

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return supabase_url, supabase_key


def fetch_raw_ingestions(supabase: Any) -> list[dict[str, Any]]:
    rows = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            supabase.table("raw_ingestions")
            .select("id, slug, place_id, ingested_at, raw_data")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = result.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    return rows


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
        supabase_url, supabase_key = load_environment()
    except RuntimeError as error:
        print(f"❌ {error}")
        return 1

    supabase = create_client(supabase_url, supabase_key)

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
