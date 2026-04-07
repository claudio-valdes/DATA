"""
Link new_discovery mentions to their place_id now that they exist in silver_restaurants.
Run this after transform_silver_restaurants.py has promoted new discoveries.

Usage:
    python offmenu/services/discovery/relink_new_discoveries.py

Required env vars:
    SUPABASE_URL  (or NEXT_PUBLIC_SUPABASE_URL)
    SUPABASE_KEY  (or SERVICE_ROLE_KEY)
"""

import os
import sys
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

load_dotenv(".env.local")

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.environ["NEXT_PUBLIC_SUPABASE_URL"]
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ["SERVICE_ROLE_KEY"]


def fetch_new_discoveries(supabase: Any) -> list[dict[str, Any]]:
    result = (
        supabase.table("silver_social_mentions")
        .select("id, restaurant_name")
        .eq("match_status", "new_discovery")
        .is_("place_id", "null")
        .execute()
    )
    return result.data or []


def fetch_name_to_place_id(supabase: Any) -> dict[str, str]:
    name_map: dict[str, str] = {}
    page_size = 1000
    offset = 0
    while True:
        result = (
            supabase.table("silver_restaurants")
            .select("place_id, name")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = result.data or []
        for row in batch:
            if row.get("name"):
                name_map[row["name"].lower().strip()] = row["place_id"]
        if len(batch) < page_size:
            break
        offset += page_size
    return name_map


def fetch_raw_ingestion_place_ids(supabase: Any) -> dict[str, str]:
    """Returns {slug: place_id} from raw_ingestions where source=social_discovery."""
    result = (
        supabase.table("raw_ingestions")
        .select("place_id, slug")
        .eq("source", "social_discovery")
        .execute()
    )
    return {r["slug"]: r["place_id"] for r in (result.data or []) if r.get("place_id")}


def main() -> int:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    mentions = fetch_new_discoveries(supabase)
    print(f"New discovery mentions to re-link: {len(mentions)}")

    if not mentions:
        print("Nothing to do.")
        return 0

    name_map = fetch_name_to_place_id(supabase)

    linked = 0
    still_missing = 0

    # Group by name to bulk-update
    groups: dict[str, list[str]] = {}
    for mention in mentions:
        key = (mention.get("restaurant_name") or "").lower().strip()
        groups.setdefault(key, []).append(mention["id"])

    for name_key, ids in groups.items():
        place_id = name_map.get(name_key)

        if place_id:
            supabase.table("silver_social_mentions").update({
                "place_id": place_id,
                "match_status": "matched",
            }).in_("id", ids).execute()
            linked += len(ids)
            print(f"  ✓ '{name_key}' → linked ({len(ids)} mentions)")
        else:
            still_missing += len(ids)
            print(f"  ? '{name_key}' → still not in silver_restaurants")

    print("---")
    print(f"Linked: {linked} | Still missing: {still_missing}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
