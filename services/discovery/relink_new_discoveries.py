"""
Link new_discovery mentions to their place_id now that they exist in silver_restaurants.
Run this after transform_silver_restaurants.py has promoted new discoveries.

Usage:
    python services/discovery/relink_new_discoveries.py

Required env vars:
    SUPABASE_URL  (or NEXT_PUBLIC_SUPABASE_URL)
    SUPABASE_KEY  (or SERVICE_ROLE_KEY)
"""

import sys
from typing import Any

from repository.db import get_client, paginate


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
    rows = paginate(lambda sb: sb.table("silver_restaurants").select("place_id, name"))
    name_map: dict[str, str] = {}
    for row in rows:
        if row.get("name"):
            name_map[row["name"].lower().strip()] = row["place_id"]
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
    supabase = get_client()

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
