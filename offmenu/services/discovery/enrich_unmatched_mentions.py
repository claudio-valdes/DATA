"""
Enrich unmatched silver_social_mentions by querying OutScraper Google Maps.

For each unique unmatched restaurant name:
  1. Search Google Maps via OutScraper
  2. Validate the result is actually a restaurant
  3a. place_id already in silver_restaurants → link mention
  3b. place_id not in silver_restaurants → insert to raw_ingestions for normal pipeline
  3c. Not a restaurant or not found → mark accordingly

Usage:
    python offmenu/services/discovery/enrich_unmatched_mentions.py

Required env vars:
    OUTSCRAPER_API_KEY
    SUPABASE_URL  (or NEXT_PUBLIC_SUPABASE_URL)
    SUPABASE_KEY  (or SERVICE_ROLE_KEY)
"""

import os
import re
import sys
import time
from typing import Any

from dotenv import load_dotenv
from outscraper import ApiClient
from supabase import create_client

load_dotenv(".env.local")

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.environ["NEXT_PUBLIC_SUPABASE_URL"]
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ["SERVICE_ROLE_KEY"]
OUTSCRAPER_KEY = os.environ["OUTSCRAPER_API_KEY"]

RESTAURANT_TYPES = {
    "restaurant", "food", "cafe", "bakery", "bar", "meal_takeaway",
    "meal_delivery", "coffee", "ice_cream", "pizza", "sushi", "ramen",
    "bistro", "brasserie", "diner", "eatery", "gastropub",
}

NON_RESTAURANT_TYPES = {
    "night_club", "bar_nightclub", "nightclub", "club", "concert_hall",
    "music_venue", "museum", "art_gallery", "hotel", "lodging",
    "tourist_attraction", "store", "shopping_mall", "gym", "spa",
}

SEARCH_FIELDS = ["name", "place_id", "type", "subtypes", "address", "city", "rating", "reviews", "website", "phone"]


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def fetch_unmatched_mentions(supabase: Any) -> list[dict[str, Any]]:
    result = (
        supabase.table("silver_social_mentions")
        .select("id, restaurant_name, neighbourhood, cuisine")
        .eq("match_status", "needs_review")
        .eq("is_restaurant_mention", True)
        .not_.is_("restaurant_name", "null")
        .execute()
    )
    return result.data or []


def fetch_known_place_ids(supabase: Any) -> set[str]:
    known: set[str] = set()
    page_size = 1000
    offset = 0
    while True:
        result = (
            supabase.table("silver_restaurants")
            .select("place_id")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = result.data or []
        known.update(r["place_id"] for r in batch if r.get("place_id"))
        if len(batch) < page_size:
            break
        offset += page_size
    return known


def group_by_name(mentions: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for mention in mentions:
        key = (mention.get("restaurant_name") or "").lower().strip()
        groups.setdefault(key, []).append(mention)
    return groups


def build_query(mention: dict[str, Any]) -> str:
    name = mention.get("restaurant_name", "")
    neighbourhood = mention.get("neighbourhood")
    if neighbourhood:
        return f"{name} {neighbourhood} Berlin"
    return f"{name} Berlin"


def is_restaurant(result: dict[str, Any]) -> bool:
    type_str = (result.get("type") or "").lower()
    subtypes = [s.lower() for s in (result.get("subtypes") or [])]
    all_types = {type_str} | set(subtypes)

    if all_types & NON_RESTAURANT_TYPES:
        return False
    if all_types & RESTAURANT_TYPES:
        return True
    # fallback: if type contains "restaurant" or "food" anywhere
    combined = " ".join(all_types)
    return "restaurant" in combined or "food" in combined or "cafe" in combined


def search_google(client: ApiClient, query: str) -> dict[str, Any] | None:
    try:
        results = client.google_maps_search(
            query,
            limit=1,
            language="en",
            region="DE",
            fields=SEARCH_FIELDS,
        )
        flat = []
        for item in results or []:
            if isinstance(item, list):
                flat.extend(i for i in item if isinstance(i, dict))
            elif isinstance(item, dict):
                flat.append(item)
        return flat[0] if flat else None
    except Exception as e:
        print(f"  ⚠ OutScraper error for '{query}': {e}")
        return None


def insert_raw_ingestion(supabase: Any, result: dict[str, Any], query: str) -> None:
    place_id = result["place_id"]
    supabase.table("raw_ingestions").upsert({
        "source": "social_discovery",
        "query": query,
        "slug": slugify(result.get("name") or place_id),
        "place_id": place_id,
        "raw_data": {"data": [result]},
    }, on_conflict="place_id").execute()


def update_mentions_bulk(
    supabase: Any,
    mention_ids: list[str],
    place_id: str | None,
    method: str | None,
    status: str,
) -> None:
    supabase.table("silver_social_mentions").update({
        "place_id": place_id,
        "match_method": method,
        "match_status": status,
    }).in_("id", mention_ids).execute()


def main() -> int:
    client = ApiClient(api_key=OUTSCRAPER_KEY)
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    mentions = fetch_unmatched_mentions(supabase)
    print(f"Unmatched mentions to enrich: {len(mentions)}")

    if not mentions:
        print("Nothing to do.")
        return 0

    known_place_ids = fetch_known_place_ids(supabase)
    groups = group_by_name(mentions)
    print(f"Unique names to search: {len(groups)}")

    linked = 0
    new_discoveries = 0
    not_restaurant = 0
    unresolved = 0

    for index, (name_key, group) in enumerate(groups.items()):
        all_ids = [m["id"] for m in group]
        representative = group[0]
        query = build_query(representative)

        result = search_google(client, query)

        if not result or not result.get("place_id"):
            update_mentions_bulk(supabase, all_ids, None, None, "unresolved")
            unresolved += len(all_ids)
            print(f"  - '{representative.get('restaurant_name')}' → not found on Google")

        elif not is_restaurant(result):
            update_mentions_bulk(supabase, all_ids, None, None, "not_a_restaurant")
            not_restaurant += len(all_ids)
            print(f"  ✗ '{representative.get('restaurant_name')}' → not a restaurant ({result.get('type')})")

        elif result["place_id"] in known_place_ids:
            place_id = result["place_id"]
            update_mentions_bulk(supabase, all_ids, place_id, "google+existing", "matched")
            linked += len(all_ids)
            print(f"  ✓ '{representative.get('restaurant_name')}' → linked to existing restaurant")

        else:
            place_id = result["place_id"]
            insert_raw_ingestion(supabase, result, query)
            # place_id not in silver_restaurants yet — leave null, link after transform runs
            update_mentions_bulk(supabase, all_ids, None, "google+new", "new_discovery")
            new_discoveries += len(all_ids)
            print(f"  ★ '{representative.get('restaurant_name')}' → new discovery, added to pipeline")

        if index < len(groups) - 1:
            time.sleep(1)

    print("---")
    print(f"Linked to existing: {linked}")
    print(f"New discoveries added to pipeline: {new_discoveries}")
    print(f"Not a restaurant: {not_restaurant}")
    print(f"Unresolved (not found): {unresolved}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
