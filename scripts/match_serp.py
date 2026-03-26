"""
Extract structured signals from bronze_serp into silver_search_visibility.

Required env vars:
    NEXT_PUBLIC_SUPABASE_URL
    NEXT_PUBLIC_SUPABASE_ANON_KEY

Usage:
    python scripts/match_serp.py
"""

import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(".env.local")

SUPABASE_URL = os.environ["NEXT_PUBLIC_SUPABASE_URL"]
SUPABASE_KEY = os.environ["NEXT_PUBLIC_SUPABASE_ANON_KEY"]

BATCH_SIZE = 100


def extract_signals(place_id: str, query: str, query_type: str, serp_json: dict, fetched_at: str) -> dict:
    signals = {
        "place_id": place_id,
        "query": query,
        "query_type": query_type,
        "organic_position": None,
        "in_local_pack": False,
        "local_pack_position": None,
        "has_knowledge_panel": False,
        "knowledge_panel_rating": None,
        "knowledge_panel_reviews": None,
        "total_results_estimate": None,
        "fetched_at": fetched_at,
    }

    restaurant_name = query.replace(" berlin", "").lower()

    for item in serp_json.get("organic_results", []):
        if restaurant_name in item.get("title", "").lower():
            signals["organic_position"] = item.get("position")
            break

    local_places = serp_json.get("local_results", {}).get("places", [])
    for place in local_places:
        if restaurant_name in place.get("title", "").lower():
            signals["in_local_pack"] = True
            signals["local_pack_position"] = place.get("position")
            break

    kp = serp_json.get("knowledge_graph", {})
    if kp:
        signals["has_knowledge_panel"] = True
        signals["knowledge_panel_rating"] = kp.get("rating")
        signals["knowledge_panel_reviews"] = kp.get("reviews")

    si = serp_json.get("search_information", {})
    signals["total_results_estimate"] = si.get("total_results")

    return signals


def load_all_bronze_serp(supabase) -> list:
    all_rows = []
    page_size = 1000
    offset = 0
    while True:
        r = supabase.table("bronze_serp") \
            .select("place_id, query, query_type, raw_json, fetched_at") \
            .not_.is_("place_id", "null") \
            .eq("query_type", "brand") \
            .range(offset, offset + page_size - 1) \
            .execute()
        all_rows.extend(r.data or [])
        if len(r.data or []) < page_size:
            break
        offset += page_size
    return all_rows


def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("Loading brand queries from bronze_serp...")
    rows = load_all_bronze_serp(supabase)
    print(f"Loaded {len(rows):,} brand SERP rows")

    print("Extracting signals and upserting...")
    batch = []
    processed = 0

    for row in rows:
        signals = extract_signals(
            place_id=row["place_id"],
            query=row["query"],
            query_type=row["query_type"],
            serp_json=row["raw_json"],
            fetched_at=row["fetched_at"],
        )
        batch.append(signals)
        processed += 1

        if len(batch) >= BATCH_SIZE:
            supabase.table("silver_search_visibility").upsert(
                batch, on_conflict="place_id,query,fetched_at::DATE"
            ).execute()
            batch = []

    if batch:
        supabase.table("silver_search_visibility").upsert(
            batch, on_conflict="place_id,query,fetched_at::DATE"
        ).execute()

    r = supabase.table("silver_search_visibility").select("id", count="exact").execute()
    print(f"\n✓ Signals extracted:                {processed:,}")
    print(f"  silver_search_visibility rows:    {r.count:,}")


if __name__ == "__main__":
    main()
