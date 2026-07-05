"""
Extract structured signals from bronze_serp into silver_search_visibility.
"""

from datetime import datetime

from repository.db import get_client, paginate

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
        "fetched_date": datetime.fromisoformat(fetched_at.replace("Z", "+00:00")).date().isoformat(),
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


def load_all_bronze_serp(supabase, place_id: str | None = None) -> list:
    def build(sb):
        query = (
            sb.table("bronze_serp")
            .select("place_id, query, query_type, raw_json, fetched_at")
            .not_.is_("place_id", "null")
            .eq("query_type", "brand")
        )
        if place_id:
            query = query.eq("place_id", place_id)
        return query

    return paginate(build)


def match_signals(place_id: str | None = None) -> dict:
    """Extract structured signals from brand bronze_serp rows and upsert silver_search_visibility.

    Pass place_id to scope to a single restaurant (avoids reprocessing every brand
    row on a single-restaurant call). Returns summary counts.
    """
    supabase = get_client()

    print("Loading brand queries from bronze_serp...")
    rows = load_all_bronze_serp(supabase, place_id=place_id)
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
                batch, on_conflict="place_id,query,fetched_date"
            ).execute()
            batch = []

    if batch:
        supabase.table("silver_search_visibility").upsert(
            batch, on_conflict="place_id,query,fetched_date"
        ).execute()

    r = supabase.table("silver_search_visibility").select("id", count="exact").execute()
    print(f"\n✓ Signals extracted:                {processed:,}")
    print(f"  silver_search_visibility rows:    {r.count:,}")
    return {"signals_extracted": processed, "silver_search_visibility_rows": r.count}
