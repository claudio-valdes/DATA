"""
Maintains restaurant_pipeline.query_appearances — a count of distinct search
queries each restaurant has surfaced in, used by promote()'s default criteria.
Only geo/social discovery populates bronze_search_rankings, so this only needs
to run after discover(), not after target()/refresh().
"""

from collections import Counter

from repository.db import get_client, paginate


def refresh_query_appearances() -> dict:
    """Recompute COUNT(DISTINCT query) per place_id from bronze_search_rankings
    and write it onto restaurant_pipeline.query_appearances for every restaurant
    currently in the pipeline. Returns summary counts.
    """
    supabase = get_client()

    rows = paginate(lambda sb: sb.table("bronze_search_rankings").select("place_id, query"))
    appearances: dict[str, set[str]] = {}
    for row in rows:
        place_id = row.get("place_id")
        query = row.get("query")
        if place_id and query:
            appearances.setdefault(place_id, set()).add(query)

    counts = Counter({place_id: len(queries) for place_id, queries in appearances.items()})

    pipeline_rows = paginate(lambda sb: sb.table("restaurant_pipeline").select("place_id"))
    updated = 0
    for row in pipeline_rows:
        place_id = row["place_id"]
        count = counts.get(place_id, 0)
        supabase.table("restaurant_pipeline").update({"query_appearances": count}).eq("place_id", place_id).execute()
        updated += 1

    print(f"✓ query_appearances refreshed for {updated} restaurants")
    return {"updated": updated}
