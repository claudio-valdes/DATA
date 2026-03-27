"""
Run discovery + category queries against SerpAPI, store raw JSON in bronze_serp,
and auto-add top 10 results to target_accounts.

Required env vars:
    NEXT_PUBLIC_SUPABASE_URL
    NEXT_PUBLIC_SUPABASE_ANON_KEY
    SERPAPI_KEY

Usage:
    python scripts/fetch_serp_discovery.py
"""

import os
import time
from collections import Counter
from dotenv import load_dotenv
import requests
from supabase import create_client

load_dotenv(".env.local")

SUPABASE_URL = os.environ["NEXT_PUBLIC_SUPABASE_URL"]
SUPABASE_KEY = os.environ["NEXT_PUBLIC_SUPABASE_ANON_KEY"]
SERPAPI_KEY = os.environ["SERPAPI_KEY"]


def fetch_serp(query: str) -> dict:
    params = {
        "q": query,
        "location": "Berlin, Germany",
        "hl": "en",
        "gl": "de",
        "num": 100,
        "api_key": SERPAPI_KEY,
    }
    resp = requests.get("https://serpapi.com/search", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def build_discovery_queries(supabase) -> list[dict]:
    queries = []

    fixed = [
        "best restaurants berlin",
        "where to eat berlin",
        "food spots berlin",
        "hidden gems berlin",
        "best brunch berlin",
        "best breakfast in berlin",
    ]
    for q in fixed:
        queries.append({"query": q, "query_type": "discovery"})

    r = supabase.table("silver_labels") \
        .select("label_value") \
        .eq("label_type", "cuisine") \
        .execute()

    cuisine_counts = Counter(row["label_value"] for row in (r.data or []))
    for cuisine, _ in cuisine_counts.most_common():
        q = f"best {cuisine.lower()} in berlin"
        queries.append({"query": q, "query_type": "category"})

    return queries


def auto_add_from_local_pack(supabase, serp_json: dict, query: str, query_type: str):
    """Only add actual restaurants from the Maps local pack."""
    places = serp_json.get("local_results", {}).get("places", [])
    for place in places:
        name = place.get("title", "").strip()
        google_place_id = str(place.get("place_id", ""))
        if not name:
            continue
        try:
            supabase.table("target_accounts").upsert({
                "name": name,
                "google_place_id": google_place_id,
                "added_by": query_type,
                "source_query": query,
                "source_position": place.get("position"),
            }, on_conflict="name").execute()
        except Exception as e:
            print(f"  Could not add {name}: {e}")


def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    queries = build_discovery_queries(supabase)
    print(f"Total queries to run: {len(queries)}")

    for i, q in enumerate(queries):
        print(f"  [{i+1}/{len(queries)}] {q['query']}")
        try:
            result = fetch_serp(q["query"])

            supabase.table("bronze_serp").upsert({
                "query": q["query"],
                "query_type": q["query_type"],
                "place_id": None,
                "raw_json": result,
            }, on_conflict="query").execute()

            auto_add_from_local_pack(supabase, result, q["query"], q["query_type"])

            time.sleep(1)
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

    r = supabase.table("target_accounts").select("id", count="exact").execute()
    print(f"\n✓ bronze_serp queries stored: {len(queries)}")
    print(f"✓ target_accounts total:      {r.count}")


if __name__ == "__main__":
    main()
