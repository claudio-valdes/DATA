"""
Run brand queries ("{restaurant name} berlin") for every target account with a place_id.
Stores raw SerpAPI JSON in bronze_serp with query_type='brand'.
"""

import os
import time

import requests

from repository.db import get_client

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


def run_brand_query_for_restaurant(place_id: str, name: str) -> dict:
    """Run a single brand SERP query for one restaurant, bypassing target_accounts entirely."""
    supabase = get_client()
    query = f"{name} berlin"
    result = fetch_serp(query)
    supabase.table("bronze_serp").upsert({
        "query": query,
        "query_type": "brand",
        "place_id": place_id,
        "raw_json": result,
    }, on_conflict="query,fetched_at::DATE").execute()
    return {"query": query}


def run_brand_queries() -> dict:
    """Run a brand SERP query for every target account with a place_id. Returns summary counts."""
    supabase = get_client()

    r = supabase.table("target_accounts") \
        .select("place_id, name") \
        .not_.is_("place_id", "null") \
        .execute()

    accounts = r.data or []
    print(f"Running brand queries for {len(accounts)} target accounts...")

    for i, account in enumerate(accounts):
        query = f"{account['name']} berlin"
        print(f"  [{i+1}/{len(accounts)}] {query}")
        try:
            result = fetch_serp(query)
            supabase.table("bronze_serp").upsert({
                "query": query,
                "query_type": "brand",
                "place_id": account["place_id"],
                "raw_json": result,
            }, on_conflict="query,fetched_at::DATE").execute()
            time.sleep(1)
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

    print(f"\n✓ Brand queries complete: {len(accounts)}")
    return {"accounts_queried": len(accounts)}
