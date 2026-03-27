"""
Run brand queries ("{restaurant name} berlin") for every target account with a place_id.
Stores raw SerpAPI JSON in bronze_serp with query_type='brand'.

Required env vars:
    NEXT_PUBLIC_SUPABASE_URL
    NEXT_PUBLIC_SUPABASE_ANON_KEY
    SERPAPI_KEY

Usage:
    python scripts/fetch_serp_brand.py
"""

import os
import time
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


def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

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


if __name__ == "__main__":
    main()
