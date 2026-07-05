"""
Fetch all Berlin Wolt venues from the public API and upsert into bronze_wolt.
Run weekly or whenever fresh scores/data are needed. Never called during matching.
"""

import time
from datetime import datetime

import requests

from repository.db import get_client

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

BERLIN_COORDS = [
    (52.5200, 13.4050),  # Mitte
    (52.5462, 13.4135),  # Prenzlauer Berg
    (52.4800, 13.4050),  # Neukölln
    (52.5100, 13.3200),  # Charlottenburg
    (52.4900, 13.4800),  # Treptow
]

BATCH_SIZE = 100


def fetch_wolt_venues(lat: float, lon: float) -> list[dict]:
    url = f"https://restaurant-api.wolt.com/v1/pages/restaurants?lat={lat}&lon={lon}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    venues = []
    for section in data.get("sections", []):
        if section.get("template") == "venue-vertical-list":
            for item in section.get("items", []):
                v = item.get("venue")
                if v:
                    venues.append(v)
    return venues


def fetch_bronze() -> dict:
    """Fetch all Berlin Wolt venues and upsert into bronze_wolt. Returns summary counts."""
    supabase = get_client()

    print("Fetching Wolt venues...")
    all_venues: dict[str, dict] = {}
    for lat, lon in BERLIN_COORDS:
        try:
            venues = fetch_wolt_venues(lat, lon)
            print(f"  ({lat}, {lon}): {len(venues)} venues")
            for v in venues:
                all_venues[v["id"]] = v
        except Exception as e:
            print(f"  ERROR fetching ({lat}, {lon}): {e}")
        time.sleep(2)

    print(f"Total unique venues: {len(all_venues)}")

    print("Upserting to bronze_wolt...")
    now = datetime.utcnow().isoformat()
    batch = []
    upserted = 0

    for venue in all_venues.values():
        batch.append({
            "wolt_id": venue["id"],
            "wolt_slug": venue.get("slug"),
            "wolt_name": venue.get("name"),
            "raw_json": venue,
            "updated_at": now,
        })
        if len(batch) >= BATCH_SIZE:
            supabase.table("bronze_wolt").upsert(batch, on_conflict="wolt_id").execute()
            upserted += len(batch)
            print(f"  Upserted {upserted}/{len(all_venues)}...")
            batch = []

    if batch:
        supabase.table("bronze_wolt").upsert(batch, on_conflict="wolt_id").execute()
        upserted += len(batch)

    r = supabase.table("bronze_wolt").select("wolt_id", count="exact").execute()
    print(f"\n✓ bronze_wolt total rows: {r.count:,}")
    return {"fetched": len(all_venues), "upserted": upserted, "bronze_wolt_rows": r.count}
