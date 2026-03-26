"""
Match bronze_wolt venues to silver_restaurants and upsert into wolt_enrichments.
Zero API calls — reads only from Supabase tables.

Required env vars:
    NEXT_PUBLIC_SUPABASE_URL
    NEXT_PUBLIC_SUPABASE_ANON_KEY

Usage:
    python scripts/match_wolt.py
"""

import os
from difflib import SequenceMatcher
from supabase import create_client
from dotenv import load_dotenv

load_dotenv(".env.local")

SUPABASE_URL = os.environ["NEXT_PUBLIC_SUPABASE_URL"]
SUPABASE_KEY = os.environ["NEXT_PUBLIC_SUPABASE_ANON_KEY"]

BATCH_SIZE = 100


def load_all_bronze(supabase) -> list:
    all_rows = []
    page_size = 1000
    offset = 0
    while True:
        r = supabase.table("bronze_wolt").select("wolt_id, wolt_slug, wolt_name, raw_json").range(offset, offset + page_size - 1).execute()
        all_rows.extend(r.data)
        if len(r.data) < page_size:
            break
        offset += page_size
    return all_rows


def match_venue(
    wolt_slug: str,
    wolt_name: str,
    silver_by_slug: dict,
    silver_names: list,
) -> tuple[str | None, str | None]:
    # Tier 1: exact slug
    if wolt_slug in silver_by_slug:
        return silver_by_slug[wolt_slug], "exact_slug"

    # Tier 2: prefix slug (e.g. "wen-cheng-hand-pulled-noodles" → "wen-cheng")
    for slug, pid in silver_by_slug.items():
        if wolt_slug.startswith(slug):
            return pid, "prefix_slug"

    # Tier 3: name fuzzy match
    wname = wolt_name.lower() if wolt_name else ""
    best, best_pid = 0.0, None
    for sname, pid in silver_names:
        score = SequenceMatcher(None, wname, sname).ratio()
        if score > best:
            best, best_pid = score, pid
    if best > 0.7:
        return best_pid, f"name_fuzzy({best:.2f})"

    return None, None


def build_enrichment_record(place_id: str, venue: dict) -> dict:
    badges = venue.get("badges", []) or []
    badge_texts = [b.get("text", "") for b in badges if isinstance(b, dict)]
    is_exclusive = any("Wolt" in t for t in badge_texts)
    rating = venue.get("rating", {}) or {}

    return {
        "place_id": place_id,
        "wolt_id": venue.get("id"),
        "wolt_slug": venue.get("slug"),
        "wolt_name": venue.get("name"),
        "wolt_score": rating.get("score"),
        "wolt_rating_volume": rating.get("volume"),
        "wolt_price_range": venue.get("price_range"),
        "wolt_tags": venue.get("tags", []),
        "wolt_estimate_range": venue.get("estimate_range"),
        "wolt_delivers": venue.get("delivers", False),
        "wolt_online": venue.get("online", False),
        "wolt_exclusive": is_exclusive,
        "wolt_badge": badge_texts[0] if badge_texts else None,
        "wolt_preview_items": venue.get("venue_preview_items", []),
        "raw_response": venue,
    }


def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Load silver restaurants
    print("Loading silver_restaurants...")
    silver = supabase.table("silver_restaurants").select("place_id, name, slug").execute().data or []
    silver_by_slug = {r["slug"]: r["place_id"] for r in silver if r.get("slug")}
    silver_names = [(r["name"].lower(), r["place_id"]) for r in silver if r.get("name")]
    print(f"Loaded {len(silver_by_slug)} slugs, {len(silver_names)} names")

    # Load all bronze_wolt rows (no API call)
    print("Loading bronze_wolt...")
    bronze = load_all_bronze(supabase)
    print(f"Loaded {len(bronze):,} Wolt venues from bronze")

    # Match and upsert
    print("Matching and upserting...")
    matched, unmatched = 0, 0
    batch = []

    for row in bronze:
        venue = row["raw_json"]
        place_id, method = match_venue(
            row.get("wolt_slug") or "",
            row.get("wolt_name") or "",
            silver_by_slug,
            silver_names,
        )
        if place_id:
            batch.append(build_enrichment_record(place_id, venue))
            matched += 1
            if len(batch) >= BATCH_SIZE:
                supabase.table("wolt_enrichments").upsert(batch, on_conflict="wolt_id").execute()
                batch = []
        else:
            unmatched += 1

    if batch:
        supabase.table("wolt_enrichments").upsert(batch, on_conflict="wolt_id").execute()

    r = supabase.table("wolt_enrichments").select("wolt_id", count="exact").execute()
    print(f"\n✓ Matched and upserted:      {matched:,}")
    print(f"  Unmatched (not in silver): {unmatched:,}")
    print(f"  wolt_enrichments rows:     {r.count:,}")


if __name__ == "__main__":
    main()
