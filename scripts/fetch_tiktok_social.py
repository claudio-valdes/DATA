"""
Fetch TikTok profile + posts for a restaurant via Apify.

Usage:
    python scripts/fetch_tiktok_social.py <restaurant_id>

Required env vars:
    APIFY_TOKEN
    NEXT_PUBLIC_SUPABASE_URL
    NEXT_PUBLIC_SUPABASE_ANON_KEY
"""

import os
import sys
import time
from datetime import date, datetime, timezone
from dotenv import load_dotenv
import httpx
from supabase import create_client

load_dotenv(".env.local")

SUPABASE_URL = os.environ["NEXT_PUBLIC_SUPABASE_URL"]
SUPABASE_KEY = os.environ["NEXT_PUBLIC_SUPABASE_ANON_KEY"]
APIFY_TOKEN = os.environ["APIFY_TOKEN"]


def run_apify_actor(actor_id: str, input_payload: dict) -> list:
    url = f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items?token={APIFY_TOKEN}"
    response = httpx.post(url, json=input_payload, timeout=300)
    response.raise_for_status()
    return response.json()


def get_tiktok_handle(supabase, restaurant_id: str) -> str | None:
    r = supabase.table("restaurant_social_handles") \
        .select("tiktok_handle") \
        .eq("restaurant_id", restaurant_id) \
        .single() \
        .execute()
    if not r.data or not r.data.get("tiktok_handle"):
        return None
    return r.data["tiktok_handle"]


def main():
    if len(sys.argv) < 2:
        print("Usage: python fetch_tiktok_social.py <restaurant_id>")
        sys.exit(1)

    restaurant_id = sys.argv[1]
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    tiktok_handle = get_tiktok_handle(supabase, restaurant_id)
    if not tiktok_handle:
        print(f"No TikTok handle found for restaurant_id={restaurant_id}. Exiting.")
        sys.exit(0)

    print(f"Fetching TikTok profile for @{tiktok_handle}...")
    try:
        items = run_apify_actor("clockworks~tiktok-profile-scraper", {
            "profiles": [tiktok_handle],
            "resultsLimit": 50,
        })
    except Exception as e:
        print(f"ERROR fetching TikTok data: {e}")
        sys.exit(1)

    if not items:
        print("No data returned from Apify.")
        sys.exit(0)

    # Profile stats live inside authorMeta on each video item
    author_meta = items[0].get("authorMeta", {}) or {}
    now = datetime.now(timezone.utc).isoformat()

    supabase.table("bronze_tiktok_profiles").upsert({
        "restaurant_id": restaurant_id,
        "tiktok_handle": tiktok_handle,
        "fans": author_meta.get("fans"),
        "following": author_meta.get("following"),
        "verified": author_meta.get("verified", False),
        "heart_total": author_meta.get("heart"),
        "video_count": author_meta.get("video"),
        "signature": author_meta.get("signature"),
        "bio_link": author_meta.get("bioLink", {}).get("link") if isinstance(author_meta.get("bioLink"), dict) else None,
        "snapshot_date": date.today().isoformat(),
        "fetched_at": now,
    }, on_conflict="restaurant_id,snapshot_date").execute()
    print(f"  Profile upserted — {author_meta.get('fans', 0):,} fans")

    # Upsert posts
    post_records = []
    for item in items:
        post_id = item.get("id")
        if not post_id:
            continue

        video_meta = item.get("videoMeta", {}) or {}
        location = item.get("locationMeta", {}) or {}
        hashtags = [h.get("name") for h in item.get("hashtags", []) if h.get("name")]

        post_records.append({
            "post_id": post_id,
            "restaurant_id": restaurant_id,
            "tiktok_handle": tiktok_handle,
            "text": item.get("text"),
            "hashtags": hashtags,
            "location_name": location.get("locationName"),
            "location_city": location.get("city"),
            "location_address": location.get("address"),
            "likes": item.get("diggCount"),
            "shares": item.get("shareCount"),
            "plays": item.get("playCount"),
            "collects": item.get("collectCount"),
            "comments_count": item.get("commentCount"),
            "is_sponsored": item.get("isAd", False),
            "is_slideshow": item.get("imagePost", False),
            "has_transcription": bool(video_meta.get("subtitleLinks")),
            "web_video_url": item.get("webVideoUrl"),
            "created_at": datetime.fromtimestamp(item["createTime"], tz=timezone.utc).isoformat() if item.get("createTime") else None,
            "fetched_at": now,
        })

    if post_records:
        supabase.table("bronze_tiktok_posts").upsert(
            post_records, on_conflict="post_id"
        ).execute()

    print(f"  Posts upserted: {len(post_records)}")
    print(f"\n✓ TikTok data fetched for @{tiktok_handle}")


if __name__ == "__main__":
    main()
