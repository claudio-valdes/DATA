"""
Scrape TikTok and Instagram for Berlin restaurant discovery content.
Results land in bronze_social_posts for agent processing.

Usage:
    python services/discovery/scrape_social_discovery.py --platform tiktok
    python services/discovery/scrape_social_discovery.py --platform instagram
    python services/discovery/scrape_social_discovery.py --platform both

Required env vars:
    APIFY_TOKEN
    SUPABASE_URL  (or NEXT_PUBLIC_SUPABASE_URL)
    SUPABASE_KEY  (or SERVICE_ROLE_KEY)
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

import httpx

from repository.db import get_client

APIFY_TOKEN = os.environ["APIFY_TOKEN"]

TIKTOK_HASHTAGS = [
    "berlinfood",
    "berlinrestaurant",
    "berlineats",
    "foodberlin",
    "berlinfoodies",
    "bestrestaurantsberlin",
    "berlinhiddengems",
    "prenzlauerberg",
    "kreuzberg",
    "neukoelln",
    "friedrichshain",
    "berlinmitte",
]

TIKTOK_KEYWORDS = [
    "best restaurants berlin",
    "hidden gems berlin food",
    "berlin food review",
    "best breakfast berlin",
    "best brunch berlin",
    "best coffee berlin",
    "best ramen berlin",
    "best pizza berlin",
    "trendy restaurants berlin",
    "romantic dinner berlin",
]

INSTAGRAM_HASHTAGS = [
    "berlinfood",
    "berlinrestaurant",
    "berlineats",
    "foodberlin",
    "berlinfoodies",
    "chinesefoodberlin",
    "berlinasianfood",
    "bestrestaurantsberlin",
    "berlinhiddengems",
    "brunchberlin",
    "breakfastberlin",
]

RESULTS_PER_QUERY = 30


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape TikTok and IG for Berlin restaurant discovery")
    parser.add_argument(
        "--platform",
        choices=["tiktok", "instagram", "both"],
        required=True,
        help="Platform to scrape",
    )
    return parser.parse_args()


def run_apify_actor(actor_id: str, input_payload: dict) -> list:
    url = f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items?token={APIFY_TOKEN}"
    response = httpx.post(url, json=input_payload, timeout=300)
    response.raise_for_status()
    return response.json()


def upsert_posts(supabase, records: list[dict]) -> tuple[int, int]:
    upserted = 0
    errors = 0
    for record in records:
        try:
            supabase.table("bronze_social_posts").upsert(
                record, on_conflict="platform,post_id"
            ).execute()
            upserted += 1
        except Exception as e:
            errors += 1
            print(f"  ✗ upsert failed for {record.get('post_id')}: {e}")
    return upserted, errors


def scrape_tiktok_hashtag(supabase, hashtag: str) -> tuple[int, int]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    try:
        items = run_apify_actor("clockworks~tiktok-scraper", {
            "hashtags": [hashtag],
            "resultsPerPage": RESULTS_PER_QUERY,
        })
    except Exception as e:
        print(f"  ✗ #{hashtag} → {e}")
        return 0, 1

    records = []
    for item in items:
        post_id = item.get("id")
        if not post_id:
            continue
        location = item.get("locationMeta") or {}
        records.append({
            "platform": "tiktok",
            "post_id": str(post_id),
            "query": f"#{hashtag}",
            "author_handle": (item.get("authorMeta") or {}).get("name"),
            "caption": item.get("text"),
            "hashtags": [h.get("name") for h in item.get("hashtags", []) if h.get("name")],
            "likes": item.get("diggCount"),
            "comments_count": item.get("commentCount"),
            "shares": item.get("shareCount"),
            "plays": item.get("playCount"),
            "location_name": location.get("locationName"),
            "posted_at": datetime.fromtimestamp(item["createTime"], tz=timezone.utc).isoformat() if item.get("createTime") else None,
            "fetched_at": fetched_at,
        })

    upserted, errors = upsert_posts(supabase, records)
    print(f"  #{hashtag} → {len(items)} fetched, {upserted} upserted")
    return upserted, errors


def scrape_tiktok_keyword(supabase, keyword: str) -> tuple[int, int]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    try:
        items = run_apify_actor("clockworks~tiktok-scraper", {
            "searchQueries": [keyword],
            "resultsPerPage": RESULTS_PER_QUERY,
        })
    except Exception as e:
        print(f"  ✗ '{keyword}' → {e}")
        return 0, 1

    records = []
    for item in items:
        post_id = item.get("id")
        if not post_id:
            continue
        location = item.get("locationMeta") or {}
        records.append({
            "platform": "tiktok",
            "post_id": str(post_id),
            "query": keyword,
            "author_handle": (item.get("authorMeta") or {}).get("name"),
            "caption": item.get("text"),
            "hashtags": [h.get("name") for h in item.get("hashtags", []) if h.get("name")],
            "likes": item.get("diggCount"),
            "comments_count": item.get("commentCount"),
            "shares": item.get("shareCount"),
            "plays": item.get("playCount"),
            "location_name": location.get("locationName"),
            "posted_at": datetime.fromtimestamp(item["createTime"], tz=timezone.utc).isoformat() if item.get("createTime") else None,
            "fetched_at": fetched_at,
        })

    upserted, errors = upsert_posts(supabase, records)
    print(f"  '{keyword}' → {len(items)} fetched, {upserted} upserted")
    return upserted, errors


def scrape_instagram_hashtag(supabase, hashtag: str) -> tuple[int, int]:
    fetched_at = datetime.now(timezone.utc).isoformat()
    try:
        items = run_apify_actor("apify~instagram-hashtag-scraper", {
            "hashtags": [hashtag],
            "resultsLimit": RESULTS_PER_QUERY,
        })
    except Exception as e:
        print(f"  ✗ #{hashtag} → {e}")
        return 0, 1

    records = []
    for item in items:
        post_id = item.get("id") or item.get("shortCode")
        if not post_id:
            continue
        records.append({
            "platform": "instagram",
            "post_id": str(post_id),
            "query": f"#{hashtag}",
            "author_handle": item.get("ownerUsername"),
            "caption": item.get("caption"),
            "hashtags": item.get("hashtags", []),
            "likes": item.get("likesCount"),
            "comments_count": item.get("commentsCount"),
            "shares": None,
            "plays": item.get("videoViewCount"),
            "location_name": item.get("locationName"),
            "posted_at": item.get("timestamp"),
            "fetched_at": fetched_at,
        })

    upserted, errors = upsert_posts(supabase, records)
    print(f"  #{hashtag} → {len(items)} fetched, {upserted} upserted")
    return upserted, errors


def main() -> int:
    args = parse_args()
    supabase = get_client()

    total_upserted = 0
    total_errors = 0

    if args.platform in ("tiktok", "both"):
        print(f"TikTok hashtags ({len(TIKTOK_HASHTAGS)})...")
        for hashtag in TIKTOK_HASHTAGS:
            u, e = scrape_tiktok_hashtag(supabase, hashtag)
            total_upserted += u
            total_errors += e
            time.sleep(2)

        print(f"\nTikTok keywords ({len(TIKTOK_KEYWORDS)})...")
        for keyword in TIKTOK_KEYWORDS:
            u, e = scrape_tiktok_keyword(supabase, keyword)
            total_upserted += u
            total_errors += e
            time.sleep(2)

    if args.platform in ("instagram", "both"):
        print(f"\nInstagram hashtags ({len(INSTAGRAM_HASHTAGS)})...")
        for hashtag in INSTAGRAM_HASHTAGS:
            u, e = scrape_instagram_hashtag(supabase, hashtag)
            total_upserted += u
            total_errors += e
            time.sleep(2)

    print("---")
    print(f"Total upserted: {total_upserted} | Errors: {total_errors}")
    return 0 if total_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
