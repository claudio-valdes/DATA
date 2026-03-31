"""
Fetch Instagram profile + posts + hashtag posts for a restaurant via Apify.

Usage:
    python offmenu/services/instagram/fetch_ig_social.py --slug <slug>
    python offmenu/services/instagram/fetch_ig_social.py --all

Required env vars:
    APIFY_TOKEN
    SUPABASE_URL  (or NEXT_PUBLIC_SUPABASE_URL)
    SUPABASE_KEY  (or NEXT_PUBLIC_SUPABASE_ANON_KEY)
"""

import argparse
import os
import sys
import time
from datetime import date, datetime, timezone
from dotenv import load_dotenv
import httpx
from supabase import create_client

load_dotenv(".env.local")

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.environ["NEXT_PUBLIC_SUPABASE_URL"]
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ["NEXT_PUBLIC_SUPABASE_ANON_KEY"]
APIFY_TOKEN = os.environ["APIFY_TOKEN"]

# Berlin-specific hashtags for discovery
BERLIN_HASHTAGS = [
    "berlinfood",
    "berlinrestaurant",
    "berlineats",
    "foodberlin",
    "berlinfoodies",
    "chinesefoodberlin",
    "berlinasianfood",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Instagram profiles and posts via Apify")
    parser.add_argument("--slug", help="Process one restaurant by slug")
    parser.add_argument("--all", action="store_true", help="Process all restaurants with an IG handle")
    args = parser.parse_args()

    if bool(args.slug) == bool(args.all):
        parser.error("Use exactly one of --slug or --all")

    return args


def fetch_restaurants(supabase, slug: str | None) -> list[dict]:
    if slug:
        r = supabase.table("silver_restaurants").select("place_id").eq("slug", slug).execute()
        if not r.data:
            return []
        place_id = r.data[0]["place_id"]
        r2 = (
            supabase.table("restaurant_social_handles")
            .select("restaurant_id, ig_handle")
            .eq("restaurant_id", place_id)
            .execute()
        )
        return [row for row in (r2.data or []) if row.get("ig_handle")]

    rows = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            supabase.table("restaurant_social_handles")
            .select("restaurant_id, ig_handle")
            .not_.is_("ig_handle", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = result.data or []
        rows.extend(row for row in batch if row.get("ig_handle"))
        if len(batch) < page_size:
            break
        offset += page_size

    return rows


def run_apify_actor(actor_id: str, input_payload: dict) -> list:
    url = f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items?token={APIFY_TOKEN}"
    response = httpx.post(url, json=input_payload, timeout=300)
    response.raise_for_status()
    return response.json()


def upsert_profile(supabase, restaurant_id: str, ig_handle: str, items: list):
    if not items:
        print("  No profile data returned.")
        return

    profile = items[0]
    posts = profile.get("latestPosts", []) or []

    likes_list = [p.get("likesCount", 0) or 0 for p in posts]
    comments_list = [p.get("commentsCount", 0) or 0 for p in posts]
    avg_likes = sum(likes_list) / len(likes_list) if likes_list else None
    avg_comments = sum(comments_list) / len(comments_list) if comments_list else None

    supabase.table("bronze_ig_profiles").upsert({
        "restaurant_id": restaurant_id,
        "ig_handle": ig_handle,
        "followers": profile.get("followersCount"),
        "following": profile.get("followsCount"),
        "verified": profile.get("verified", False),
        "business_account": profile.get("isBusinessAccount", False),
        "category": profile.get("businessCategoryName"),
        "bio": profile.get("biography"),
        "website": profile.get("externalUrl"),
        "post_count_returned": len(posts),
        "avg_likes": avg_likes,
        "avg_comments": avg_comments,
        "snapshot_date": date.today().isoformat(),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }, on_conflict="restaurant_id,snapshot_date").execute()
    print(f"  Profile upserted — {profile.get('followersCount', 0):,} followers")

    return posts


def upsert_posts(supabase, restaurant_id: str, ig_handle: str, posts: list) -> int:
    records = []
    for post in posts:
        records.append({
            "post_id": post.get("id") or post.get("shortCode"),
            "restaurant_id": restaurant_id,
            "ig_handle": ig_handle,
            "shortcode": post.get("shortCode"),
            "type": post.get("type"),
            "caption": post.get("caption"),
            "hashtags": post.get("hashtags", []),
            "mentions": post.get("mentions", []),
            "tagged_users": post.get("taggedUsers"),
            "likes": post.get("likesCount"),
            "comments_count": post.get("commentsCount"),
            "is_comments_disabled": post.get("commentsDisabled", False),
            "video_url": post.get("videoUrl"),
            "display_url": post.get("displayUrl"),
            "location_name": post.get("locationName"),
            "location_id": post.get("locationId"),
            "posted_at": post.get("timestamp"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        })

    if records:
        supabase.table("bronze_ig_posts").upsert(records, on_conflict="post_id").execute()
    return len(records)


def upsert_hashtag_posts(supabase, hashtag: str, items: list) -> int:
    records = []
    for item in items:
        post_id = item.get("id") or item.get("shortCode")
        if not post_id:
            continue
        records.append({
            "post_id": post_id,
            "hashtag_queried": hashtag,
            "owner_username": item.get("ownerUsername"),
            "owner_id": item.get("ownerId"),
            "caption": item.get("caption"),
            "hashtags": item.get("hashtags", []),
            "likes": item.get("likesCount"),
            "comments_count": item.get("commentsCount"),
            "location_name": item.get("locationName"),
            "posted_at": item.get("timestamp"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        })

    if records:
        supabase.table("bronze_ig_hashtag_posts").upsert(
            records, on_conflict="post_id,hashtag_queried"
        ).execute()
    return len(records)


def main():
    args = parse_args()
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    restaurants = fetch_restaurants(supabase, args.slug)

    if not restaurants:
        target = args.slug or "all restaurants"
        print(f"⚠️ No restaurants with IG handle found for {target}")
        sys.exit(0)

    print(f"Processing {len(restaurants)} restaurants...")

    scraped = 0
    errors = 0

    for index, row in enumerate(restaurants):
        restaurant_id = row["restaurant_id"]
        ig_handle = row["ig_handle"]

        print(f"Fetching Instagram profile for @{ig_handle}...")
        try:
            items = run_apify_actor("apify~instagram-profile-scraper", {
                "usernames": [ig_handle],
                "resultsLimit": 50,
            })
            posts = upsert_profile(supabase, restaurant_id, ig_handle, items) or []
            post_count = upsert_posts(supabase, restaurant_id, ig_handle, posts)
            print(f"  Posts upserted: {post_count}")
            scraped += 1
        except Exception as e:
            errors += 1
            print(f"  ✗ @{ig_handle} → {e}")

        if index < len(restaurants) - 1:
            time.sleep(2)

    print(f"\n---")
    print(f"Profiles: {scraped}/{len(restaurants)} scraped, {errors} errors")

    print(f"\nFetching hashtag posts for {len(BERLIN_HASHTAGS)} Berlin hashtags...")
    total_hashtag_posts = 0
    for hashtag in BERLIN_HASHTAGS:
        try:
            items = run_apify_actor("apify~instagram-hashtag-scraper", {
                "hashtags": [hashtag],
                "resultsLimit": 30,
            })
            count = upsert_hashtag_posts(supabase, hashtag, items)
            total_hashtag_posts += count
            print(f"  #{hashtag}: {count} posts")
            time.sleep(2)
        except Exception as e:
            print(f"  ✗ #{hashtag} → {e}")

    print(f"Hashtag posts upserted: {total_hashtag_posts}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
