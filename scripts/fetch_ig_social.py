"""
Fetch Instagram profile + posts + hashtag posts for a restaurant via Apify.

Usage:
    python scripts/fetch_ig_social.py <restaurant_id>

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


def run_apify_actor(actor_id: str, input_payload: dict) -> list:
    url = f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items?token={APIFY_TOKEN}"
    response = httpx.post(url, json=input_payload, timeout=300)
    response.raise_for_status()
    return response.json()


def get_ig_handle(supabase, restaurant_id: str) -> str | None:
    r = supabase.table("restaurant_social_handles") \
        .select("ig_handle") \
        .eq("restaurant_id", restaurant_id) \
        .single() \
        .execute()
    if not r.data or not r.data.get("ig_handle"):
        return None
    return r.data["ig_handle"]


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
    if len(sys.argv) < 2:
        print("Usage: python fetch_ig_social.py <restaurant_id>")
        sys.exit(1)

    restaurant_id = sys.argv[1]
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    ig_handle = get_ig_handle(supabase, restaurant_id)
    if not ig_handle:
        print(f"No Instagram handle found for restaurant_id={restaurant_id}. Exiting.")
        sys.exit(0)

    print(f"Fetching Instagram profile for @{ig_handle}...")
    try:
        items = run_apify_actor("apify~instagram-profile-scraper", {
            "usernames": [ig_handle],
            "resultsLimit": 50,
        })
        posts = upsert_profile(supabase, restaurant_id, ig_handle, items)
        posts = posts or []
        post_count = upsert_posts(supabase, restaurant_id, ig_handle, posts)
        print(f"  Posts upserted: {post_count}")
    except Exception as e:
        print(f"  ERROR fetching profile: {e}")

    time.sleep(2)

    print(f"Fetching hashtag posts for {len(BERLIN_HASHTAGS)} Berlin hashtags...")
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
            print(f"  ERROR fetching #{hashtag}: {e}")
            continue

    print(f"\n✓ Profile + posts fetched for @{ig_handle}")
    print(f"✓ Hashtag posts upserted: {total_hashtag_posts}")


if __name__ == "__main__":
    main()
