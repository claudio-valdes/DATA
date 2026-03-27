"""
Fetch Instagram comments for all posts belonging to a restaurant via Apify.

Usage:
    python scripts/fetch_ig_comments.py <restaurant_id>

Required env vars:
    APIFY_TOKEN
    NEXT_PUBLIC_SUPABASE_URL
    NEXT_PUBLIC_SUPABASE_ANON_KEY
"""

import os
import sys
import time
from datetime import datetime, timezone
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


def get_ig_handle(supabase, restaurant_id: str) -> str | None:
    r = supabase.table("restaurant_social_handles") \
        .select("ig_handle") \
        .eq("restaurant_id", restaurant_id) \
        .single() \
        .execute()
    if not r.data or not r.data.get("ig_handle"):
        return None
    return r.data["ig_handle"]


def get_posts_for_comments(supabase, restaurant_id: str) -> list:
    r = supabase.table("bronze_ig_posts") \
        .select("post_id, shortcode, comments_count, is_comments_disabled") \
        .eq("restaurant_id", restaurant_id) \
        .eq("is_comments_disabled", False) \
        .gt("comments_count", 0) \
        .execute()
    return r.data or []


def flatten_comments(post_id: str, restaurant_id: str, ig_handle: str, items: list) -> list:
    rows = []
    now = datetime.now(timezone.utc).isoformat()

    for comment in items:
        comment_id = comment.get("id")
        if not comment_id:
            continue

        rows.append({
            "comment_id": comment_id,
            "post_id": post_id,
            "restaurant_id": restaurant_id,
            "text": comment.get("text"),
            "owner_username": comment.get("ownerUsername"),
            "owner_id": comment.get("ownerId"),
            "owner_full_name": comment.get("ownerFullName"),
            "owner_is_verified": comment.get("ownerIsVerified", False),
            "owner_is_private": comment.get("ownerIsPrivate", False),
            "owner_latest_reel": comment.get("ownerLatestReelMedia"),
            "likes": comment.get("likesCount", 0),
            "replies_count": len(comment.get("replies", [])),
            "is_reply": False,
            "parent_comment_id": None,
            "is_restaurant_account": comment.get("ownerUsername") == ig_handle,
            "posted_at": comment.get("timestamp"),
            "fetched_at": now,
        })

        # Flatten nested replies
        for reply in comment.get("replies", []):
            reply_id = reply.get("id")
            if not reply_id:
                continue
            rows.append({
                "comment_id": reply_id,
                "post_id": post_id,
                "restaurant_id": restaurant_id,
                "text": reply.get("text"),
                "owner_username": reply.get("ownerUsername"),
                "owner_id": reply.get("ownerId"),
                "owner_full_name": reply.get("ownerFullName"),
                "owner_is_verified": reply.get("ownerIsVerified", False),
                "owner_is_private": reply.get("ownerIsPrivate", False),
                "owner_latest_reel": reply.get("ownerLatestReelMedia"),
                "likes": reply.get("likesCount", 0),
                "replies_count": 0,
                "is_reply": True,
                "parent_comment_id": comment_id,
                "is_restaurant_account": reply.get("ownerUsername") == ig_handle,
                "posted_at": reply.get("timestamp"),
                "fetched_at": now,
            })

    return rows


def main():
    if len(sys.argv) < 2:
        print("Usage: python fetch_ig_comments.py <restaurant_id>")
        sys.exit(1)

    restaurant_id = sys.argv[1]
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    ig_handle = get_ig_handle(supabase, restaurant_id)
    if not ig_handle:
        print(f"No Instagram handle found for restaurant_id={restaurant_id}. Exiting.")
        sys.exit(0)

    posts = get_posts_for_comments(supabase, restaurant_id)
    print(f"Found {len(posts)} posts with comments for @{ig_handle}")

    total_comments = 0

    for i, post in enumerate(posts):
        shortcode = post.get("shortcode")
        post_id = post["post_id"]
        if not shortcode:
            print(f"  [{i+1}/{len(posts)}] Skipping post {post_id} — no shortcode")
            continue

        print(f"  [{i+1}/{len(posts)}] Fetching comments for post {shortcode} ({post.get('comments_count', 0)} comments)...")
        try:
            items = run_apify_actor("apify~instagram-comment-scraper", {
                "directUrls": [f"https://www.instagram.com/p/{shortcode}"],
                "resultsLimit": 200,
            })
            rows = flatten_comments(post_id, restaurant_id, ig_handle, items)
            if rows:
                supabase.table("bronze_ig_comments").upsert(
                    rows, on_conflict="comment_id"
                ).execute()
            total_comments += len(rows)
            print(f"    {len(rows)} comments/replies upserted")
            time.sleep(2)
        except Exception as e:
            print(f"    ERROR: {e}")
            continue

    print(f"\n✓ Total comments/replies upserted: {total_comments}")


if __name__ == "__main__":
    main()
