"""
Fetch TikTok comments for all posts belonging to a restaurant via Apify.

Usage:
    python scripts/fetch_tiktok_comments.py <restaurant_id>

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


def get_tiktok_handle(supabase, restaurant_id: str) -> str | None:
    r = supabase.table("restaurant_social_handles") \
        .select("tiktok_handle") \
        .eq("restaurant_id", restaurant_id) \
        .single() \
        .execute()
    if not r.data or not r.data.get("tiktok_handle"):
        return None
    return r.data["tiktok_handle"]


def get_posts_for_comments(supabase, restaurant_id: str) -> list:
    r = supabase.table("bronze_tiktok_posts") \
        .select("post_id, web_video_url, comments_count") \
        .eq("restaurant_id", restaurant_id) \
        .gt("comments_count", 0) \
        .execute()
    return r.data or []


def main():
    if len(sys.argv) < 2:
        print("Usage: python fetch_tiktok_comments.py <restaurant_id>")
        sys.exit(1)

    restaurant_id = sys.argv[1]
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    tiktok_handle = get_tiktok_handle(supabase, restaurant_id)
    if not tiktok_handle:
        print(f"No TikTok handle found for restaurant_id={restaurant_id}. Exiting.")
        sys.exit(0)

    posts = get_posts_for_comments(supabase, restaurant_id)
    print(f"Found {len(posts)} posts with comments for @{tiktok_handle}")

    total_comments = 0
    now = datetime.now(timezone.utc).isoformat()

    for i, post in enumerate(posts):
        post_id = post["post_id"]
        web_video_url = post.get("web_video_url")
        if not web_video_url:
            print(f"  [{i+1}/{len(posts)}] Skipping post {post_id} — no web_video_url")
            continue

        print(f"  [{i+1}/{len(posts)}] Fetching comments for post {post_id} ({post.get('comments_count', 0)} comments)...")
        try:
            items = run_apify_actor("clockworks~tiktok-comment-scraper", {
                "postURLs": [web_video_url],
                "resultsLimit": 200,
            })

            rows = []
            for comment in items:
                comment_id = comment.get("id") or comment.get("cid")
                if not comment_id:
                    continue

                # TikTok comments may have nested replies
                is_reply = bool(comment.get("replyCommentTotal") is None and comment.get("replyId"))
                parent_id = comment.get("replyId") if is_reply else None

                rows.append({
                    "comment_id": str(comment_id),
                    "post_id": post_id,
                    "restaurant_id": restaurant_id,
                    "text": comment.get("text"),
                    "owner_username": comment.get("uniqueId") or comment.get("authorUsername"),
                    "owner_id": str(comment.get("uid") or comment.get("authorId") or ""),
                    "likes": comment.get("diggCount", 0),
                    "replies_count": comment.get("replyCommentTotal", 0) or 0,
                    "is_reply": is_reply,
                    "parent_comment_id": str(parent_id) if parent_id else None,
                    "is_restaurant_account": (comment.get("uniqueId") or comment.get("authorUsername")) == tiktok_handle,
                    "posted_at": datetime.fromtimestamp(comment["createTime"], tz=timezone.utc).isoformat() if comment.get("createTime") else None,
                    "fetched_at": now,
                })

            if rows:
                supabase.table("bronze_tiktok_comments").upsert(
                    rows, on_conflict="comment_id"
                ).execute()
            total_comments += len(rows)
            print(f"    {len(rows)} comments upserted")
            time.sleep(2)
        except Exception as e:
            print(f"    ERROR: {e}")
            continue

    print(f"\n✓ Total TikTok comments upserted: {total_comments}")


if __name__ == "__main__":
    main()
