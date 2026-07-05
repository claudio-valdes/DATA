"""
Fetch TikTok profile + posts for a restaurant via Apify.
"""

import os
import time
from datetime import date, datetime, timezone

import httpx

from repository.db import get_client, paginate

APIFY_TOKEN = os.environ["APIFY_TOKEN"]


def fetch_tier_map(supabase) -> dict[str, int]:
    rows = paginate(lambda sb: sb.table("restaurant_pipeline").select("place_id, tier"))
    tier_map: dict[str, int] = {}
    for row in rows:
        if row.get("place_id") and row.get("tier") is not None:
            tier_map[row["place_id"]] = row["tier"]
    return tier_map


def fetch_restaurants(supabase, slug: str | None) -> list[dict]:
    if slug:
        r = supabase.table("silver_restaurants").select("place_id").eq("slug", slug).execute()
        if not r.data:
            return []
        place_id = r.data[0]["place_id"]
        r2 = (
            supabase.table("restaurant_social_handles")
            .select("restaurant_id, tiktok_handle")
            .eq("restaurant_id", place_id)
            .execute()
        )
        return [row for row in (r2.data or []) if row.get("tiktok_handle")]

    rows = paginate(
        lambda sb: sb.table("restaurant_social_handles").select("restaurant_id, tiktok_handle").not_.is_("tiktok_handle", "null")
    )
    return [row for row in rows if row.get("tiktok_handle")]


def run_apify_actor(actor_id: str, input_payload: dict) -> list:
    url = f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items?token={APIFY_TOKEN}"
    response = httpx.post(url, json=input_payload, timeout=300)
    response.raise_for_status()
    return response.json()


def fetch_social(slug: str | None = None) -> dict:
    """Fetch TikTok profile/posts for one restaurant (slug) or all Tier 3+ restaurants with a TikTok handle (slug=None)."""
    supabase = get_client()
    restaurants = fetch_restaurants(supabase, slug)

    if not restaurants:
        target = slug or "all restaurants"
        print(f"⚠️ No restaurants with TikTok handle found for {target}")
        return {"scraped": 0, "errors": 0}

    if slug is None:
        tier_map = fetch_tier_map(supabase)
        before = len(restaurants)
        restaurants = [r for r in restaurants if tier_map.get(r["restaurant_id"], 1) >= 3]
        skipped = before - len(restaurants)
        if skipped:
            print(f"⏭️  Skipping {skipped} restaurants below Tier 3")

    print(f"Processing {len(restaurants)} restaurants...")

    scraped = 0
    errors = 0

    for index, row in enumerate(restaurants):
        restaurant_id = row["restaurant_id"]
        tiktok_handle = row["tiktok_handle"]

        print(f"Fetching TikTok profile for @{tiktok_handle}...")
        try:
            items = run_apify_actor("clockworks~tiktok-profile-scraper", {
                "profiles": [tiktok_handle],
                "resultsLimit": 50,
            })

            if not items:
                print(f"  ⚠️ No data returned from Apify")
                continue

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
            scraped += 1

        except Exception as e:
            errors += 1
            print(f"  ✗ @{tiktok_handle} → {e}")

        if index < len(restaurants) - 1:
            time.sleep(2)

    print("---")
    print(f"Done: {scraped}/{len(restaurants)} scraped, {errors} errors")
    return {"scraped": scraped, "errors": errors}
