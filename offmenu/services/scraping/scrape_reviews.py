import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from dotenv import load_dotenv
from outscraper import ApiClient
from supabase import create_client


REVIEWS_LIMIT = 100


def load_environment() -> tuple[str, str, str]:
    load_dotenv(".env.local")

    outscraper_key = os.getenv("OUTSCRAPER_API_KEY")
    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

    missing = []
    if not outscraper_key:
        missing.append("OUTSCRAPER_API_KEY")
    if not supabase_url:
        missing.append("SUPABASE_URL")
    if not supabase_key:
        missing.append("SUPABASE_KEY")

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return outscraper_key, supabase_url, supabase_key


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Google reviews via OutScraper into reviews table")
    parser.add_argument("--slug", help="Only process one restaurant slug")
    parser.add_argument("--all", action="store_true", help="Process all restaurants with a place_id")
    args = parser.parse_args()

    if bool(args.slug) == bool(args.all):
        parser.error("Use exactly one of --slug or --all")

    return args


def fetch_restaurants(supabase: Any, slug: str | None) -> list[dict[str, Any]]:
    if slug:
        result = (
            supabase.table("silver_restaurants")
            .select("id, slug, place_id")
            .eq("slug", slug)
            .execute()
        )
        rows = result.data or []
        return [row for row in rows if row.get("place_id")]

    rows = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            supabase.table("silver_restaurants")
            .select("id, slug, place_id")
            .neq("place_id", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = result.data or []
        rows.extend(row for row in batch if row.get("place_id"))
        if len(batch) < page_size:
            break
        offset += page_size

    return rows


def fetch_recently_scraped_restaurants(supabase: Any) -> set[str]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    restaurant_ids: set[str] = set()
    page_size = 1000
    offset = 0

    while True:
        result = (
            supabase.table("reviews")
            .select("restaurant_id")
            .gte("scraped_at", cutoff)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = result.data or []
        restaurant_ids.update(row["restaurant_id"] for row in batch if row.get("restaurant_id"))
        if len(batch) < page_size:
            break
        offset += page_size

    return restaurant_ids


def fetch_reviews(client: ApiClient, place_id: str) -> list[dict[str, Any]]:
    results = client.google_maps_reviews(
        place_id,
        reviews_limit=REVIEWS_LIMIT,
        language=["en", "de"],
    )

    if not results or not isinstance(results, list):
        return []

    place = results[0] if isinstance(results[0], dict) else {}
    return place.get("reviews_data") or []


def build_review_row(
    restaurant_id: str,
    review: dict[str, Any],
    scraped_at: datetime,
) -> dict[str, Any] | None:
    google_review_id = review.get("review_id")
    if not google_review_id:
        return None

    owner_answer_date = review.get("owner_answer_datetime_utc")

    return {
        "restaurant_id": restaurant_id,
        "google_review_id": google_review_id,
        "author_name": review.get("author_title"),
        "author_id": review.get("author_id"),
        "review_text": review.get("review_text"),
        "rating": review.get("review_rating"),
        "review_date": review.get("review_datetime_utc"),
        "owner_answer": review.get("owner_answer"),
        "owner_answer_date": owner_answer_date if owner_answer_date else None,
        "likes": review.get("review_likes") or 0,
        "scraped_at": scraped_at.isoformat(),
    }


def main() -> int:
    args = parse_args()

    try:
        outscraper_key, supabase_url, supabase_key = load_environment()
    except RuntimeError as error:
        print(f"❌ {error}")
        return 1

    client = ApiClient(api_key=outscraper_key)
    supabase = create_client(supabase_url, supabase_key)
    restaurants = fetch_restaurants(supabase, args.slug)
    scraped_at = datetime.now(timezone.utc)

    if not restaurants:
        target = args.slug or "all restaurants"
        print(f"⚠️ No restaurants found for {target}")
        return 0

    if args.all:
        recently_scraped = fetch_recently_scraped_restaurants(supabase)
        before = len(restaurants)
        restaurants = [r for r in restaurants if r.get("id") not in recently_scraped]
        skipped = before - len(restaurants)
        if skipped:
            print(f"⏭️  Skipping {skipped} restaurants scraped within the last 7 days")

    total_upserted = 0
    total_skipped = 0
    errors = 0

    for index, restaurant in enumerate(restaurants):
        slug = restaurant["slug"]
        place_id = restaurant["place_id"]
        restaurant_id = restaurant["id"]

        try:
            reviews = fetch_reviews(client, place_id)

            if not reviews:
                print(f"⚠️ {slug} → no reviews found")
                continue

            rows = [build_review_row(restaurant_id, r, scraped_at) for r in reviews]
            rows = [r for r in rows if r is not None]

            upserted = 0
            for row in rows:
                try:
                    result = (
                        supabase.table("reviews")
                        .upsert(row, on_conflict="google_review_id", ignore_duplicates=True)
                        .execute()
                    )
                    if getattr(result, "error", None):
                        raise RuntimeError(result.error.message)
                    upserted += 1
                except Exception as row_error:
                    total_skipped += 1
                    print(f"  ✗ {slug} → row upsert failed: {row_error}")

            total_upserted += upserted
            print(f"✓ {slug} → {upserted}/{len(rows)} reviews upserted")

        except Exception as error:
            errors += 1
            print(f"✗ {slug} → {error}")

        if index < len(restaurants) - 1:
            time.sleep(2)

    print("---")
    print(f"Done: {len(restaurants)} restaurants processed")
    print(f"Reviews upserted: {total_upserted} | skipped: {total_skipped} | errors: {errors}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
