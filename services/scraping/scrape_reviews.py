import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from outscraper import ApiClient

from repository.db import get_client, paginate

REVIEWS_LIMIT_BY_TIER: dict[int, int] = {
    1: 20,
    2: 50,
    3: 100,
    4: 100,
}
REVIEWS_LIMIT_DEFAULT = 20


def fetch_tier_map(supabase: Any) -> dict[str, int]:
    """Returns {place_id: tier} for all restaurants in restaurant_pipeline."""
    rows = paginate(lambda sb: sb.table("restaurant_pipeline").select("place_id, tier"))
    tier_map: dict[str, int] = {}
    for row in rows:
        if row.get("place_id") and row.get("tier") is not None:
            tier_map[row["place_id"]] = row["tier"]
    return tier_map


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

    rows = paginate(lambda sb: sb.table("silver_restaurants").select("id, slug, place_id").neq("place_id", "null"))
    return [row for row in rows if row.get("place_id")]


def fetch_recently_scraped_restaurants(supabase: Any) -> set[str]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    rows = paginate(lambda sb: sb.table("reviews").select("restaurant_id").gte("scraped_at", cutoff))
    return {row["restaurant_id"] for row in rows if row.get("restaurant_id")}


def fetch_reviews(client: ApiClient, place_id: str, reviews_limit: int) -> list[dict[str, Any]]:
    results = client.google_maps_reviews(
        place_id,
        reviews_limit=reviews_limit,
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


def scrape_reviews(slug: str | None = None) -> dict:
    """Scrape Google reviews for one restaurant (slug) or all restaurants with a place_id (slug=None)."""
    outscraper_key = os.environ["OUTSCRAPER_API_KEY"]
    supabase = get_client()

    client = ApiClient(api_key=outscraper_key)
    restaurants = fetch_restaurants(supabase, slug)
    tier_map = fetch_tier_map(supabase)
    scraped_at = datetime.now(timezone.utc)

    if not restaurants:
        target = slug or "all restaurants"
        print(f"⚠️ No restaurants found for {target}")
        return {"upserted": 0, "skipped": 0, "errors": 0}

    if slug is None:
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

        tier = tier_map.get(place_id, 1)
        reviews_limit = REVIEWS_LIMIT_BY_TIER.get(tier, REVIEWS_LIMIT_DEFAULT)

        try:
            reviews = fetch_reviews(client, place_id, reviews_limit)

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
            print(f"✓ {slug} (tier {tier}, limit {reviews_limit}) → {upserted}/{len(rows)} reviews upserted")

        except Exception as error:
            errors += 1
            print(f"✗ {slug} → {error}")

        if index < len(restaurants) - 1:
            time.sleep(2)

    print("---")
    print(f"Done: {len(restaurants)} restaurants processed")
    print(f"Reviews upserted: {total_upserted} | skipped: {total_skipped} | errors: {errors}")
    return {"upserted": total_upserted, "skipped": total_skipped, "errors": errors}
