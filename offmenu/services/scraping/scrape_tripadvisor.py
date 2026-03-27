#!/usr/bin/env python3

import argparse
import os
import re
import sys
import time
from typing import Any

from dotenv import load_dotenv
from serpapi import GoogleSearch
from supabase import create_client


TRIPADVISOR_DOMAIN = "www.tripadvisor.com"


def load_environment() -> tuple[str, str, str]:
    load_dotenv(".env.local")
    load_dotenv(".env")

    serpapi_key = os.getenv("SERPAPI_KEY")
    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

    missing = []
    if not serpapi_key:
        missing.append("SERPAPI_KEY")
    if not supabase_url:
        missing.append("SUPABASE_URL")
    if not supabase_key:
        missing.append("SUPABASE_KEY")

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return serpapi_key, supabase_url, supabase_key


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape TripAdvisor details and reviews via Google Search discovery")
    parser.add_argument("--slug", help="Only process one restaurant slug")
    parser.add_argument("--all", action="store_true", help="Process all restaurants")
    args = parser.parse_args()

    if bool(args.slug) == bool(args.all):
        parser.error("Use exactly one of --slug or --all")

    return args


def fetch_restaurants(supabase: Any, slug: str | None) -> list[dict[str, Any]]:
    query = supabase.table("silver_restaurants").select(
        "raw_ingestion_id, slug, name, review_count"
    )

    if slug:
        query = query.eq("slug", slug)

    result = query.execute()
    rows = result.data or []
    return [row for row in rows if row.get("slug") and row.get("name")]


def google_search_tripadvisor_url(serpapi_key: str, restaurant_name: str, address: str | None) -> dict[str, Any]:
    query_parts = [restaurant_name]
    if address:
        query_parts.append(address)
    query_parts.append("site:tripadvisor.com")

    params = {
        "engine": "google",
        "q": " ".join(query_parts),
        "num": 3,
        "api_key": serpapi_key,
    }
    return GoogleSearch(params).get_dict()


def extract_tripadvisor_url(search_results: dict[str, Any]) -> str | None:
    organic = search_results.get("organic_results") or []
    for result in organic:
        url = result.get("link", "")
        if "tripadvisor.com/Restaurant_Review" in url:
            return url
    return None


def extract_place_id(ta_url: str) -> str | None:
    match = re.search(r"-d(\d+)-", ta_url)
    if not match:
        return None
    return match.group(1)


def tripadvisor_place(serpapi_key: str, place_id: str) -> dict[str, Any]:
    params = {
        "engine": "tripadvisor_place",
        "place_id": place_id,
        "tripadvisor_domain": TRIPADVISOR_DOMAIN,
        "api_key": serpapi_key,
    }
    return GoogleSearch(params).get_dict()


def parse_ranking(ranking_str: str | None) -> tuple[int | None, int | None, float | None]:
    if not ranking_str:
        return None, None, None

    match = re.search(r"#(\d+) of ([\d,]+)", ranking_str)
    if not match:
        return None, None, None

    position = int(match.group(1))
    total = int(match.group(2).replace(",", ""))
    percentile = round((position / total) * 100, 1) if total else None
    return position, total, percentile


def parse_tourist_signals(place: dict[str, Any], google_review_count: int | None) -> tuple[float | None, float | None, bool, list[dict[str, Any]]]:
    reviews_data = place.get("reviews_data") or []
    total_sampled = len(reviews_data)

    non_german = [
        review
        for review in reviews_data
        if review.get("language") not in ("de", None, "")
    ]
    tourist_ratio = round(len(non_german) / total_sampled, 3) if total_sampled > 0 else None

    ta_review_count = place.get("reviews")
    ta_to_google_ratio = None
    if google_review_count and ta_review_count:
        ta_to_google_ratio = round(ta_review_count / google_review_count, 3)

    is_tourist_venue = (
        (tourist_ratio is not None and tourist_ratio > 0.4)
        or (ta_to_google_ratio is not None and ta_to_google_ratio > 0.3)
    )

    return ta_to_google_ratio, tourist_ratio, is_tourist_venue, reviews_data[:10]


def build_upsert_row(
    restaurant: dict[str, Any],
    ta_url: str | None,
    place_id: str | None,
    raw_response: Any,
    error_message: str | None = None,
) -> dict[str, Any]:
    place = raw_response.get("place_result", {}) if isinstance(raw_response, dict) else {}
    ranking_position, ranking_total, ranking_percentile = parse_ranking(place.get("ranking"))
    ta_to_google_ratio, tourist_ratio, is_tourist_venue, reviews_sample = parse_tourist_signals(
        place,
        restaurant.get("review_count"),
    )

    return {
        "raw_ingestion_id": restaurant.get("raw_ingestion_id"),
        "slug": restaurant.get("slug"),
        "ta_place_id": place_id,
        "ta_url": ta_url,
        "ta_name": place.get("name"),
        "ta_rating": place.get("rating"),
        "ta_review_count": place.get("reviews"),
        "ta_ranking_position": ranking_position,
        "ta_ranking_total": ranking_total,
        "ta_ranking_percentile": ranking_percentile,
        "ta_description": place.get("description"),
        "ta_price_range": place.get("price_range"),
        "ta_cuisine": place.get("cuisine"),
        "ta_has_award": bool(place.get("awards")),
        "google_review_count": restaurant.get("review_count"),
        "ta_to_google_ratio": ta_to_google_ratio,
        "tourist_review_ratio": tourist_ratio,
        "is_tourist_venue": is_tourist_venue,
        "reviews_sample": reviews_sample,
        "raw_response": raw_response,
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "error_message": error_message,
    }


def build_empty_row(
    restaurant: dict[str, Any],
    ta_url: str | None,
    place_id: str | None,
    error_message: str | None,
    raw_response: Any,
) -> dict[str, Any]:
    return {
        "raw_ingestion_id": restaurant.get("raw_ingestion_id"),
        "slug": restaurant.get("slug"),
        "ta_place_id": place_id,
        "ta_url": ta_url,
        "ta_name": None,
        "ta_rating": None,
        "ta_review_count": None,
        "ta_ranking_position": None,
        "ta_ranking_total": None,
        "ta_ranking_percentile": None,
        "ta_description": None,
        "ta_price_range": None,
        "ta_cuisine": None,
        "ta_has_award": None,
        "google_review_count": restaurant.get("review_count"),
        "ta_to_google_ratio": None,
        "tourist_review_ratio": None,
        "is_tourist_venue": None,
        "reviews_sample": None,
        "raw_response": raw_response,
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "error_message": error_message,
    }


def upsert_row(supabase: Any, row: dict[str, Any]) -> None:
    result = supabase.table("tripadvisor_enrichments").upsert(row, on_conflict="slug").execute()
    if getattr(result, "error", None):
        raise RuntimeError(result.error.message)


def format_summary(slug: str, row: dict[str, Any]) -> str:
    ta_rating = row.get("ta_rating")
    ta_review_count = row.get("ta_review_count")
    position = row.get("ta_ranking_position")
    total = row.get("ta_ranking_total")
    percentile = row.get("ta_ranking_percentile")
    tourist = "YES" if row.get("is_tourist_venue") else "NO"
    tourist_ratio = row.get("tourist_review_ratio")
    tourist_ratio_text = (
        f"{round(tourist_ratio * 100)}% non-DE" if tourist_ratio is not None else "lang n/a"
    )
    return (
        f"✓ {slug} → TA: {ta_rating}★ | {ta_review_count} reviews | "
        f"#{position}/{total} ({percentile}th pct) | tourist: {tourist} ({tourist_ratio_text})"
    )


def main() -> int:
    args = parse_args()

    try:
        serpapi_key, supabase_url, supabase_key = load_environment()
    except RuntimeError as error:
        print(f"❌ {error}")
        return 1

    supabase = create_client(supabase_url, supabase_key)
    restaurants = fetch_restaurants(supabase, args.slug)

    if not restaurants:
        target = args.slug or "all restaurants"
        print(f"⚠️ No restaurants found for {target}")
        return 0

    scraped = 0
    errors = 0

    for index, restaurant in enumerate(restaurants):
        try:
            search_results = google_search_tripadvisor_url(
                serpapi_key,
                restaurant["name"],
                restaurant.get("address"),
            )
            ta_url = extract_tripadvisor_url(search_results)

            if not ta_url:
                upsert_row(
                    supabase,
                    build_empty_row(
                        restaurant,
                        None,
                        None,
                        "TripAdvisor URL not found via Google Search",
                        search_results,
                    ),
                )
                print(f"✗ {restaurant['slug']} → TripAdvisor URL not found via Google Search")
                continue

            place_id = extract_place_id(ta_url)
            if not place_id:
                upsert_row(
                    supabase,
                    build_empty_row(
                        restaurant,
                        ta_url,
                        None,
                        "place_id regex failed",
                        search_results,
                    ),
                )
                print(f"✗ {restaurant['slug']} → failed to extract place_id from URL")
                continue

            raw_response = tripadvisor_place(serpapi_key, place_id)
            row = build_upsert_row(restaurant, ta_url, place_id, raw_response)
            upsert_row(supabase, row)
            scraped += 1
            print(format_summary(restaurant["slug"], row))
        except Exception as error:
            errors += 1
            try:
                upsert_row(
                    supabase,
                    build_empty_row(
                        restaurant,
                        None,
                        None,
                        str(error),
                        None,
                    ),
                )
            except Exception:
                pass
            print(f"✗ {restaurant['slug']} → {error}")

        if index < len(restaurants) - 1:
            time.sleep(2)

    print("---")
    print(f"Done: {scraped}/{len(restaurants)} scraped, {errors} errors")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
