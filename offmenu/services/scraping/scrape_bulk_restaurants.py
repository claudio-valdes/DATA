#!/usr/bin/env python3

import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from outscraper import ApiClient
from supabase import create_client


NEIGHBOURHOODS = [
    "Prenzlauer Berg",
    "Mitte",
    "Kreuzberg",
    "Friedrichshain",
    "Neukölln",
    "Schöneberg",
    "Charlottenburg",
    "Wilmersdorf",
    "Tempelhof",
    "Wedding",
    "Steglitz",
    "Pankow",
    "Lichtenberg",
    "Treptow",
    "Spandau",
    "Zehlendorf",
]

POTSDAM_QUERY = "restaurants in Potsdam"

OCCASION_LABELS = [
    "Breakfast",
    "Brunch",
    "Late-night food",
    "Lunch",
    "Romantic",
    "Cosy",
    "Trendy",
    "Family friendly",
    "LGBTQ+ friendly",
]

EXCLUDE_LABELS = {
    "Restaurant",
    "Dine-in",
    "Takeaway",
    "Coffee",
    "Beer",
    "Wine",
    "Alcohol",
    "Spirits",
    "Cocktails",
    "Small plates",
    "Healthy options",
    "Vegetarian options",
    "Quick bite",
    "Delivery",
}

INCLUDE_LABEL_TYPES = {
    "atmosphere",
    "crowd",
    "offering",
    "cuisine",
}

PROFILE_FIELDS = [
    "name",
    "place_id",
    "address",
    "street",
    "city",
    "postal_code",
    "latitude",
    "longitude",
    "rating",
    "reviews",
    "reviews_per_score",
    "phone",
    "website",
    "type",
    "subtypes",
    "category",
    "range",
    "description",
    "working_hours",
    "working_hours_csv_compatible",
    "photos_count",
    "about",
    "business_status",
    "verified",
    "owner_id",
    "logo",
    "photo",
]


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
    parser = argparse.ArgumentParser(description="Bulk scrape Berlin restaurant profiles into raw_ingestions")
    parser.add_argument("--query", help="Run a single query")
    parser.add_argument("--neighbourhoods", action="store_true", help="Run all neighbourhood queries")
    parser.add_argument("--labels", action="store_true", help="Run all label-based queries")
    parser.add_argument("--all", action="store_true", help="Run all queries")
    args = parser.parse_args()

    selected = sum(bool(value) for value in [args.query, args.neighbourhoods, args.labels, args.all])
    if selected != 1:
        parser.error("Use exactly one of --query, --neighbourhoods, --labels, or --all")

    return args


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def flatten_results(results: Any) -> list[dict[str, Any]]:
    if isinstance(results, list):
        flattened = []
        for item in results:
            if isinstance(item, list):
                flattened.extend(subitem for subitem in item if isinstance(subitem, dict))
            elif isinstance(item, dict):
                flattened.append(item)
        return flattened
    return []


def format_label_query(label_value: str, suffix: str = "Berlin") -> str:
    cleaned = label_value.replace("restaurant", "").replace("Restaurant", "").strip()
    return f"{cleaned} restaurant {suffix}"


def build_query_list(supabase: Any) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()

    def add(q: str) -> None:
        if q not in seen:
            seen.add(q)
            queries.append(q)

    # --- Neighbourhood queries ---
    for neighbourhood in NEIGHBOURHOODS:
        add(f"restaurants in {neighbourhood} Berlin")
    add(POTSDAM_QUERY)

    # --- Label-based queries (flat, city-wide) ---
    response = (
        supabase.table("silver_labels")
        .select("label_value, label_type")
        .in_("label_type", list(INCLUDE_LABEL_TYPES))
        .execute()
    )

    label_rows = response.data or []

    seen_labels: set[str] = set()
    valid_labels: list[str] = []
    for row in label_rows:
        lv = row["label_value"]
        if lv not in seen_labels and lv not in EXCLUDE_LABELS:
            seen_labels.add(lv)
            valid_labels.append(lv)

    for label_value in valid_labels:
        add(format_label_query(label_value))

    # --- Neighbourhood x occasion combined queries ---
    for neighbourhood in NEIGHBOURHOODS:
        for label_value in OCCASION_LABELS:
            cleaned = label_value.replace("restaurant", "").replace("Restaurant", "").strip()
            add(f"{cleaned} restaurant {neighbourhood} Berlin")

    return queries


def run_search(client: ApiClient, query: str) -> list[dict[str, Any]]:
    results = client.google_maps_search(
        query,
        limit=100,
        language="en",
        region="DE",
        fields=PROFILE_FIELDS,
    )
    return flatten_results(results)


def upsert_search_ranking(
    supabase: Any,
    query: str,
    place_id: str,
    position: int,
    scraped_at: datetime,
    rating: float | None,
    review_count: int | None,
) -> None:
    record = {
        "place_id": place_id,
        "query": query,
        "position": position,
        "scraped_at": scraped_at.isoformat(),
        "scraped_date": scraped_at.date().isoformat(),
        "rating": rating,
        "review_count": review_count,
    }
    result = (
        supabase.table("bronze_search_rankings")
        .upsert(record, on_conflict="place_id,query,scraped_date")
        .execute()
    )
    if getattr(result, "error", None):
        raise RuntimeError(result.error.message)


def upsert_raw_ingestion(supabase: Any, query: str, restaurant: dict[str, Any]) -> bool:
    place_id = restaurant.get("place_id")
    if not place_id:
        return False

    record = {
        "source": "outscraper",
        "query": query,
        "slug": slugify(restaurant.get("name") or place_id),
        "place_id": place_id,
        "raw_data": {"data": [restaurant]},
    }

    result = supabase.table("raw_ingestions").upsert(record, on_conflict="place_id").execute()
    if getattr(result, "error", None):
        raise RuntimeError(result.error.message)
    return True


def selected_queries(args: argparse.Namespace, supabase: Any) -> list[str]:
    if args.query:
        return [args.query]
    if args.neighbourhoods:
        queries = [f"restaurants in {n} Berlin" for n in NEIGHBOURHOODS]
        queries.append(POTSDAM_QUERY)
        return queries
    if args.labels:
        response = (
            supabase.table("silver_labels")
            .select("label_value, label_type")
            .in_("label_type", list(INCLUDE_LABEL_TYPES))
            .execute()
        )
        seen: set[str] = set()
        queries: list[str] = []
        for row in response.data or []:
            lv = row["label_value"]
            if lv not in seen and lv not in EXCLUDE_LABELS:
                seen.add(lv)
                queries.append(format_label_query(lv))
        return queries
    return build_query_list(supabase)


def main() -> int:
    args = parse_args()

    try:
        outscraper_key, supabase_url, supabase_key = load_environment()
    except RuntimeError as error:
        print(f"❌ {error}")
        return 1

    client = ApiClient(api_key=outscraper_key)
    supabase = create_client(supabase_url, supabase_key)
    queries = selected_queries(args, supabase)
    scraped_at = datetime.now(timezone.utc)

    print(f"Queries to run: {len(queries)}")

    total_found = 0
    total_upserts = 0
    duplicates_in_run = 0
    errors = 0
    seen_place_ids: set[str] = set()

    for index, query in enumerate(queries):
        try:
            restaurants = run_search(client, query)
            if not restaurants:
                print(f"⚠️ {query} → no results")
                continue

            query_found = len(restaurants)
            query_upserts = 0
            query_duplicates = 0
            total_found += query_found

            for position, restaurant in enumerate(restaurants, start=1):
                place_id = restaurant.get("place_id")
                if not place_id:
                    continue

                try:
                    rating = restaurant.get("rating")
                    reviews = restaurant.get("reviews")
                    upsert_search_ranking(
                        supabase, query, place_id, position, scraped_at,
                        rating=float(rating) if rating is not None else None,
                        review_count=int(reviews) if reviews is not None else None,
                    )
                except Exception as error:
                    errors += 1
                    print(f"✗ {query} → ranking upsert failed for {place_id}: {error}")

                if place_id in seen_place_ids:
                    query_duplicates += 1
                    duplicates_in_run += 1
                    continue

                seen_place_ids.add(place_id)

                try:
                    if upsert_raw_ingestion(supabase, query, restaurant):
                        total_upserts += 1
                        query_upserts += 1
                except Exception as error:
                    errors += 1
                    print(f"✗ {query} → upsert failed for {place_id}: {error}")

            print(
                f"✓ {query} → found: {query_found} | upserts: {query_upserts} | "
                f"duplicates in run: {query_duplicates}"
            )
        except Exception as error:
            errors += 1
            print(f"✗ {query} → {error}")

        if index < len(queries) - 1:
            time.sleep(2)

    print("---")
    print(f"Queries run: {len(queries)}")
    print(f"Restaurants found: {total_found}")
    print(f"Upserts attempted: {total_upserts}")
    print(f"Duplicates skipped in run: {duplicates_in_run}")
    print(f"Errors: {errors}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
