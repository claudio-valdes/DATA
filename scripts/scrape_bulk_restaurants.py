#!/usr/bin/env python3

import argparse
import os
import re
import sys
import time
from typing import Any

from dotenv import load_dotenv
from outscraper import ApiClient
from supabase import create_client


NEIGHBOURHOOD_QUERIES = [
    "restaurants in Prenzlauer Berg Berlin",
    "restaurants in Mitte Berlin",
    "restaurants in Kreuzberg Berlin",
    "restaurants in Friedrichshain Berlin",
    "restaurants in Neukölln Berlin",
    "restaurants in Schöneberg Berlin",
    "restaurants in Charlottenburg Berlin",
    "restaurants in Mitte Berlin",
    "restaurants in Tempelhof Berlin",
    "restaurants in Wedding Berlin",
]

CUISINE_QUERIES = [
    "Chinese restaurant Berlin",
    "Vietnamese restaurant Berlin",
    "Italian restaurant Berlin",
    "Turkish restaurant Berlin",
    "Japanese restaurant Berlin",
    "Korean restaurant Berlin",
    "Thai restaurant Berlin",
    "Indian restaurant Berlin",
    "Lebanese restaurant Berlin",
    "Mexican restaurant Berlin",
    "Greek restaurant Berlin",
    "French restaurant Berlin",
]

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
    load_dotenv(".env")

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
    parser.add_argument("--cuisines", action="store_true", help="Run all cuisine queries")
    parser.add_argument("--all", action="store_true", help="Run all queries")
    args = parser.parse_args()

    selected = sum(bool(value) for value in [args.query, args.neighbourhoods, args.cuisines, args.all])
    if selected != 1:
        parser.error("Use exactly one of --query, --neighbourhoods, --cuisines, or --all")

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


def run_search(client: ApiClient, query: str) -> list[dict[str, Any]]:
    results = client.google_maps_search(
        query,
        limit=100,
        language="en",
        region="DE",
        fields=PROFILE_FIELDS,
    )
    return flatten_results(results)


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


def selected_queries(args: argparse.Namespace) -> list[str]:
    if args.query:
        return [args.query]
    if args.neighbourhoods:
        return NEIGHBOURHOOD_QUERIES
    if args.cuisines:
        return CUISINE_QUERIES
    return NEIGHBOURHOOD_QUERIES + CUISINE_QUERIES


def main() -> int:
    args = parse_args()

    try:
        outscraper_key, supabase_url, supabase_key = load_environment()
    except RuntimeError as error:
        print(f"❌ {error}")
        return 1

    client = ApiClient(api_key=outscraper_key)
    supabase = create_client(supabase_url, supabase_key)
    queries = selected_queries(args)

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

            for restaurant in restaurants:
                place_id = restaurant.get("place_id")
                if not place_id:
                    continue

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
