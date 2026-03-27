#!/usr/bin/env python3

import argparse
import os
import sys
import time
from typing import Any

import requests
from dotenv import load_dotenv
from supabase import create_client


def load_environment() -> tuple[str, str, str]:
    load_dotenv(".env.local")
    load_dotenv(".env")

    api_key = os.getenv("OUTSCRAPER_API_KEY")
    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

    missing = []
    if not api_key:
        missing.append("OUTSCRAPER_API_KEY")
    if not supabase_url:
        missing.append("SUPABASE_URL")
    if not supabase_key:
        missing.append("SUPABASE_KEY")

    if missing:
      raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return api_key, supabase_url, supabase_key


OUTSCRAPER_URL = "https://api.app.outscraper.com/emails-and-contacts"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape restaurant emails and contact details")
    parser.add_argument("--slug", help="Only process one restaurant slug")
    parser.add_argument("--all", action="store_true", help="Process all restaurants with a website")
    args = parser.parse_args()

    if bool(args.slug) == bool(args.all):
        parser.error("Use exactly one of --slug or --all")

    return args


def fetch_restaurants(supabase: Any, slug: str | None) -> list[dict[str, Any]]:
    query = (
        supabase.table("silver_restaurants")
        .select("raw_ingestion_id, slug, website")
        .neq("website", "null")
    )

    if slug:
        query = query.eq("slug", slug)

    result = query.execute()
    rows = result.data or []

    return [row for row in rows if row.get("website")]


def fetch_contacts(api_key: str, website: str) -> tuple[dict[str, Any] | None, Any]:
    response = requests.get(
        OUTSCRAPER_URL,
        params={"query": website, "async": "false"},
        headers={"X-API-KEY": api_key},
        timeout=60,
    )

    if response.status_code != 200:
        raise RuntimeError(f"API error {response.status_code}: {response.text[:200]}")

    payload = response.json()
    data = payload.get("data") or []
    first = data[0] if data else None
    return first, payload


def first_or_none(values: Any) -> Any:
    if isinstance(values, list) and values:
        return values[0]
    return None


def build_upsert_row(row: dict[str, Any], contact_data: dict[str, Any] | None, raw_response: Any) -> dict[str, Any]:
    return {
        "raw_ingestion_id": row.get("raw_ingestion_id"),
        "slug": row.get("slug"),
        "website": row.get("website"),
        "email": None,
        "phone": None,
        "instagram": None,
        "facebook": None,
        "tiktok": None,
        "twitter": None,
        "linkedin": None,
        "raw_response": raw_response,
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def format_summary(row: dict[str, Any]) -> str:
    return f"website: {row.get('website') or 'null'} | raw_response: saved"


def main() -> int:
    args = parse_args()

    try:
        api_key, supabase_url, supabase_key = load_environment()
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
        slug = restaurant["slug"]
        website = restaurant["website"]

        try:
            contact_data, raw_response = fetch_contacts(api_key, website)
            if not contact_data:
                print(f"⚠️ {slug} → no contacts found")

            upsert_row = build_upsert_row(restaurant, contact_data, raw_response)
            result = (
                supabase.table("contact_enrichments")
                .upsert(upsert_row, on_conflict="slug")
                .execute()
            )

            if getattr(result, "error", None):
                raise RuntimeError(result.error.message)

            scraped += 1
            print(f"✓ {slug} → {format_summary(upsert_row)}")
        except Exception as error:
            errors += 1
            print(f"✗ {slug} → {error}")

        if index < len(restaurants) - 1:
            time.sleep(2)

    print("---")
    print(f"Done: {scraped}/{len(restaurants)} scraped, {errors} errors")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
