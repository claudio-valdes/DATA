#!/usr/bin/env python3

import os
import time
from datetime import date, timedelta
from typing import Any

import requests

from repository.db import get_client, paginate

OUTSCRAPER_URL = "https://api.app.outscraper.com/emails-and-contacts"

# Domains that are not restaurant websites — scraping them returns wrong or useless contact data.
# instagram.com / facebook.com: the restaurant's "website" IS their social page — handle is
# extracted directly from the URL in the SQL backfill step rather than via API.
# speisekartenweb.de: aggregator that hosts menu pages for many restaurants.
SKIP_DOMAINS = [
    "instagram.com",
    "facebook.com",
    "speisekartenweb.de",
]


def fetch_restaurants(supabase: Any, slug: str | None) -> list[dict[str, Any]]:
    if slug:
        result = (
            supabase.table("silver_restaurants")
            .select("raw_ingestion_id, slug, place_id, website")
            .eq("slug", slug)
            .execute()
        )
        rows = result.data or []
        return [row for row in rows if row.get("website")]

    rows = paginate(
        lambda sb: sb.table("silver_restaurants").select("raw_ingestion_id, slug, place_id, website").neq("website", "null")
    )
    return [
        row for row in rows
        if row.get("website") and not any(d in row["website"] for d in SKIP_DOMAINS)
    ]


def fetch_recently_scraped(supabase: Any) -> set[str]:
    cutoff = (date.today() - timedelta(days=30)).isoformat()
    rows = paginate(lambda sb: sb.table("contact_enrichments").select("place_id").gte("scrape_date", cutoff))
    return {row["place_id"] for row in rows if row.get("place_id")}


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
    emails = (contact_data or {}).get("emails") or []
    phones = (contact_data or {}).get("phones") or []
    socials = (contact_data or {}).get("socials") or {}

    return {
        "raw_ingestion_id": row.get("raw_ingestion_id"),
        "slug": row.get("slug"),
        "place_id": row.get("place_id"),
        "website": row.get("website"),
        "email": first_or_none([e.get("value") for e in emails if e.get("value")]),
        "phone": first_or_none([p.get("value") for p in phones if p.get("value")]),
        "instagram": socials.get("instagram"),
        "facebook": socials.get("facebook"),
        "tiktok": socials.get("tiktok"),
        "twitter": socials.get("twitter"),
        "linkedin": socials.get("linkedin"),
        "raw_response": raw_response,
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "scrape_date": date.today().isoformat(),
    }


def format_summary(row: dict[str, Any]) -> str:
    return f"website: {row.get('website') or 'null'} | raw_response: saved"


def scrape_contacts(slug: str | None = None) -> dict:
    """Scrape contact details for one restaurant (slug) or all restaurants with a website (slug=None)."""
    api_key = os.environ["OUTSCRAPER_API_KEY"]
    supabase = get_client()

    restaurants = fetch_restaurants(supabase, slug)

    if not restaurants:
        target = slug or "all restaurants"
        print(f"⚠️ No restaurants found for {target}")
        return {"scraped": 0, "errors": 0}

    if slug is None:
        recently_scraped = fetch_recently_scraped(supabase)
        before = len(restaurants)
        restaurants = [r for r in restaurants if r.get("place_id") not in recently_scraped]
        skipped = before - len(restaurants)
        if skipped:
            print(f"⏭️  Skipping {skipped} restaurants scraped within the last 30 days")

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
    return {"scraped": scraped, "errors": errors}
