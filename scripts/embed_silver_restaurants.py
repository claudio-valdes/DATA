#!/usr/bin/env python3
"""
Embed silver_restaurants using Voyage AI and store vectors in silver_restaurants.embedding.
Skips restaurants that already have an embedding unless --force is passed.

Usage:
    python scripts/embed_silver_restaurants.py
    python scripts/embed_silver_restaurants.py --force
"""

import argparse
import os
import sys
from typing import Any

import voyageai
from dotenv import load_dotenv
from supabase import create_client


EMBEDDING_MODEL = "voyage-3-lite"
BATCH_SIZE = 128


def load_environment() -> tuple[str, str, str]:
    load_dotenv(".env.local")

    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("SERVICE_ROLE_KEY")
    voyage_key = os.getenv("VOYAGE_KEY")

    missing = []
    if not supabase_url:
        missing.append("SUPABASE_URL")
    if not supabase_key:
        missing.append("SUPABASE_KEY")
    if not voyage_key:
        missing.append("VOYAGE_KEY")

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return supabase_url, supabase_key, voyage_key


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Embed silver_restaurants with Voyage AI")
    parser.add_argument("--force", action="store_true", help="Re-embed all, including those with existing embeddings")
    return parser.parse_args()


def fetch_restaurants(supabase: Any, force: bool) -> list[dict[str, Any]]:
    rows = []
    page_size = 1000
    offset = 0

    while True:
        query = supabase.table("silver_restaurants").select("place_id, name")
        if not force:
            query = query.is_("embedding", "null")
        result = query.range(offset, offset + page_size - 1).execute()
        batch = result.data or []
        rows.extend(row for row in batch if row.get("name"))
        if len(batch) < page_size:
            break
        offset += page_size

    return rows


def build_embedding_text(restaurant: dict[str, Any]) -> str:
    return f"Restaurant: {restaurant['name']}, Berlin, Germany"


def main() -> int:
    args = parse_args()

    try:
        supabase_url, supabase_key, voyage_key = load_environment()
    except RuntimeError as error:
        print(f"❌ {error}")
        return 1

    supabase = create_client(supabase_url, supabase_key)
    voyage = voyageai.Client(api_key=voyage_key)

    restaurants = fetch_restaurants(supabase, args.force)
    print(f"Restaurants to embed: {len(restaurants)}")

    if not restaurants:
        print("Nothing to do.")
        return 0

    embedded = 0
    errors = 0

    for i in range(0, len(restaurants), BATCH_SIZE):
        batch = restaurants[i:i + BATCH_SIZE]
        texts = [build_embedding_text(r) for r in batch]

        try:
            result = voyage.embed(texts, model=EMBEDDING_MODEL, input_type="document")
            embeddings = result.embeddings
        except Exception as error:
            errors += len(batch)
            print(f"✗ Batch {i // BATCH_SIZE + 1} embedding failed: {error}")
            continue

        for restaurant, embedding in zip(batch, embeddings):
            try:
                supabase.table("silver_restaurants").update(
                    {"embedding": embedding}
                ).eq("place_id", restaurant["place_id"]).execute()
                embedded += 1
            except Exception as error:
                errors += 1
                print(f"✗ {restaurant['name']} → update failed: {error}")

        print(f"  {min(i + BATCH_SIZE, len(restaurants))}/{len(restaurants)} embedded")

    print("---")
    print(f"Embedded: {embedded} | Errors: {errors}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
