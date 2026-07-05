#!/usr/bin/env python3
"""
Embed silver_restaurants using Voyage AI and store vectors in silver_restaurants.embedding.
Skips restaurants that already have an embedding unless --force is passed.

Usage:
    python entrypoints/embed_silver_restaurants.py
    python entrypoints/embed_silver_restaurants.py --force
"""

import argparse
import os
import sys
from typing import Any

import voyageai

from repository.db import get_client, paginate

EMBEDDING_MODEL = "voyage-3-lite"
BATCH_SIZE = 128


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Embed silver_restaurants with Voyage AI")
    parser.add_argument("--force", action="store_true", help="Re-embed all, including those with existing embeddings")
    return parser.parse_args()


def fetch_restaurants(supabase: Any, force: bool) -> list[dict[str, Any]]:
    def build(sb):
        query = sb.table("silver_restaurants").select("place_id, name")
        if not force:
            query = query.is_("embedding", "null")
        return query

    rows = paginate(build)
    return [row for row in rows if row.get("name")]


def build_embedding_text(restaurant: dict[str, Any]) -> str:
    return f"Restaurant: {restaurant['name']}, Berlin, Germany"


def main() -> int:
    args = parse_args()

    try:
        voyage_key = os.environ["VOYAGE_KEY"]
        supabase = get_client()
    except (KeyError, RuntimeError) as error:
        print(f"❌ {error}")
        return 1

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
