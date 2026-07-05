#!/usr/bin/env python3

import argparse
import sys

from repository.db import get_client


def parse_args():
    parser = argparse.ArgumentParser(description="Promote a restaurant to Tier 3 (manual target)")
    parser.add_argument("--slug", required=True, help="Restaurant slug from silver_restaurants")
    parser.add_argument("--notes", default="", help="Reason for manual promotion")
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        supabase = get_client()
    except RuntimeError as error:
        print(f"❌ {error}")
        return 1

    # Look up place_id from slug
    result = (
        supabase.table("silver_restaurants")
        .select("place_id, name")
        .eq("slug", args.slug)
        .single()
        .execute()
    )

    if not result.data:
        print(f"❌ No restaurant found with slug: {args.slug}")
        return 1

    place_id = result.data["place_id"]
    name = result.data["name"]

    # Upsert into pipeline as tier 3
    supabase.table("restaurant_pipeline").upsert(
        {
            "place_id": place_id,
            "tier": 3,
            "tier_source": "manual",
            "notes": args.notes,
        },
        on_conflict="place_id"
    ).execute()

    print(f"✓ {name} promoted to Tier 3")
    if args.notes:
        print(f"  Notes: {args.notes}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
