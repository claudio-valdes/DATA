#!/usr/bin/env python3

import argparse
import os
import sys

from dotenv import load_dotenv
from supabase import create_client


def load_environment():
    load_dotenv(".env.local")
    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("SERVICE_ROLE_KEY")
    missing = []
    if not supabase_url:
        missing.append("SUPABASE_URL")
    if not supabase_key:
        missing.append("SUPABASE_KEY")
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
    return supabase_url, supabase_key


def parse_args():
    parser = argparse.ArgumentParser(description="Promote a restaurant to Tier 3 (manual target)")
    parser.add_argument("--slug", required=True, help="Restaurant slug from silver_restaurants")
    parser.add_argument("--notes", default="", help="Reason for manual promotion")
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        supabase_url, supabase_key = load_environment()
    except RuntimeError as error:
        print(f"❌ {error}")
        return 1

    supabase = create_client(supabase_url, supabase_key)

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
