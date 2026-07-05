#!/usr/bin/env python3
"""
Enrich a single restaurant end-to-end: check what's stale or missing, run only
the channels that need it, and rescore.

Usage:
    python -m entrypoints.target --slug wen-cheng
    python -m entrypoints.target --slug wen-cheng --for-report
"""

import argparse
import sys

from entrypoints import score_sales_pipeline
from repository.db import get_client
from services.competitors.identify_competitors import identify_competitors
from services.state.enrichment_planner import plan_enrichment
from services.state.restaurant_state import get_enrichment_status
from services.state.service_map import SERVICE_MAP


def _resolve_restaurant(slug: str) -> dict:
    supabase = get_client()
    result = (
        supabase.table("silver_restaurants")
        .select("id, slug, place_id, name")
        .eq("slug", slug)
        .single()
        .execute()
    )
    if not result.data:
        raise RuntimeError(f"No restaurant found with slug: {slug}")
    return result.data


def _resolve_restaurant_by_place_id(place_id: str) -> dict:
    supabase = get_client()
    result = (
        supabase.table("silver_restaurants")
        .select("id, slug, place_id, name")
        .eq("place_id", place_id)
        .single()
        .execute()
    )
    if not result.data:
        raise RuntimeError(f"No restaurant found with place_id: {place_id}")
    return result.data


def _run_enrichment_plan(restaurant: dict, mode: str) -> list[str]:
    """Shared by target() and refresh(): status -> plan -> dispatch via SERVICE_MAP."""
    status = get_enrichment_status(restaurant)
    plan = plan_enrichment(status, mode=mode)

    if not plan:
        print(f"  {restaurant['slug']} → nothing stale, skipping")
        return plan

    print(f"  {restaurant['slug']} → running: {', '.join(plan)}")
    for step in plan:
        SERVICE_MAP[step](restaurant)

    return plan


def target(slug: str, for_report: bool = False) -> dict:
    restaurant = _resolve_restaurant(slug)
    mode = "report" if for_report else "normal"
    plan = _run_enrichment_plan(restaurant, mode)

    if for_report:
        identify_competitors(restaurant)

    result = score_sales_pipeline.score_one(restaurant["place_id"])
    return {"slug": slug, "ran": plan, **result}


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich a single restaurant end-to-end")
    parser.add_argument("--slug", required=True)
    parser.add_argument("--for-report", action="store_true")
    args = parser.parse_args()

    try:
        target(args.slug, for_report=args.for_report)
    except (KeyError, RuntimeError) as error:
        print(f"❌ {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
