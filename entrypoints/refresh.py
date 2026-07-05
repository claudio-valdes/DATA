#!/usr/bin/env python3
"""
Refresh enrichment for every restaurant in restaurant_pipeline whose data is
stale, optionally filtered by tier. Reuses target()'s enrichment-plan dispatch —
only restaurants that actually need something get touched.

Usage:
    python -m entrypoints.refresh
    python -m entrypoints.refresh --tier 3 --tier 4
"""

import argparse
import sys

from entrypoints import score_sales_pipeline
from entrypoints.target import _resolve_restaurant_by_place_id, _run_enrichment_plan
from repository.db import get_client, paginate


def refresh(tier_filter: list[int] | None = None) -> dict:
    supabase = get_client()

    def build(sb):
        query = sb.table("restaurant_pipeline").select("place_id")
        if tier_filter:
            query = query.in_("tier", tier_filter)
        return query

    pipeline_rows = paginate(build)
    print(f"Checking {len(pipeline_rows)} restaurants for staleness...")

    refreshed = 0
    skipped = 0

    for row in pipeline_rows:
        try:
            restaurant = _resolve_restaurant_by_place_id(row["place_id"])
        except RuntimeError as error:
            print(f"  ⚠ {error}")
            continue

        plan = _run_enrichment_plan(restaurant, mode="normal")
        if plan:
            refreshed += 1
        else:
            skipped += 1

    print("---")
    print(f"Refreshed: {refreshed} | Up to date: {skipped}")
    score_sales_pipeline.score_all()
    return {"refreshed": refreshed, "skipped": skipped}


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh stale restaurant enrichment")
    parser.add_argument("--tier", type=int, action="append", dest="tier_filter", help="Repeatable; restricts to these tiers")
    args = parser.parse_args()

    try:
        refresh(tier_filter=args.tier_filter)
    except (KeyError, RuntimeError) as error:
        print(f"❌ {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
