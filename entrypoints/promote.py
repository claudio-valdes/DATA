#!/usr/bin/env python3
"""
Promote restaurants to Tier 3 (sales targets) and run their target() enrichment.

Two modes:
  - force_slug: manual override, bypasses all criteria (replaces the old
    promote_target.py --slug behaviour, including --notes).
  - default criteria: query_appearances >= 3, rating >= 4.2, review_count >= 50,
    tier < 3. Override individual thresholds via `criteria={...}`.

Usage:
    python -m entrypoints.promote --force-slug wen-cheng --notes "manual pick"
    python -m entrypoints.promote
    python -m entrypoints.promote --limit 10 --for-report
"""

import argparse
import sys

from entrypoints.target import _resolve_restaurant, target
from repository.db import get_client, paginate

DEFAULT_CRITERIA = {
    "min_query_appearances": 3,
    "min_rating": 4.2,
    "min_review_count": 50,
}


def _set_tier(place_id: str, tier: int, tier_source: str, notes: str = "") -> None:
    supabase = get_client()
    supabase.table("restaurant_pipeline").upsert({
        "place_id": place_id,
        "tier": tier,
        "tier_source": tier_source,
        "notes": notes,
    }, on_conflict="place_id").execute()


def _find_candidates(criteria: dict, limit: int | None) -> list[dict]:
    supabase = get_client()

    pipeline_rows = paginate(lambda sb: sb.table("restaurant_pipeline").select("place_id, tier, query_appearances"))
    eligible_pipeline = {
        row["place_id"]: row
        for row in pipeline_rows
        if (row.get("tier") or 1) < 3 and (row.get("query_appearances") or 0) >= criteria["min_query_appearances"]
    }

    if not eligible_pipeline:
        return []

    restaurant_rows = paginate(lambda sb: sb.table("silver_restaurants").select("place_id, slug, name, rating, review_count"))
    candidates = [
        {**r, "query_appearances": eligible_pipeline[r["place_id"]]["query_appearances"]}
        for r in restaurant_rows
        if r["place_id"] in eligible_pipeline
        and (r.get("rating") or 0) >= criteria["min_rating"]
        and (r.get("review_count") or 0) >= criteria["min_review_count"]
    ]

    candidates.sort(key=lambda r: r["query_appearances"], reverse=True)
    return candidates[:limit] if limit else candidates


def promote(
    criteria: dict | None = None,
    limit: int | None = None,
    for_report: bool = False,
    force_slug: str | None = None,
    notes: str = "",
) -> dict:
    if force_slug:
        restaurant = _resolve_restaurant(force_slug)
        _set_tier(restaurant["place_id"], tier=3, tier_source="manual", notes=notes)
        print(f"✓ {restaurant['name']} promoted to Tier 3 (manual)")
        return {"promoted": [force_slug], "result": target(force_slug, for_report=for_report)}

    merged_criteria = {**DEFAULT_CRITERIA, **(criteria or {})}
    candidates = _find_candidates(merged_criteria, limit)
    print(f"Found {len(candidates)} restaurants meeting promotion criteria")

    promoted = []
    for restaurant in candidates:
        _set_tier(restaurant["place_id"], tier=3, tier_source="auto_criteria")
        print(f"✓ {restaurant['name']} ({restaurant['slug']}) promoted to Tier 3 — {restaurant['query_appearances']} appearances")
        target(restaurant["slug"], for_report=for_report)
        promoted.append(restaurant["slug"])

    return {"promoted": promoted}


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote restaurants to Tier 3 and enrich them")
    parser.add_argument("--force-slug", dest="force_slug", help="Manually promote one restaurant, bypassing criteria")
    parser.add_argument("--notes", default="", help="Notes for the manual override (used with --force-slug)")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--for-report", action="store_true", dest="for_report")
    parser.add_argument("--min-query-appearances", type=int, dest="min_query_appearances")
    parser.add_argument("--min-rating", type=float, dest="min_rating")
    parser.add_argument("--min-review-count", type=int, dest="min_review_count")
    args = parser.parse_args()

    criteria = {
        k: v
        for k, v in {
            "min_query_appearances": args.min_query_appearances,
            "min_rating": args.min_rating,
            "min_review_count": args.min_review_count,
        }.items()
        if v is not None
    }

    try:
        promote(
            criteria=criteria or None,
            limit=args.limit,
            for_report=args.for_report,
            force_slug=args.force_slug,
            notes=args.notes,
        )
    except (KeyError, RuntimeError) as error:
        print(f"❌ {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
