#!/usr/bin/env python3
"""
Restaurant discovery pipeline:
  mode="geo":    search Google Maps -> transform -> extract labels -> embed
  mode="social": scrape TikTok/Instagram -> extract mentions -> match -> enrich
                 unmatched -> transform (promotes new discoveries) -> re-link
  mode="all":    geo then social

Always ends with: recompute query_appearances, then rescore everyone.

Usage:
    python -m entrypoints.discover --mode geo
    python -m entrypoints.discover --mode social --platform tiktok
    python -m entrypoints.discover --mode all
"""

import argparse
import sys

from entrypoints import score_sales_pipeline
from services.discovery import (
    enrich_unmatched_mentions,
    extract_social_mentions,
    match_social_mentions,
    relink_new_discoveries,
    scrape_social_discovery,
)
from services.scraping import (
    embed_silver_restaurants,
    extract_silver_labels,
    scrape_bulk_restaurants,
    transform_silver_restaurants,
)
from services.state import query_appearances


def _discover_geo(query: str | None = None, neighbourhoods_only: bool = False, labels_only: bool = False, **_ignored) -> dict:
    scrape_bulk_restaurants.search_restaurants(query=query, neighbourhoods_only=neighbourhoods_only, labels_only=labels_only)
    transform_silver_restaurants.transform()
    extract_silver_labels.extract()
    embed_silver_restaurants.embed()
    return {"status": "done"}


def _discover_social(platform: str = "both", mention_limit: int = 500, **_ignored) -> dict:
    scrape_social_discovery.scrape(platform=platform)
    extract_social_mentions.extract_mentions(limit=mention_limit)
    match_social_mentions.match_mentions()
    enrich_unmatched_mentions.enrich()
    transform_silver_restaurants.transform()  # promotes any new_discovery rows added above
    relink_new_discoveries.relink()
    return {"status": "done"}


def discover(mode: str = "geo", **kwargs) -> dict:
    result: dict = {}

    if mode in ("geo", "all"):
        result["geo"] = _discover_geo(**kwargs)
    if mode in ("social", "all"):
        result["social"] = _discover_social(**kwargs)

    query_appearances.refresh_query_appearances()
    result["scoring"] = score_sales_pipeline.score_all()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the restaurant discovery pipeline")
    parser.add_argument("--mode", choices=["geo", "social", "all"], default="geo")
    parser.add_argument("--query", help="geo mode: run a single search query")
    parser.add_argument("--neighbourhoods-only", action="store_true", dest="neighbourhoods_only")
    parser.add_argument("--labels-only", action="store_true", dest="labels_only")
    parser.add_argument("--platform", choices=["tiktok", "instagram", "both"], default="both", help="social mode")
    parser.add_argument("--mention-limit", type=int, default=500, dest="mention_limit", help="social mode")
    args = parser.parse_args()

    try:
        discover(
            mode=args.mode,
            query=args.query,
            neighbourhoods_only=args.neighbourhoods_only,
            labels_only=args.labels_only,
            platform=args.platform,
            mention_limit=args.mention_limit,
        )
    except (KeyError, RuntimeError) as error:
        print(f"❌ {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
