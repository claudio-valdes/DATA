#!/usr/bin/env python3
"""
Compute sales_score for all restaurants in restaurant_pipeline.

Score components (0-1 each):
  - Social prominence  35%  mention count + plays from silver_social_mentions
  - Google prominence  20%  score_band from restaurant_pipeline
  - Reachability       25%  email + phone from contact_enrichments
  - Social handles     10%  ig + tiktok from restaurant_social_handles
  - Quality            10%  rating from silver_restaurants

Usage:
    python -m entrypoints.score_sales_pipeline
"""

import math
import sys
from datetime import datetime, timezone

from repository.db import fetch_all, get_client, paginate

SCORE_BAND_MAP = {
    "tier_1": 1.0,
    "tier_2": 0.83,
    "tier_3": 0.67,
    "tier_4": 0.50,
    "tier_5": 0.33,
    "tier_6": 0.0,
}

WEIGHTS = {
    "social":     0.35,
    "google":     0.20,
    "reachable":  0.25,
    "handles":    0.10,
    "quality":    0.10,
}


def log_norm(value: float, scale: float) -> float:
    """Normalise a value using log scale. scale = value that maps to ~1.0."""
    if not value or value <= 0:
        return 0.0
    return min(math.log1p(value) / math.log1p(scale), 1.0)


def social_score(mentions: int, total_plays: int) -> float:
    mention_norm = log_norm(mentions, 20)
    plays_norm = log_norm(total_plays, 500_000)
    return (mention_norm * 0.5) + (plays_norm * 0.5)


def google_score(score_band: str | None) -> float:
    return SCORE_BAND_MAP.get(score_band or "", 0.0)


def reachability_score(has_email: bool, has_phone: bool) -> float:
    if has_email and has_phone:
        return 1.0
    if has_email or has_phone:
        return 0.6
    return 0.0


def handles_score(has_ig: bool, has_tiktok: bool) -> float:
    return (0.5 if has_ig else 0.0) + (0.5 if has_tiktok else 0.0)


def quality_score(rating: float | None) -> float:
    if not rating:
        return 0.0
    return max(0.0, min((rating - 3.5) / 1.5, 1.0))


def compute_sales_score(
    mentions: int,
    total_plays: int,
    score_band: str | None,
    has_email: bool,
    has_phone: bool,
    has_ig: bool,
    has_tiktok: bool,
    rating: float | None,
) -> float:
    components = {
        "social":    social_score(mentions, total_plays),
        "google":    google_score(score_band),
        "reachable": reachability_score(has_email, has_phone),
        "handles":   handles_score(has_ig, has_tiktok),
        "quality":   quality_score(rating),
    }
    score = sum(components[k] * WEIGHTS[k] for k in components)
    return round(score, 4)


def score_one(place_id: str) -> dict:
    """Recompute sales_score for a single restaurant. Returns the new score."""
    supabase = get_client()

    pipeline_result = supabase.table("restaurant_pipeline").select("score_band").eq("place_id", place_id).execute()
    pipeline_row = (pipeline_result.data or [{}])[0]

    restaurant_result = supabase.table("silver_restaurants").select("rating").eq("place_id", place_id).execute()
    restaurant_row = (restaurant_result.data or [{}])[0]

    contact_result = supabase.table("contact_enrichments").select("email, phone").eq("place_id", place_id).execute()
    contact_row = (contact_result.data or [{}])[0]

    handles_result = (
        supabase.table("restaurant_social_handles")
        .select("ig_handle, tiktok_handle")
        .eq("restaurant_id", place_id)
        .execute()
    )
    handles_row = (handles_result.data or [{}])[0]

    mentions_result = (
        supabase.table("silver_social_mentions")
        .select("post_plays")
        .eq("place_id", place_id)
        .eq("is_restaurant_mention", True)
        .execute()
    )
    mention_rows = mentions_result.data or []

    score = compute_sales_score(
        mentions=len(mention_rows),
        total_plays=sum(m.get("post_plays") or 0 for m in mention_rows),
        score_band=pipeline_row.get("score_band"),
        has_email=bool(contact_row.get("email")),
        has_phone=bool(contact_row.get("phone")),
        has_ig=bool(handles_row.get("ig_handle")),
        has_tiktok=bool(handles_row.get("tiktok_handle")),
        rating=restaurant_row.get("rating"),
    )

    now = datetime.now(timezone.utc).isoformat()
    supabase.table("restaurant_pipeline").update({
        "sales_score": score,
        "sales_score_updated_at": now,
    }).eq("place_id", place_id).execute()

    print(f"✓ {place_id} → sales_score: {score}")
    return {"place_id": place_id, "sales_score": score}


def score_all() -> dict:
    """Recompute sales_score for every restaurant in restaurant_pipeline. Returns summary counts."""
    supabase = get_client()

    print("Fetching data...")

    pipeline_rows = fetch_all("restaurant_pipeline", "place_id, score_band")
    restaurants = fetch_all("silver_restaurants", "place_id, rating")
    contacts = fetch_all("contact_enrichments", "place_id, email, phone")
    handles = fetch_all("restaurant_social_handles", "restaurant_id, ig_handle, tiktok_handle")
    mentions = paginate(
        lambda sb: sb.table("silver_social_mentions")
        .select("place_id, post_plays, post_likes")
        .eq("is_restaurant_mention", True)
        .not_.is_("place_id", "null")
    )

    # Build lookup maps
    rating_map: dict[str, float] = {r["place_id"]: r["rating"] for r in restaurants if r.get("place_id")}

    contact_map: dict[str, dict] = {}
    for c in contacts:
        pid = c.get("place_id")
        if pid:
            contact_map[pid] = {
                "has_email": bool(c.get("email")),
                "has_phone": bool(c.get("phone")),
            }

    handle_map: dict[str, dict] = {}
    for h in handles:
        pid = h.get("restaurant_id")
        if pid:
            handle_map[pid] = {
                "has_ig": bool(h.get("ig_handle")),
                "has_tiktok": bool(h.get("tiktok_handle")),
            }

    mention_map: dict[str, dict] = {}
    for m in mentions:
        pid = m.get("place_id")
        if not pid:
            continue
        entry = mention_map.setdefault(pid, {"count": 0, "total_plays": 0})
        entry["count"] += 1
        entry["total_plays"] += m.get("post_plays") or 0

    print(f"Scoring {len(pipeline_rows)} restaurants...")

    updated = 0
    now = datetime.now(timezone.utc).isoformat()

    for row in pipeline_rows:
        place_id = row["place_id"]
        m = mention_map.get(place_id, {})
        c = contact_map.get(place_id, {})
        h = handle_map.get(place_id, {})

        score = compute_sales_score(
            mentions=m.get("count", 0),
            total_plays=m.get("total_plays", 0),
            score_band=row.get("score_band"),
            has_email=c.get("has_email", False),
            has_phone=c.get("has_phone", False),
            has_ig=h.get("has_ig", False),
            has_tiktok=h.get("has_tiktok", False),
            rating=rating_map.get(place_id),
        )

        supabase.table("restaurant_pipeline").update({
            "sales_score": score,
            "sales_score_updated_at": now,
        }).eq("place_id", place_id).execute()
        updated += 1

    print("---")
    print(f"Scored: {updated} restaurants")
    return {"scored": updated}


def main() -> int:
    try:
        score_all()
    except RuntimeError as error:
        print(f"❌ {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
