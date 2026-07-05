"""
Single source of truth for "what do we have on this restaurant, and how fresh is it."

Wolt and Trends are intentionally excluded — Wolt is a city-wide bulk fetch with no
per-restaurant API filter, and Trends is scoped to a cuisine/label query, not a
restaurant_id, so neither fits a per-restaurant status check.
"""

from typing import Any

from repository.db import get_client


def _latest(table: str, column: str, value: str, order_by: str) -> str | None:
    supabase = get_client()
    result = (
        supabase.table(table)
        .select(order_by)
        .eq(column, value)
        .order(order_by, desc=True)
        .limit(1)
        .execute()
    )
    rows = result.data or []
    return rows[0][order_by] if rows else None


def get_enrichment_status(restaurant: dict[str, Any]) -> dict[str, Any]:
    """restaurant = a row from silver_restaurants (id, slug, place_id, name)."""
    supabase = get_client()
    place_id = restaurant["place_id"]
    slug = restaurant["slug"]
    restaurant_uuid = restaurant["id"]

    handles_result = (
        supabase.table("restaurant_social_handles")
        .select("ig_handle, tiktok_handle")
        .eq("restaurant_id", place_id)
        .execute()
    )
    handles = (handles_result.data or [{}])[0]

    serp_result = (
        supabase.table("bronze_serp")
        .select("fetched_at")
        .eq("place_id", place_id)
        .eq("query_type", "brand")
        .order("fetched_at", desc=True)
        .limit(1)
        .execute()
    )
    serp_rows = serp_result.data or []

    return {
        "has_ig_handle": bool(handles.get("ig_handle")),
        "ig_last_fetched": _latest("bronze_ig_profiles", "restaurant_id", place_id, "fetched_at"),
        "has_tiktok_handle": bool(handles.get("tiktok_handle")),
        "tiktok_last_fetched": _latest("bronze_tiktok_profiles", "restaurant_id", place_id, "fetched_at"),
        "reviews_last_scraped": _latest("reviews", "restaurant_id", restaurant_uuid, "scraped_at"),
        "contacts_last_scraped": _latest("contact_enrichments", "slug", slug, "scraped_at"),
        "tripadvisor_last_checked": _latest("tripadvisor_enrichments", "slug", slug, "scraped_at"),
        "serp_last_checked": serp_rows[0]["fetched_at"] if serp_rows else None,
    }
