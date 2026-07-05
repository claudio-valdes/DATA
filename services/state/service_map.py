"""
Single place mapping plan-step names (from enrichment_planner.plan_enrichment) to
the actual service calls that fulfil them, so target()/refresh() never hardcode
this routing themselves.

Each entry takes the resolved restaurant dict (id, slug, place_id, name from
silver_restaurants) and translates it to whichever identifier the underlying
service function actually expects.
"""

from services.instagram import fetch_ig_comments, fetch_ig_social
from services.scraping import scrape_emails_contacts, scrape_reviews, scrape_tripadvisor
from services.serp import fetch_serp_brand, match_serp
from services.tiktok import fetch_tiktok_comments, fetch_tiktok_social


def _run_serp(restaurant: dict) -> dict:
    fetch_serp_brand.run_brand_query_for_restaurant(restaurant["place_id"], restaurant["name"])
    return match_serp.match_signals(place_id=restaurant["place_id"])


SERVICE_MAP = {
    "ig_social": lambda restaurant: fetch_ig_social.fetch_social(slug=restaurant["slug"]),
    "ig_comments": lambda restaurant: fetch_ig_comments.fetch_comments(restaurant_id=restaurant["place_id"]),
    "tiktok_social": lambda restaurant: fetch_tiktok_social.fetch_social(slug=restaurant["slug"]),
    "tiktok_comments": lambda restaurant: fetch_tiktok_comments.fetch_comments(restaurant_id=restaurant["place_id"]),
    "serp": _run_serp,
    "reviews": lambda restaurant: scrape_reviews.scrape_reviews(slug=restaurant["slug"]),
    "contacts": lambda restaurant: scrape_emails_contacts.scrape_contacts(slug=restaurant["slug"]),
    "tripadvisor": lambda restaurant: scrape_tripadvisor.scrape_tripadvisor(slug=restaurant["slug"]),
}
