#!/usr/bin/env python3

import argparse
import os
import sys
import time
from statistics import mean
from typing import Any

from dotenv import load_dotenv
from serpapi import GoogleSearch
from supabase import create_client


CUISINE_QUERIES = [
    "vietnamesisches Restaurant",
    "chinesisches Restaurant",
    "italienisches Restaurant",
    "türkisches Restaurant",
    "japanisches Restaurant",
    "koreanisches Restaurant",
    "Thai Restaurant",
    "indisches Restaurant",
    "libanesisches Restaurant",
    "mexikanisches Restaurant",
]

GEO = "DE"
PERIOD = "today 12-m"
TZ = "-60"


def load_environment() -> tuple[str, str, str]:
    load_dotenv(".env.local")
    load_dotenv(".env")

    serpapi_key = os.getenv("SERPAPI_KEY")
    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

    missing = []
    if not serpapi_key:
        missing.append("SERPAPI_KEY")
    if not supabase_url:
        missing.append("SUPABASE_URL")
    if not supabase_key:
        missing.append("SUPABASE_KEY")

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return serpapi_key, supabase_url, supabase_key


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape cuisine-level Google Trends data for Germany using cuisine-category queries")
    parser.add_argument("--query", help="Run a single cuisine query")
    parser.add_argument("--all", action="store_true", help="Run all cuisine queries")
    args = parser.parse_args()

    if bool(args.query) == bool(args.all):
        parser.error("Use exactly one of --query or --all")

    return args


def fetch_trends(serpapi_key: str, query: str) -> dict[str, Any]:
    params = {
        "engine": "google_trends",
        "q": query,
        "geo": GEO,
        "date": PERIOD,
        "data_type": "TIMESERIES",
        "tz": TZ,
        "api_key": serpapi_key,
    }

    search = GoogleSearch(params)
    return search.get_dict()


def parse_timeline(results: dict[str, Any], query: str) -> tuple[list[dict[str, Any]], int | None]:
    interest = results.get("interest_over_time")
    if not interest:
        raise ValueError(f"no data returned for {query}")

    timeline = interest.get("timeline_data") or []
    if not timeline:
        raise ValueError(f"no data returned for {query}")

    parsed = []
    for item in timeline:
        values = item.get("values") or []
        if not values:
            continue

        first_value = values[0]
        extracted_value = first_value.get("extracted_value")
        parsed.append(
            {
                "date": item.get("date"),
                "timestamp": item.get("timestamp"),
                "value": extracted_value,
                "query": first_value.get("query", query),
            }
        )

    if not parsed:
        raise ValueError(f"no timeline points returned for {query}")

    if all((point.get("value") or 0) == 0 for point in parsed):
        raise ValueError(f"query volume too low for {query}")

    averages = interest.get("averages") or []
    average_interest = None
    if averages:
        average_interest = averages[0].get("value")

    return parsed, average_interest


def derive_trend(timeline: list[dict[str, Any]]) -> tuple[float, str]:
    midpoint = len(timeline) // 2
    first_half = [point["value"] for point in timeline[:midpoint] if point.get("value") is not None]
    second_half = [point["value"] for point in timeline[midpoint:] if point.get("value") is not None]

    if not first_half or not second_half:
        return 0.0, "stable"

    first_half_avg = mean(first_half)
    second_half_avg = mean(second_half)

    if first_half_avg == 0:
        trend_pct = 0.0 if second_half_avg == 0 else 100.0
    else:
        trend_pct = ((second_half_avg - first_half_avg) / first_half_avg) * 100

    if trend_pct > 10:
        direction = "rising"
    elif trend_pct < -10:
        direction = "declining"
    else:
        direction = "stable"

    return trend_pct, direction


def peak_point(timeline: list[dict[str, Any]]) -> tuple[str | None, int | None]:
    points = [point for point in timeline if point.get("value") is not None]
    if not points:
        return None, None

    peak = max(points, key=lambda point: point["value"])
    return peak.get("date"), peak.get("value")


def upsert_trends(supabase: Any, query: str, average_interest: int | None, trend_pct: float, trend_direction: str, timeline: list[dict[str, Any]]) -> None:
    peak_date, peak_value = peak_point(timeline)
    row = {
        "query": query,
        "geo": GEO,
        "period": PERIOD,
        "average_interest": average_interest,
        "trend_direction": trend_direction,
        "trend_pct": round(trend_pct, 1),
        "peak_date": peak_date,
        "peak_value": peak_value,
        "timeline_data": timeline,
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    result = (
        supabase.table("trends_data")
        .upsert(row, on_conflict="query,geo,period")
        .execute()
    )

    if getattr(result, "error", None):
        raise RuntimeError(result.error.message)


def format_summary(query: str, average_interest: int | None, trend_pct: float, direction: str) -> str:
    icon = "📈" if direction == "rising" else "📉" if direction == "declining" else "→"
    avg = average_interest if average_interest is not None else "null"
    sign = "+" if trend_pct > 0 else ""
    return f"✓ {query} → avg: {avg}, trend: {sign}{round(trend_pct, 1)}% ({direction}) {icon}"


def main() -> int:
    args = parse_args()

    try:
        serpapi_key, supabase_url, supabase_key = load_environment()
    except RuntimeError as error:
        print(f"❌ {error}")
        return 1

    supabase = create_client(supabase_url, supabase_key)
    queries = [args.query] if args.query else CUISINE_QUERIES

    scraped = 0

    for index, query in enumerate(queries):
        try:
            results = fetch_trends(serpapi_key, query)
            timeline, average_interest = parse_timeline(results, query)
            trend_pct, trend_direction = derive_trend(timeline)
            upsert_trends(supabase, query, average_interest, trend_pct, trend_direction, timeline)
            print(format_summary(query, average_interest, trend_pct, trend_direction))
            scraped += 1
        except Exception as error:
            print(f"✗ {query} → {error}")

        if index < len(queries) - 1:
            time.sleep(2)

    print("---")
    print(f"Done: {scraped}/{len(queries)} scraped")
    return 0 if scraped == len(queries) else 1


if __name__ == "__main__":
    sys.exit(main())
