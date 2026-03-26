"""
Extract structured labels from raw_ingestions into silver_labels (tall format).
Enables clustering, eigenvector analysis, and neighbourhood/cuisine filtering.

Required env vars:
    SUPABASE_URL
    SUPABASE_KEY  (service role key)

Usage:
    python scripts/extract_silver_labels.py
"""

import re
import os
import sys
from collections import defaultdict
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(".env.local")

SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set.")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BATCH_SIZE = 500


def extract_labels(row: dict) -> list[tuple[str, str, str]]:
    """Return list of (label_type, label_value, source) tuples for a row."""
    labels = []

    raw_data = row.get("raw_data")
    query = row.get("query", "")

    # Parse raw_data safely
    try:
        data = raw_data["data"][0] if raw_data and "data" in raw_data else {}
    except (KeyError, IndexError, TypeError):
        data = {}

    # --- cuisine (from outscraper type field) ---
    primary_type = data.get("type")
    if primary_type:
        cuisine = (
            primary_type
            .replace(" restaurant", "")
            .replace(" Restaurant", "")
            .strip()
        )
        if cuisine:
            labels.append(("cuisine", cuisine, "outscraper_type"))

    # --- cuisine_sub (from outscraper subtypes field) ---
    subtypes_str = data.get("subtypes", "")
    if subtypes_str:
        for subtype in subtypes_str.split(","):
            subtype = subtype.strip()
            if subtype:
                labels.append(("cuisine_sub", subtype, "outscraper_subtypes"))

    # --- neighbourhood (from query: "restaurants in X Berlin") ---
    match = re.search(r'restaurants in (.+?) Berlin', query)
    if match:
        neighbourhood = match.group(1).strip()
        if neighbourhood:
            labels.append(("neighbourhood", neighbourhood, "query"))

    # --- cuisine_query (from query: "X restaurant Berlin") ---
    match = re.search(r'^(.+?) restaurant Berlin', query, re.IGNORECASE)
    if match:
        cuisine_q = match.group(1).strip()
        if cuisine_q:
            labels.append(("cuisine_query", cuisine_q, "query"))

    # --- price_tier (from outscraper range field) ---
    price_range = data.get("range")
    if price_range:
        labels.append(("price_tier", price_range, "outscraper_range"))

    # --- about-based labels ---
    about = data.get("about", {}) or {}

    about_sections = {
        "crowd":         ("Crowd",          "outscraper_about"),
        "service":       ("Service options", "outscraper_about"),
        "atmosphere":    ("Atmosphere",      "outscraper_about"),
        "offering":      ("Offerings",       "outscraper_about"),
        "accessibility": ("Accessibility",   "outscraper_about"),
        "planning":      ("Planning",        "outscraper_about"),
    }

    for label_type, (section_key, source) in about_sections.items():
        section = about.get(section_key, {}) or {}
        for attribute, value in section.items():
            if value is True:
                labels.append((label_type, attribute, source))

    return labels


def fetch_all_rows() -> list[dict]:
    """Paginate through raw_ingestions, returning all matching rows."""
    page_size = 1000
    offset = 0
    all_rows = []

    while True:
        result = (
            supabase.table("raw_ingestions")
            .select("id, query, place_id, raw_data")
            .eq("source", "outscraper")
            .not_.is_("place_id", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        page = result.data or []
        all_rows.extend(page)
        print(f"  Fetched {len(all_rows)} records so far...")
        if len(page) < page_size:
            break
        offset += page_size

    return all_rows


def main():
    print("Fetching raw_ingestions...")
    data_rows = fetch_all_rows()
    print(f"Found {len(data_rows)} ingestion records.")

    all_records = []
    type_counts = defaultdict(int)
    processed = 0
    skipped = 0

    for row in data_rows:
        place_id = row.get("place_id")
        if not place_id:
            skipped += 1
            continue

        try:
            labels = extract_labels(row)
        except Exception as e:
            print(f"  WARN: Failed to extract labels for place_id={place_id}: {e}")
            skipped += 1
            continue

        for label_type, label_value, source in labels:
            all_records.append({
                "place_id": place_id,
                "label_type": label_type,
                "label_value": label_value,
                "source": source,
            })
            type_counts[label_type] += 1

        processed += 1

    print(f"Processed {processed} restaurants ({skipped} skipped).")
    print(f"Upserting {len(all_records)} label records in batches of {BATCH_SIZE}...")

    upserted = 0
    for i in range(0, len(all_records), BATCH_SIZE):
        batch = all_records[i : i + BATCH_SIZE]
        try:
            supabase.table("silver_labels").upsert(
                batch,
                on_conflict="place_id,label_type,label_value"
            ).execute()
            upserted += len(batch)
            print(f"  Upserted {upserted}/{len(all_records)}...")
        except Exception as e:
            print(f"  ERROR on batch {i}–{i+BATCH_SIZE}: {e}")

    print()
    print(f"✓ Processed {processed} restaurants")
    print(f"✓ Labels inserted/updated: {upserted:,}")
    for label_type, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {label_type:<16} {count:>6,}")


if __name__ == "__main__":
    main()
