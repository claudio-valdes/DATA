"""
Match silver_social_mentions against silver_restaurants using:
  1. Exact handle match  → restaurant_social_handles
  2. Exact name match    → silver_restaurants.name
  3. Vector shortlist + agent validation → silver_restaurants.embedding + Claude Haiku

Usage:
    python offmenu/services/discovery/match_social_mentions.py

Required env vars:
    VOYAGE_KEY
    ANTHROPIC_KEY
    SUPABASE_URL  (or NEXT_PUBLIC_SUPABASE_URL)
    SUPABASE_KEY  (or SERVICE_ROLE_KEY)
"""

import os
import sys
from typing import Any

import anthropic
import voyageai
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(".env.local")

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.environ["NEXT_PUBLIC_SUPABASE_URL"]
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ["SERVICE_ROLE_KEY"]
VOYAGE_KEY = os.environ["VOYAGE_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_KEY"]

EMBEDDING_MODEL = "voyage-3-lite"
VECTOR_CANDIDATES = 3
MODEL = "claude-haiku-4-5-20251001"

MATCH_PROMPT = """You are validating whether a restaurant mention from a social media post matches a known restaurant in our database.

Given:
- The mention extracted from social media (name, neighbourhood, cuisine)
- A shortlist of candidate restaurants from our database

Determine which candidate is the correct match, if any.

Respond ONLY with valid JSON:
{
  "place_id": "the place_id of the matching restaurant, or null if no confident match",
  "reasoning": "one sentence explanation"
}

Rules:
- Only match if you are confident it is the same restaurant
- Name variations are fine (e.g. "Shanius" = "Shaniu's House of Noodles")
- If neighbourhood or cuisine conflicts with the candidate, be more cautious
- Return null if none of the candidates are a convincing match"""


def fetch_unmatched_mentions(supabase: Any) -> list[dict[str, Any]]:
    result = (
        supabase.table("silver_social_mentions")
        .select("id, restaurant_name, social_handle, neighbourhood, cuisine, confidence")
        .eq("is_restaurant_mention", True)
        .is_("place_id", "null")
        .execute()
    )
    return result.data or []


def fetch_handle_map(supabase: Any) -> dict[str, str]:
    handle_map: dict[str, str] = {}
    page_size = 1000
    offset = 0
    while True:
        result = (
            supabase.table("restaurant_social_handles")
            .select("restaurant_id, ig_handle, tiktok_handle")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = result.data or []
        for row in batch:
            place_id = row["restaurant_id"]
            if row.get("ig_handle"):
                handle_map[row["ig_handle"].lstrip("@").lower()] = place_id
            if row.get("tiktok_handle"):
                handle_map[row["tiktok_handle"].lstrip("@").lower()] = place_id
        if len(batch) < page_size:
            break
        offset += page_size
    return handle_map


def fetch_name_map(supabase: Any) -> dict[str, str]:
    name_map: dict[str, str] = {}
    page_size = 1000
    offset = 0
    while True:
        result = (
            supabase.table("silver_restaurants")
            .select("place_id, name")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = result.data or []
        for row in batch:
            if row.get("name"):
                name_map[row["name"].lower().strip()] = row["place_id"]
        if len(batch) < page_size:
            break
        offset += page_size
    return name_map


def build_embedding_text(mention: dict[str, Any]) -> str:
    name = mention.get("restaurant_name") or ""
    neighbourhood = mention.get("neighbourhood") or "Berlin"
    return f"Restaurant: {name}, {neighbourhood}, Germany"


def get_vector_candidates(
    supabase: Any,
    embedding: list[float],
) -> list[dict[str, Any]]:
    vector_str = "[" + ",".join(str(x) for x in embedding) + "]"
    try:
        result = supabase.rpc(
            "match_restaurant_by_embedding",
            {
                "query_embedding": vector_str,
                "match_threshold": 0.6,
                "match_count": VECTOR_CANDIDATES,
            },
        ).execute()
        return result.data or []
    except Exception as e:
        print(f"  ⚠ vector search failed: {e}")
        return []


def agent_validate(
    client: anthropic.Anthropic,
    mention: dict[str, Any],
    candidates: list[dict[str, Any]],
    name_map_reversed: dict[str, str],
) -> str | None:
    candidate_lines = "\n".join(
        f"- place_id: {c['place_id']}, name: {name_map_reversed.get(c['place_id'], 'unknown')}, similarity: {c['similarity']:.3f}"
        for c in candidates
    )
    prompt = f"""Mention from social media:
- Name: {mention.get('restaurant_name')}
- Neighbourhood: {mention.get('neighbourhood') or 'unknown'}
- Cuisine: {mention.get('cuisine') or 'unknown'}

Candidates from database:
{candidate_lines}"""

    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=128,
            system=MATCH_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        import json
        parsed = json.loads(raw)
        return parsed.get("place_id")
    except Exception as e:
        print(f"  ⚠ agent validation failed for '{mention.get('restaurant_name')}': {e}")
        return None


def update_mentions_bulk(
    supabase: Any,
    mention_ids: list[str],
    place_id: str | None,
    method: str | None,
    status: str = "matched",
) -> None:
    supabase.table("silver_social_mentions").update({
        "place_id": place_id,
        "match_method": method,
        "match_status": status,
    }).in_("id", mention_ids).execute()


def group_by_name(mentions: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group mentions by normalised restaurant name. Preserves one representative per group."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for mention in mentions:
        key = (mention.get("restaurant_name") or "").lower().strip()
        groups.setdefault(key, []).append(mention)
    return groups


def main() -> int:
    voyage = voyageai.Client(api_key=VOYAGE_KEY)
    ai_client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    mentions = fetch_unmatched_mentions(supabase)
    print(f"Unmatched mentions to process: {len(mentions)}")

    if not mentions:
        print("Nothing to do.")
        return 0

    handle_map = fetch_handle_map(supabase)
    name_map = fetch_name_map(supabase)
    name_map_reversed = {v: k for k, v in name_map.items()}

    handle_matched = 0
    name_matched = 0
    agent_matched = 0
    needs_review = 0
    to_vector: list[dict[str, Any]] = []   # one representative per unique name
    to_vector_ids: dict[str, list[str]] = {}  # name_key → all mention IDs in that group

    groups = group_by_name(mentions)
    print(f"Unique restaurant names: {len(groups)}")

    for name_key, group in groups.items():
        all_ids = [m["id"] for m in group]
        representative = group[0]
        name = name_key.strip()
        handle = (representative.get("social_handle") or "").lstrip("@").lower().strip()

        # 1. Exact handle match
        if handle and handle in handle_map:
            update_mentions_bulk(supabase, all_ids, handle_map[handle], "handle")
            handle_matched += len(all_ids)
            continue

        # 2. Exact name match
        if name and name in name_map:
            update_mentions_bulk(supabase, all_ids, name_map[name], "exact_name")
            name_matched += len(all_ids)
            continue

        if name:
            to_vector.append(representative)
            to_vector_ids[name_key] = all_ids
        else:
            update_mentions_bulk(supabase, all_ids, None, None, "needs_review")
            needs_review += len(all_ids)

    print(f"  Handle matches: {handle_matched}")
    print(f"  Exact name matches: {name_matched}")
    print(f"  Sending {len(to_vector)} unique names to vector + agent matching...")

    # 3. Vector shortlist → agent validation (once per unique name)
    if to_vector:
        texts = [build_embedding_text(m) for m in to_vector]
        embeddings = voyage.embed(texts, model=EMBEDDING_MODEL, input_type="query").embeddings

        for mention, embedding in zip(to_vector, embeddings):
            name_key = (mention.get("restaurant_name") or "").lower().strip()
            all_ids = to_vector_ids[name_key]
            candidates = get_vector_candidates(supabase, embedding)

            if not candidates:
                update_mentions_bulk(supabase, all_ids, None, None, "needs_review")
                needs_review += len(all_ids)
                continue

            place_id = agent_validate(ai_client, mention, candidates, name_map_reversed)

            if place_id:
                update_mentions_bulk(supabase, all_ids, place_id, "vector+agent")
                agent_matched += len(all_ids)
                print(f"  ✓ '{mention.get('restaurant_name')}' → matched ({len(all_ids)} mentions)")
            else:
                update_mentions_bulk(supabase, all_ids, None, None, "needs_review")
                needs_review += len(all_ids)
                print(f"  ? '{mention.get('restaurant_name')}' → no match found")

    total_matched = handle_matched + name_matched + agent_matched
    print("---")
    print(f"Matched: {total_matched} (handle: {handle_matched}, exact: {name_matched}, agent: {agent_matched})")
    print(f"Needs review: {needs_review}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
