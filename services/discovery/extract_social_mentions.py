"""
Process unprocessed bronze_social_posts through Claude Haiku to extract restaurant mentions.
Results land in silver_social_mentions. Posts are marked processed=true when done.
"""

import asyncio
import json
import os
from typing import Any

import anthropic

from repository.db import get_client

ANTHROPIC_KEY = os.environ["ANTHROPIC_KEY"]

MODEL = "claude-haiku-4-5-20251001"
CONCURRENCY = 5
FLUSH_SIZE = 50

SYSTEM_PROMPT = """You are extracting restaurant mentions from social media posts about Berlin food.

A single post may mention multiple restaurants. Extract ALL of them.

Signal priority (strongest to weakest):
1. Location tag — if present, it is almost certainly a restaurant name (e.g. "Samurai Wagyu Yakiniku & Sushi Berlin Mitte")
2. Street addresses in caption — restaurants listed with "📍" or a street address are named restaurants
3. Restaurant-specific hashtags — hashtags like #wenchengberlin #anneliesberlin #currybaude are restaurant names (strip city suffix: #wenchengberlin → "Wen Cheng")
4. Restaurant name explicitly in caption text

Respond ONLY with valid JSON in this exact format:
{
  "mentions": [
    {
      "restaurant_name": "name as best you can determine",
      "neighbourhood": "Berlin neighbourhood or null",
      "cuisine": "cuisine type or null",
      "social_handle": "restaurant handle if tagged (NOT the post author) or null",
      "address": "street address if present in caption or null",
      "confidence": 0.0 to 1.0
    }
  ]
}

If no specific restaurant is identifiable, return: {"mentions": []}

Confidence scoring:
- 0.9–1.0: location tag, address with name, or unambiguous restaurant hashtag
- 0.6–0.8: restaurant name clearly in caption text
- 0.3–0.5: inferable but uncertain
- 0.0–0.2: very unlikely

Rules:
- Only include real named restaurants — not markets, food halls, or generic food spots unless named
- neighbourhood: use Berlin neighbourhood names (Mitte, Kreuzberg, Prenzlauer Berg, Neukölln, etc.)
- social_handle: only the restaurant's own handle, never the post author"""


def fetch_unprocessed_posts(supabase: Any, limit: int) -> list[dict[str, Any]]:
    result = (
        supabase.table("bronze_social_posts")
        .select("id, platform, query, author_handle, caption, hashtags, likes, comments_count, shares, plays, location_name")
        .eq("processed", False)
        .limit(limit)
        .execute()
    )
    return result.data or []


def build_post_text(post: dict[str, Any]) -> str:
    parts = []
    if post.get("caption"):
        parts.append(f"Caption: {post['caption']}")
    if post.get("hashtags"):
        parts.append(f"Hashtags: {' '.join(f'#{h}' for h in post['hashtags'])}")
    if post.get("location_name"):
        parts.append(f"Location tag: {post['location_name']}")
    if post.get("author_handle"):
        parts.append(f"Posted by: @{post['author_handle']}")
    return "\n".join(parts) if parts else ""


def build_mention_records(post: dict[str, Any], mentions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build one record per restaurant mention. If no mentions, build one null record for audit."""
    base = {
        "bronze_post_id": post["id"],
        "platform": post["platform"],
        "match_status": "needs_review",
        "post_likes": post.get("likes"),
        "post_plays": post.get("plays"),
        "post_shares": post.get("shares"),
        "post_comments": post.get("comments_count"),
    }
    if not mentions:
        return [{**base, "is_restaurant_mention": False, "confidence": None}]
    return [
        {
            **base,
            "restaurant_name": m.get("restaurant_name"),
            "neighbourhood": m.get("neighbourhood"),
            "cuisine": m.get("cuisine"),
            "social_handle": m.get("social_handle"),
            "address": m.get("address"),
            "confidence": m.get("confidence"),
            "is_restaurant_mention": True,
        }
        for m in mentions
    ]


def flush(supabase: Any, mention_buffer: list[dict], processed_ids: list[str]) -> None:
    if mention_buffer:
        supabase.table("silver_social_mentions").insert(mention_buffer).execute()
    if processed_ids:
        supabase.table("bronze_social_posts").update(
            {"processed": True}
        ).in_("id", processed_ids).execute()


async def extract_one(
    client: anthropic.AsyncAnthropic,
    post: dict[str, Any],
    semaphore: asyncio.Semaphore,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    post_text = build_post_text(post)
    if not post_text.strip():
        return post, []

    async with semaphore:
        try:
            message = await client.messages.create(
                model=MODEL,
                max_tokens=512,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": post_text}],
            )
            raw = message.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("```", 2)[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.strip()
            try:
                parsed = json.loads(raw)
                return post, parsed.get("mentions") or []
            except json.JSONDecodeError:
                print(f"  ⚠ JSON parse failed for post {post['id']}. Raw response: {raw[:200]}")
                return post, []
        except Exception as e:
            print(f"  ⚠ API error for post {post['id']}: {e}")
            return post, []


async def _extract_mentions(limit: int) -> dict:
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)
    supabase = get_client()
    semaphore = asyncio.Semaphore(CONCURRENCY)

    posts = fetch_unprocessed_posts(supabase, limit)
    print(f"Unprocessed posts to extract: {len(posts)}")

    if not posts:
        print("Nothing to do.")
        return {"processed": 0, "mentions_found": 0, "errors": 0}

    tasks = [extract_one(client, post, semaphore) for post in posts]

    extracted = 0
    mentions_found = 0
    errors = 0
    mention_buffer: list[dict] = []
    processed_ids: list[str] = []

    for coro in asyncio.as_completed(tasks):
        try:
            post, mentions = await coro
            processed_ids.append(post["id"])
            mention_buffer.extend(build_mention_records(post, mentions))
            extracted += 1

            for m in mentions:
                mentions_found += 1
                name = m.get("restaurant_name", "?")
                confidence = m.get("confidence", 0)
                print(f"  ✓ [{post['platform']}] {name} (confidence: {confidence:.2f})")

        except Exception as e:
            errors += 1
            print(f"  ✗ {e}")

        if len(processed_ids) % FLUSH_SIZE == 0:
            flush(supabase, mention_buffer, processed_ids)
            mention_buffer = []
            processed_ids = []
            print(f"  [{extracted}/{len(posts)}] flushed to db")

    flush(supabase, mention_buffer, processed_ids)

    print("---")
    print(f"Posts processed: {extracted} | Restaurant mentions found: {mentions_found} | Errors: {errors}")
    return {"processed": extracted, "mentions_found": mentions_found, "errors": errors}


def extract_mentions(limit: int = 500) -> dict:
    """Extract restaurant mentions from unprocessed bronze_social_posts via Claude Haiku. Returns summary counts."""
    return asyncio.run(_extract_mentions(limit))
