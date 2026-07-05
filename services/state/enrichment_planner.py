"""
Pure decision logic — given a restaurant's enrichment status, decide which channels
need to run. No I/O here; keeps this trivially testable against a status dict.
"""

from datetime import datetime, timedelta, timezone

STALENESS_DAYS = {
    "ig": 14,
    "tiktok": 14,
    "reviews": 14,
    "contacts": 90,
    "tripadvisor": 30,
    "serp": 21,
}

# Channels forced to refresh in mode="report", regardless of staleness — report
# content can't cite stale sentiment/visibility data.
FORCE_IN_REPORT_MODE = {"ig", "tiktok", "reviews", "serp"}

_HANDLE_REQUIRED = {
    "ig": "has_ig_handle",
    "tiktok": "has_tiktok_handle",
}

_LAST_FETCHED_FIELD = {
    "ig": "ig_last_fetched",
    "tiktok": "tiktok_last_fetched",
    "reviews": "reviews_last_scraped",
    "contacts": "contacts_last_scraped",
    "tripadvisor": "tripadvisor_last_checked",
    "serp": "serp_last_checked",
}

_PLAN_STEPS = {
    "ig": ["ig_social", "ig_comments"],
    "tiktok": ["tiktok_social", "tiktok_comments"],
    "reviews": ["reviews"],
    "contacts": ["contacts"],
    "tripadvisor": ["tripadvisor"],
    "serp": ["serp"],
}


def _is_stale(last_fetched: str | None, staleness_days: int) -> bool:
    if not last_fetched:
        return True
    fetched_at = datetime.fromisoformat(last_fetched.replace("Z", "+00:00"))
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - fetched_at > timedelta(days=staleness_days)


def plan_enrichment(status: dict, mode: str = "normal") -> list[str]:
    plan: list[str] = []

    for channel, steps in _PLAN_STEPS.items():
        handle_field = _HANDLE_REQUIRED.get(channel)
        if handle_field and not status.get(handle_field):
            continue

        force = mode == "report" and channel in FORCE_IN_REPORT_MODE
        last_fetched = status.get(_LAST_FETCHED_FIELD[channel])
        if not force and not _is_stale(last_fetched, STALENESS_DAYS[channel]):
            continue

        plan.extend(steps)

    return plan
