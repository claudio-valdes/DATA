from typing import Any, Callable

from supabase import Client, create_client

from settings.config import get_supabase_key, get_supabase_url

_client: Client | None = None

PAGE_SIZE = 1000


def get_client() -> Client:
    global _client
    if _client is None:
        _client = create_client(get_supabase_url(), get_supabase_key())
    return _client


def paginate(build_query: Callable[[Client], Any], page_size: int = PAGE_SIZE) -> list[dict]:
    """Run a Supabase query across all pages and return the combined rows.

    build_query receives the client and returns a query builder with
    .select()/.eq()/etc. already applied, but without .range() called yet.
    """
    supabase = get_client()
    rows: list[dict] = []
    offset = 0
    while True:
        result = build_query(supabase).range(offset, offset + page_size - 1).execute()
        batch = result.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def fetch_all(table: str, select: str, filters: list[tuple] | None = None, page_size: int = PAGE_SIZE) -> list[dict]:
    """Convenience wrapper over paginate() for simple single-level filters,
    e.g. filters=[("eq", "is_restaurant_mention", True)].
    """

    def build(supabase: Client):
        query = supabase.table(table).select(select)
        for method, col, val in (filters or []):
            query = getattr(query, method)(col, val)
        return query

    return paginate(build, page_size=page_size)
