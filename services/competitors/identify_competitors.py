"""
STUB — no competitor identification/validation logic exists anywhere in this
codebase yet. The `competitors` table exists in the schema but nothing reads or
writes it. This function is a placeholder called from target(for_report=True)
so the report-readiness pipeline has a clear seam to fill in once the
identification/validation algorithm is decided — it deliberately does not
invent scoring logic or write to `competitors`.

TODO: implement competitor identification + LLM validation, then write results
to the `competitors` table.
"""


def identify_competitors(restaurant: dict) -> dict:
    print(f"⚠ Competitor identification not yet implemented — skipping for {restaurant.get('slug')}")
    return {"status": "not_implemented"}
