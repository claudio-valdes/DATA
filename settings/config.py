import os

from dotenv import load_dotenv

load_dotenv(".env.local")


def get_supabase_url() -> str:
    url = os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    if not url:
        raise RuntimeError("Missing required environment variable: SUPABASE_URL (or NEXT_PUBLIC_SUPABASE_URL)")
    return url


def get_supabase_key() -> str:
    key = os.environ.get("SUPABASE_KEY") or os.environ.get("SERVICE_ROLE_KEY")
    if not key:
        raise RuntimeError("Missing required environment variable: SUPABASE_KEY (or SERVICE_ROLE_KEY)")
    return key


def get_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value
