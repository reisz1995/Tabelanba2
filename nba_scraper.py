"""
NBA Daily Scraper
Scrapes: game schedule, statistical trends, and team news from scores24.live
Persists to Supabase
Run: python nba_scraper.py
"""

import os
import re
import json
import logging
import httpx
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ─── Config ────────────────────────────────────────────────────────────────────
def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise EnvironmentError(
            f"\n\n❌ Missing required secret: {name}\n"
            f"   → Go to GitHub repo → Settings → Secrets and variables → Actions\n"
            f"   → Add a secret named '{name}' with the correct value.\n"
        )
    return val

SUPABASE_URL = _require_env("SUPABASE_URL")
SUPABASE_KEY = _require_env("SUPABASE_KEY")

BASE_URL = "https://scores24.live"
NBA_PREDICTIONS_URL = f"{BASE_URL}/pt/basketball/l-usa-nba/predictions"

ET = ZoneInfo("America/New_York")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.google.com/",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Cache-Control": "max-age=0",
}


# ─── Supabase client ───────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ─── HTTP helpers ──────────────────────────────────────────────────────────────
def fetch_html(url: str, retries: int = 3) -> str | None:
    for attempt in range(1, retries + 1):
        try:
            with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30) as client:
                resp = client.get(url)
                resp.raise_for_status()
                return resp.text
        except httpx.HTTPError as e:
            log.warning(f"Tentativa {attempt}/{retries} falhou para {url}: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)  # Backoff exponencial
    log.error(f"Todas as tentativas falharam para {url}")
    return None
    
