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
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ─── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

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
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


# ─── Supabase client ───────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ─── HTTP helpers ──────────────────────────────────────────────────────────────
def fetch_html(url: str, retries: int = 3) -> str | None:
    for attempt in range(1, retries + 1):
        try:
            with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=20) as client:
                resp = client.get(url)
                resp.raise_for_status()
                return resp.text
        except httpx.HTTPError as e:
            log.warning(f"Attempt {attempt}/{retries} failed for {url}: {e}")
    log.error(f"All retries exhausted for {url}")
    return None


# ─── Parsers ───────────────────────────────────────────────────────────────────
def parse_game_list(html: str) -> list[dict]:
    """Parse upcoming + recent game cards from the NBA predictions page."""
    soup = BeautifulSoup(html, "html.parser")
    games = []

    # Game links follow pattern: /pt/basketball/m-DD-MM-YYYY-team1-team2[-prediction]
    game_pattern = re.compile(r"/pt/basketball/m-(\d{2}-\d{2}-\d{4})-(.+?)(?:-prediction)?$")

    seen_slugs: set[str] = set()

    for a_tag in soup.find_all("a", href=game_pattern):
        href = a_tag["href"]
        match = game_pattern.search(href)
        if not match:
            continue

        slug = match.group(0).rstrip("/")
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)

        date_str = match.group(1)          # DD-MM-YYYY
        teams_slug = match.group(2)        # team1-team2

        # Parse date
        try:
            game_date = datetime.strptime(date_str, "%d-%m-%Y").date()
        except ValueError:
            continue

        # Extract team names from the anchor text or child elements
        team_imgs = a_tag.find_all("img")
        team_names_from_alt = [img.get("alt", "").strip() for img in team_imgs if img.get("alt")]

        # Fallback: derive from slug
        if len(team_names_from_alt) >= 2:
            home_team = team_names_from_alt[0]
            away_team = team_names_from_alt[1]
        else:
            parts = teams_slug.split("-")
            mid = len(parts) // 2
            home_team = " ".join(p.title() for p in parts[:mid])
            away_team = " ".join(p.title() for p in parts[mid:])

        # Time extraction: look for sibling/parent text containing HH:MM
        time_match = re.search(r"(\d{2}:\d{2})", a_tag.get_text())
        game_time = time_match.group(1) if time_match else None

        # Confidence / odds from text
        confidence_match = re.search(r"(\d{1,3})%", a_tag.get_text())
        confidence_pct = int(confidence_match.group(1)) if confidence_match else None

        full_url = BASE_URL + href if href.startswith("/") else href

        games.append({
            "game_date": game_date.isoformat(),
            "game_time_et": game_time,
            "home_team": home_team,
            "away_team": away_team,
            "slug": slug,
            "source_url": full_url,
            "confidence_pct": confidence_pct,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    log.info(f"Parsed {len(games)} games")
    return games


def parse_game_trends(html: str, game_slug: str) -> list[dict]:
    """Parse statistical trends/tendencies for a single game page."""
    soup = BeautifulSoup(html, "html.parser")
    trends = []

    # Trend blocks contain a description text + bet type + odds
    # Pattern: section headings like "Resultados", "Total", "Handicap"
    for section in soup.find_all(["section", "div"], class_=re.compile(r"trend|fact|stat", re.I)):
        for item in section.find_all(["li", "div", "p"], recursive=True):
            text = item.get_text(separator=" ", strip=True)
            if not text or len(text) < 20:
                continue

            # Extract odds value (e.g. -159, +128)
            odds_match = re.search(r"([+-]\d{2,4})", text)
            odds = odds_match.group(1) if odds_match else None

            # Extract occurrence ratio (e.g. "10 dos 11")
            ratio_match = re.search(r"(\d+)\s+dos\s+(\d+)", text)
            occurrences = f"{ratio_match.group(1)}/{ratio_match.group(2)}" if ratio_match else None

            # Detect bet category
            category = "unknown"
            text_lower = text.lower()
            if any(k in text_lower for k in ["total", "pontos", "over", "under", "mais de", "menos de"]):
                category = "total"
            elif any(k in text_lower for k in ["handicap", "hándicap"]):
                category = "handicap"
            elif any(k in text_lower for k in ["vence", "vitória", "perde", "resultado"]):
                category = "result"

            if odds or occurrences:
                trends.append({
                    "game_slug": game_slug,
                    "category": category,
                    "description": text[:500],
                    "odds": odds,
                    "occurrences": occurrences,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })

    # Deduplicate by description
    seen = set()
    unique = []
    for t in trends:
        key = t["description"][:100]
        if key not in seen:
            seen.add(key)
            unique.append(t)

    log.info(f"Parsed {len(unique)} trends for {game_slug}")
    return unique


def fetch_team_news(team_name: str) -> list[dict]:
    """
    Fetch recent news for a team via ESPN search RSS or Google News.
    Falls back gracefully if unavailable.
    """
    news_items = []

    # Use ESPN search (public, no auth)
    search_url = (
        f"https://www.espn.com/espn/rss/nba/news"
    )

    html = fetch_html(search_url)
    if not html:
        return news_items

    soup = BeautifulSoup(html, "xml")
    team_lower = team_name.lower()

    for item in soup.find_all("item")[:50]:
        title = item.find("title")
        link = item.find("link")
        pub_date = item.find("pubDate")
        description = item.find("description")

        if not title:
            continue

        title_text = title.get_text(strip=True)
        desc_text = description.get_text(strip=True) if description else ""

        # Filter by team name mention
        combined = (title_text + " " + desc_text).lower()
        # Match full team name or last word (e.g. "Celtics", "Lakers")
        team_keywords = team_lower.split()
        if not any(kw in combined for kw in team_keywords if len(kw) > 4):
            continue

        news_items.append({
            "team": team_name,
            "title": title_text,
            "url": link.get_text(strip=True) if link else None,
            "published_at": pub_date.get_text(strip=True) if pub_date else None,
            "summary": desc_text[:500],
            "source": "ESPN RSS",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    log.info(f"Found {len(news_items)} news items for {team_name}")
    return news_items


# ─── Supabase upserts ──────────────────────────────────────────────────────────
def upsert_games(sb: Client, games: list[dict]) -> None:
    if not games:
        return
    result = sb.table("nba_games_schedule").upsert(games, on_conflict="slug").execute()
    log.info(f"Upserted {len(games)} games → nba_games_schedule")


def upsert_trends(sb: Client, trends: list[dict]) -> None:
    if not trends:
        return
    # Upsert on game_slug + description prefix (truncated key)
    for t in trends:
        t["trend_key"] = f"{t['game_slug']}::{t['description'][:80]}"

    result = sb.table("nba_game_trends").upsert(trends, on_conflict="trend_key").execute()
    log.info(f"Upserted {len(trends)} trends → nba_game_trends")


def upsert_news(sb: Client, news: list[dict]) -> None:
    if not news:
        return
    for n in news:
        n["news_key"] = f"{n['team']}::{n['title'][:100]}"

    result = sb.table("nba_team_news").upsert(news, on_conflict="news_key").execute()
    log.info(f"Upserted {len(news)} news → nba_team_news")


# ─── Main pipeline ─────────────────────────────────────────────────────────────
def run():
    log.info("═══ NBA Daily Scraper starting ═══")
    sb = get_supabase()

    # 1. Fetch main predictions page
    log.info("Fetching NBA predictions page...")
    html = fetch_html(NBA_PREDICTIONS_URL)
    if not html:
        log.error("Failed to fetch predictions page. Aborting.")
        return

    # 2. Parse game list
    games = parse_game_list(html)
    upsert_games(sb, games)

    # 3. For each game: fetch detailed page → parse trends
    all_trends = []
    for game in games:
        detail_url = game["source_url"].replace("-prediction", "")
        log.info(f"Fetching trends: {game['home_team']} vs {game['away_team']}")
        detail_html = fetch_html(detail_url)
        if detail_html:
            trends = parse_game_trends(detail_html, game["slug"])
            all_trends.extend(trends)

    upsert_trends(sb, all_trends)

    # 4. Fetch news for each unique team
    teams = set()
    for g in games:
        teams.add(g["home_team"])
        teams.add(g["away_team"])

    all_news = []
    for team in sorted(teams):
        if not team or team == "Unknown":
            continue
        news = fetch_team_news(team)
        all_news.extend(news)

    upsert_news(sb, all_news)

    log.info("═══ Scraper finished ═══")
    log.info(f"Summary: {len(games)} games | {len(all_trends)} trends | {len(all_news)} news")


if __name__ == "__main__":
    run()
