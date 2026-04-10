"""
NBA Daily Scraper
Usa ScrapingAnt (alternativa gratuita) para acessar scores24.live
"""

import os
import re
import logging
import httpx
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from supabase import create_client, Client
from urllib.parse import quote

# ─── Config ────────────────────────────────────────────────────────────────────
def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise EnvironmentError(f"\n\n❌ Missing required secret: {name}")
    return val

SUPABASE_URL = _require_env("SUPABASE_URL")
SUPABASE_SERVICE_KEY = _require_env("SUPABASE_SERVICE_KEY")
SCRAPINGANT_API_KEY = os.environ.get("SCRAPINGANT_API_KEY", "")

BASE_URL = "https://scores24.live"
NBA_PREDICTIONS_URL = f"{BASE_URL}/pt/basketball/l-usa-nba/predictions"

ET = ZoneInfo("America/New_York")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


# ─── Supabase client ───────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ─── ScrapingAnt Functions ────────────────────────────────────────────────────
def get_scrapingant_url(target_url: str, proxy_country: str = "us") -> str:
    """
    Gera URL do ScrapingAnt para proxy
    Documentação: https://scrapingant.com/docs/
    """
    if not SCRAPINGANT_API_KEY:
        return target_url
    
    # URL encode do target
    encoded_url = quote(target_url, safe='')
    
    # Construir URL do ScrapingAnt
    return (
        f"https://api.scrapingant.com/v2/general?"
        f"url={encoded_url}&"
        f"x-api-key={SCRAPINGANT_API_KEY}&"
        f"proxy_country={proxy_country}&"
        f"wait_for_selector=body"  # Esperar o body carregar
    )


def fetch_html(url: str, retries: int = 3) -> str | None:
    """
    Busca HTML usando ScrapingAnt como proxy
    """
    for attempt in range(1, retries + 1):
        try:
            # Usar ScrapingAnt se disponível
            target_url = get_scrapingant_url(url) if SCRAPINGANT_API_KEY else url
            
            with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=45) as client:
                log.info(f"Fetching: {url[:50]}... (ScrapingAnt: {bool(SCRAPINGANT_API_KEY)})")
                
                resp = client.get(target_url)
                resp.raise_for_status()
                
                # ScrapingAnt retorna o HTML diretamente
                return resp.text
                
        except httpx.HTTPStatusError as e:
            log.warning(f"Tentativa {attempt}/{retries} - HTTP {e.response.status_code}: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        except Exception as e:
            log.warning(f"Tentativa {attempt}/{retries} falhou: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    
    log.error(f"Todas as tentativas falharam para {url}")
    return None


# ─── Parsers ───────────────────────────────────────────────────────────────────
def parse_game_list(html: str) -> list[dict]:
    """Parse upcoming + recent game cards from the NBA predictions page."""
    soup = BeautifulSoup(html, "html.parser")
    games = []

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

        date_str = match.group(1)
        teams_slug = match.group(2)

        try:
            game_date = datetime.strptime(date_str, "%d-%m-%Y").date()
        except ValueError:
            continue

        team_imgs = a_tag.find_all("img")
        team_names_from_alt = [img.get("alt", "").strip() for img in team_imgs if img.get("alt")]

        if len(team_names_from_alt) >= 2:
            home_team = team_names_from_alt[0]
            away_team = team_names_from_alt[1]
        else:
            parts = teams_slug.split("-")
            mid = len(parts) // 2
            home_team = " ".join(p.title() for p in parts[:mid])
            away_team = " ".join(p.title() for p in parts[mid:])

        time_match = re.search(r"(\d{2}:\d{2})", a_tag.get_text())
        game_time = time_match.group(1) if time_match else None

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

    log.info(f"Parsed {len(games)} games from scores24")
    return games


def parse_game_trends(html: str, game_slug: str) -> list[dict]:
    """Parse statistical trends/tendencies for a single game page."""
    soup = BeautifulSoup(html, "html.parser")
    trends = []

    # Buscar por diferentes padrões de tendências
    selectors = [
        ("div", r"trend|fact|stat|prediction"),
        ("section", r"trend|fact|stat"),
        ("li", r"prediction|trend"),
    ]

    for tag, pattern in selectors:
        for element in soup.find_all(tag, class_=re.compile(pattern, re.I)):
            text = element.get_text(separator=" ", strip=True)
            if not text or len(text) < 20:
                continue

            # Extrair odds
            odds_match = re.search(r"([+-]\d{2,4})", text)
            odds = odds_match.group(1) if odds_match else None

            # Extrair ratio (ex: "10 dos 11")
            ratio_match = re.search(r"(\d+)\s+dos\s+(\d+)", text)
            occurrences = f"{ratio_match.group(1)}/{ratio_match.group(2)}" if ratio_match else None

            # Detectar categoria
            category = "unknown"
            text_lower = text.lower()
            if any(k in text_lower for k in ["total", "pontos", "over", "under", "mais de", "menos de"]):
                category = "total"
            elif any(k in text_lower for k in ["handicap", "hándicap", "spread"]):
                category = "handicap"
            elif any(k in text_lower for k in ["vence", "vitória", "vencedor", "moneyline"]):
                category = "result"

            if odds or occurrences or len(text) > 50:
                trends.append({
                    "game_slug": game_slug,
                    "category": category,
                    "description": text[:500],
                    "odds": odds,
                    "occurrences": occurrences,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })

    # Remover duplicatas
    seen = set()
    unique = []
    for t in trends:
        key = t["description"][:100]
        if key not in seen:
            seen.add(key)
            unique.append(t)

    log.info(f"Parsed {len(unique)} trends for {game_slug}")
    return unique


# ─── Supabase upserts ──────────────────────────────────────────────────────────
def upsert_games(sb: Client, games: list[dict]) -> None:
    if not games:
        return
    result = sb.table("nba_games_schedule").upsert(games, on_conflict="slug").execute()
    log.info(f"✓ {len(games)} games saved")


def upsert_trends(sb: Client, trends: list[dict]) -> None:
    if not trends:
        return
    for t in trends:
        t["trend_key"] = f"{t['game_slug']}::{t['description'][:80]}"

    result = sb.table("nba_game_trends").upsert(trends, on_conflict="trend_key").execute()
    log.info(f"✓ {len(trends)} trends saved")


# ─── Main pipeline ─────────────────────────────────────────────────────────────
def run():
    log.info("═══ NBA Daily Scraper (ScrapingAnt) starting ═══")
    
    if not SCRAPINGANT_API_KEY:
        log.error("❌ SCRAPINGANT_API_KEY não configurado!")
        log.info("Configure o secret no GitHub Actions para usar o proxy")
        return
    
    sb = get_supabase()

    # 1. Fetch main predictions page via ScrapingAnt
    log.info("Fetching NBA predictions page via ScrapingAnt...")
    html = fetch_html(NBA_PREDICTIONS_URL)
    if not html:
        log.error("Failed to fetch predictions page. Aborting.")
        return

    # 2. Parse game list
    games = parse_game_list(html)
    if not games:
        log.warning("No games found in page")
        return
    
    upsert_games(sb, games)

    # 3. For each game: fetch detailed page → parse trends
    all_trends = []
    for i, game in enumerate(games):
        detail_url = game["source_url"].replace("-prediction", "")
        log.info(f"[{i+1}/{len(games)}] Fetching: {game['away_team']} vs {game['home_team']}")
        
        detail_html = fetch_html(detail_url)
        if detail_html:
            trends = parse_game_trends(detail_html, game["slug"])
            all_trends.extend(trends)
        
        # Rate limiting: esperar entre requisições
        time.sleep(1.5)

    upsert_trends(sb, all_trends)

    log.info("═══ Scraper finished successfully ═══")
    log.info(f"Summary: {len(games)} games | {len(all_trends)} trends")


if __name__ == "__main__":
    run()
