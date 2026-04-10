"""
NBA Daily Scraper - Jogos e Prévias Detalhadas
Usa NewsAPI + ESPN API para notícias completas
"""

import os
import re
import logging
import httpx
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from supabase import create_client, Client

from nba_api.live.nba.endpoints import scoreboard

# ─── Config ────────────────────────────────────────────────────────────────────
def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise EnvironmentError(f"\n\n❌ Missing required secret: {name}")
    return val

SUPABASE_URL = _require_env("SUPABASE_URL")
SUPABASE_SERVICE_KEY = _require_env("SUPABASE_SERVICE_KEY")
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")

BRT = ZoneInfo("America/Sao_Paulo")
ET = ZoneInfo("America/New_York")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


# ─── Supabase client ───────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ─── Helper Functions ───────────────────────────────────────────────────────────
def get_team_names() -> dict:
    """Retorna dicionário com nomes dos times"""
    return {
        "ATL": ("Hawks", "Hawks"), "BOS": ("Celtics", "Celtics"),
        "BKN": ("Nets", "Nets"), "CHA": ("Hornets", "Hornets"),
        "CHI": ("Bulls", "Bulls"), "CLE": ("Cavaliers", "Cavaliers"),
        "DAL": ("Mavericks", "Mavericks"), "DEN": ("Nuggets", "Nuggets"),
        "DET": ("Pistons", "Pistons"), "GSW": ("Warriors", "Warriors"),
        "HOU": ("Rockets", "Rockets"), "IND": ("Pacers", "Pacers"),
        "LAC": ("Clippers", "Clippers"), "LAL": ("Lakers", "Lakers"),
        "MEM": ("Grizzlies", "Grizzlies"), "MIA": ("Heat", "Heat"),
        "MIL": ("Bucks", "Bucks"), "MIN": ("Timberwolves", "Timberwolves"),
        "NOP": ("Pelicans", "Pelicans"), "NYK": ("Knicks", "Knicks"),
        "OKC": ("Thunder", "Thunder"), "ORL": ("Magic", "Magic"),
        "PHI": ("76ers", "76ers"), "PHX": ("Suns", "Suns"),
        "POR": ("Trail Blazers", "Trail Blazers"), "SAC": ("Kings", "Kings"),
        "SAS": ("Spurs", "Spurs"), "TOR": ("Raptors", "Raptors"),
        "UTA": ("Jazz", "Jazz"), "WAS": ("Wizards", "Wizards")
    }


def convert_to_brt(game_time_utc: str) -> str:
    """Converte horário UTC para BRT"""
    try:
        utc_time = datetime.fromisoformat(game_time_utc.replace("Z", "+00:00"))
        brt_time = utc_time.astimezone(BRT)
        return brt_time.strftime("%H:%M")
    except:
        return "20:00"


# ─── NBA API Functions ─────────────────────────────────────────────────────────
def fetch_today_games() -> list[dict]:
    """Busca jogos do dia atual"""
    try:
        log.info("Buscando jogos do dia...")
        
        games_data = scoreboard.ScoreBoard()
        games_dict = games_data.get_dict()
        
        team_names = get_team_names()
        games = []
        
        for game in games_dict.get("scoreboard", {}).get("games", []):
            home_tri = game["homeTeam"]["teamTricode"]
            away_tri = game["awayTeam"]["teamTricode"]
            game_id = game["gameId"]
            
            slug = f"{away_tri.lower()}-vs-{home_tri.lower()}-{game_id}"
            game_time_brt = convert_to_brt(game.get("gameTimeUTC", ""))
            
            game_info = {
                "slug": slug,
                "game_date": datetime.now(BRT).strftime("%Y-%m-%d"),
                "game_time_brt": game_time_brt,
                "home_team": game["homeTeam"]["teamName"],
                "away_team": game["awayTeam"]["teamName"],
                "home_tri": home_tri,
                "away_tri": away_tri,
                "game_status": game["gameStatusText"],
                "nba_game_url": f"https://www.nba.com/game/{slug}",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
            games.append(game_info)
        
        log.info(f"Encontrados {len(games)} jogos")
        return games
        
    except Exception as e:
        log.error(f"Erro ao buscar jogos: {e}")
        return []


# ─── News Functions ──────────────────────────────────────────────────────────────
def fetch_game_preview_news(team1: str, team2: str, days_back: int = 7) -> list[dict]:
    """Busca notícias específicas de prévia para um confronto"""
    if not NEWSAPI_KEY:
        return []
    
    # Keywords mais específicas para prévias
    keywords = [
        f"{team1} {team2} preview",
        f"{team1} {team2} matchup",
        f"{team1} vs {team2}",
        f"{team1} injury report",
        f"{team2} injury report",
    ]
    
    all_news = []
    from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    
    for keyword in keywords:
        try:
            url = "https://newsapi.org/v2/everything"
            params = {
                "q": keyword,
                "from": from_date,
                "language": "en",
                "sortBy": "relevancy",
                "pageSize": 10,
                "apiKey": NEWSAPI_KEY,
            }
            
            with httpx.Client(timeout=30) as client:
                resp = client.get(url, params=params)
                data = resp.json()
                
                if data.get("status") == "ok":
                    for article in data.get("articles", []):
                        title = article.get("title", "")
                        if not title or title == "[Removed]":
                            continue
                        
                        # Verificar se menciona ambos os times
                        title_lower = title.lower()
                        if team1.lower() in title_lower or team2.lower() in title_lower:
                            all_news.append({
                                "news_key": f"preview-{hash(title) % 10000000}",
                                "title": title,
                                "url": article.get("url"),
                                "summary": article.get("description", "")[:500] if article.get("description") else "",
                                "published_at": article.get("publishedAt"),
                                "source": article.get("source", {}).get("name", "NewsAPI"),
                                "scraped_at": datetime.now(timezone.utc).isoformat(),
                            })
            
            time.sleep(0.3)  # Rate limiting
            
        except Exception as e:
            log.warning(f"Erro ao buscar '{keyword}': {e}")
    
    # Remover duplicatas
    seen = set()
    unique_news = []
    for n in all_news:
        if n["title"] not in seen:
            seen.add(n["title"])
            unique_news.append(n)
    
    return unique_news


def fetch_nba_com_game_preview(game_slug: str, max_retries: int = 2) -> dict:
    """Tenta extrair prévia diretamente da página do jogo no NBA.com"""
    url = f"https://www.nba.com/game/{game_slug}"
    
    for attempt in range(max_retries):
        try:
            with httpx.Client(headers=HEADERS, timeout=20, follow_redirects=True) as client:
                resp = client.get(url)
                
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    
                    # Buscar elementos de prévia
                    preview = soup.find("div", class_=re.compile(r"preview|article|content", re.I))
                    title = soup.find("h1")
                    
                    if preview:
                        text = preview.get_text(separator=" ", strip=True)
                        # Limitar tamanho
                        if len(text) > 1000:
                            text = text[:997] + "..."
                        
                        return {
                            "news_key": f"nba-com-{hash(game_slug) % 10000000}",
                            "title": title.get_text(strip=True) if title else f"Game Preview: {game_slug}",
                            "url": url,
                            "summary": text,
                            "published_at": datetime.now(timezone.utc).isoformat(),
                            "source": "NBA.com",
                            "scraped_at": datetime.now(timezone.utc).isoformat(),
                        }
                    
        except Exception as e:
            log.warning(f"Tentativa {attempt + 1} falhou para {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
    
    return None


def fetch_espn_game_story(game_slug: str) -> dict:
    """Busca história do jogo na ESPN"""
    # Extrair IDs dos times do slug
    parts = game_slug.split("-")
    if len(parts) >= 4:
        away = parts[0]
        home = parts[2]
        
        # Buscar na ESPN
        try:
            espn_url = f"https://www.espn.com/nba/game/_/gameId/{parts[-1]}"
            
            with httpx.Client(headers=HEADERS, timeout=20) as client:
                resp = client.get(espn_url)
                
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    
                    # Buscar artigo de prévia
                    article = soup.find("article") or soup.find("div", class_=re.compile(r"story|article", re.I))
                    
                    if article:
                        title_elem = soup.find("h1")
                        title = title_elem.get_text(strip=True) if title_elem else f"{away.upper()} vs {home.upper()}"
                        
                        text = article.get_text(separator=" ", strip=True)[:800]
                        
                        return {
                            "news_key": f"espn-{hash(game_slug) % 10000000}",
                            "title": title,
                            "url": espn_url,
                            "summary": text,
                            "published_at": datetime.now(timezone.utc).isoformat(),
                            "source": "ESPN",
                            "scraped_at": datetime.now(timezone.utc).isoformat(),
                        }
                        
        except Exception as e:
            log.warning(f"Erro ESPN: {e}")
    
    return None


# ─── Supabase Functions ─────────────────────────────────────────────────────────
def upsert_games(sb: Client, games: list[dict]) -> None:
    if not games:
        return
    
    result = sb.table("nba_games_schedule").upsert(games, on_conflict="slug").execute()
    log.info(f"✓ {len(games)} jogos salvos")


def upsert_news(sb: Client, news: list[dict]) -> None:
    if not news:
        return
    
    for n in news:
        if "news_key" not in n:
            n["news_key"] = f"news-{hash(n['title']) % 10000000}"
        if "team" not in n:
            n["team"] = "NBA"
    
    result = sb.table("nba_team_news").upsert(news, on_conflict="news_key").execute()
    log.info(f"✓ {len(news)} notícias salvas")


# ─── Main ──────────────────────────────────────────────────────────────────────
def run():
    log.info("═══ NBA Scraper - Prévias Detalhadas ═══")
    sb = get_supabase()

    # 1. Buscar jogos
    games = fetch_today_games()
    if not games:
        log.warning("Nenhum jogo encontrado")
        return
    
    upsert_games(sb, games)

    # 2. Buscar notícias detalhadas para cada jogo
    all_news = []
    
    for game in games:
        home = game["home_team"]
        away = game["away_team"]
        slug = game["slug"]
        
        log.info(f"Buscando notícias para: {away} vs {home}")
        
        # Buscar na NewsAPI com keywords específicas
        preview_news = fetch_game_preview_news(away, home, days_back=5)
        
        # Se não encontrou notícias detalhadas, tentar scraping direto
        if not preview_news or len(preview_news) == 0:
            nba_preview = fetch_nba_com_game_preview(slug)
            if nba_preview:
                preview_news.append(nba_preview)
            
            espn_preview = fetch_espn_game_story(slug)
            if espn_preview:
                preview_news.append(espn_preview)
        
        # Se ainda não tem notícias, criar genérica
        if not preview_news:
            preview_news.append({
                "news_key": f"fallback-{slug}",
                "game_slug": slug,
                "team": f"{away} vs {home}",
                "title": f"Game Preview: {away} vs {home}",
                "url": game["nba_game_url"],
                "summary": f"Matchup between {away} and {home} scheduled for today. Check NBA.com for latest updates on injuries, standings, and player matchups.",
                "source": "NBA.com",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
        
        # Associar game_slug a todas as notícias deste jogo
        for news in preview_news:
            news["game_slug"] = slug
            news["team"] = f"{away} vs {home}"
        
        all_news.extend(preview_news)
        time.sleep(0.5)  # Rate limiting gentil

    upsert_news(sb, all_news)

    # Resumo
    log.info("\n═══ RESUMO ═══")
    for game in games:
        game_news_count = len([n for n in all_news if n.get("game_slug") == game["slug"]])
        log.info(f"{game['game_time_brt']} - {game['away_team']} vs {game['home_team']} ({game_news_count} notícias)")
    
    log.info(f"\nTotal: {len(games)} jogos | {len(all_news)} notícias")


if __name__ == "__main__":
    run()
