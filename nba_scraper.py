"""
NBA Daily Scraper - Jogos e Prévias Detalhadas
"""

import os
import re
import logging
import httpx
import time
import hashlib
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def get_team_names() -> dict:
    return {
        "ATL": "Hawks", "BOS": "Celtics", "BKN": "Nets", "CHA": "Hornets",
        "CHI": "Bulls", "CLE": "Cavaliers", "DAL": "Mavericks", "DEN": "Nuggets",
        "DET": "Pistons", "GSW": "Warriors", "HOU": "Rockets", "IND": "Pacers",
        "LAC": "Clippers", "LAL": "Lakers", "MEM": "Grizzlies", "MIA": "Heat",
        "MIL": "Bucks", "MIN": "Timberwolves", "NOP": "Pelicans", "NYK": "Knicks",
        "OKC": "Thunder", "ORL": "Magic", "PHI": "76ers", "PHX": "Suns",
        "POR": "Trail Blazers", "SAC": "Kings", "SAS": "Spurs", "TOR": "Raptors",
        "UTA": "Jazz", "WAS": "Wizards"
    }


def convert_to_brt(game_time_utc: str) -> str:
    try:
        utc_time = datetime.fromisoformat(game_time_utc.replace("Z", "+00:00"))
        brt_time = utc_time.astimezone(BRT)
        return brt_time.strftime("%H:%M")
    except:
        return "20:00"


def generate_news_key(title: str, game_slug: str = "") -> str:
    """Gera chave única para notícia"""
    key_string = f"{game_slug}-{title}"
    hash_value = hashlib.md5(key_string.encode()).hexdigest()[:10]
    return f"news-{hash_value}"


def fetch_today_games() -> list[dict]:
    try:
        log.info("Buscando jogos do dia...")
        games_data = scoreboard.ScoreBoard()
        games_dict = games_data.get_dict()
        
        games = []
        for game in games_dict.get("scoreboard", {}).get("games", []):
            home_tri = game["homeTeam"]["teamTricode"]
            away_tri = game["awayTeam"]["teamTricode"]
            game_id = game["gameId"]
            slug = f"{away_tri.lower()}-vs-{home_tri.lower()}-{game_id}"
            
            games.append({
                "slug": slug,
                "game_date": datetime.now(BRT).strftime("%Y-%m-%d"),
                "game_time_brt": convert_to_brt(game.get("gameTimeUTC", "")),
                "home_team": game["homeTeam"]["teamName"],
                "away_team": game["awayTeam"]["teamName"],
                "home_tri": home_tri,
                "away_tri": away_tri,
                "game_status": game["gameStatusText"],
                "nba_game_url": f"https://www.nba.com/game/{slug}",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
        
        log.info(f"Encontrados {len(games)} jogos")
        return games
    except Exception as e:
        log.error(f"Erro ao buscar jogos: {e}")
        return []


def fetch_game_news_api(team1: str, team2: str, game_slug: str) -> list[dict]:
    """Busca notícias da NewsAPI para um confronto específico"""
    if not NEWSAPI_KEY:
        return []
    
    queries = [
        f"{team1} {team2} preview",
        f"{team1} {team2} injury",
        f"{team1} vs {team2} 2025",
    ]
    
    all_news = []
    from_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
    
    for query in queries:
        try:
            url = "https://newsapi.org/v2/everything"
            params = {
                "q": query,
                "from": from_date,
                "language": "en",
                "sortBy": "relevancy",
                "pageSize": 5,
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
                        
                        # Verificar se realmente menciona os times
                        title_lower = title.lower()
                        if team1.lower() in title_lower or team2.lower() in title_lower:
                            all_news.append({
                                "news_key": generate_news_key(title, game_slug),
                                "title": title,
                                "url": article.get("url"),
                                "summary": article.get("description", "")[:400] if article.get("description") else "",
                                "published_at": article.get("publishedAt"),
                                "source": article.get("source", {}).get("name", "NewsAPI"),
                                "game_slug": game_slug,
                                "team": f"{team1} vs {team2}",
                                "scraped_at": datetime.now(timezone.utc).isoformat(),
                            })
            
            time.sleep(0.3)
            
        except Exception as e:
            log.warning(f"Erro na query '{query}': {e}")
    
    return all_news


def upsert_games(sb: Client, games: list[dict]) -> None:
    if not games:
        return
    result = sb.table("nba_games_schedule").upsert(games, on_conflict="slug").execute()
    log.info(f"✓ {len(games)} jogos salvos")


def upsert_news(sb: Client, news: list[dict]) -> None:
    """Salva notícias removendo duplicatas"""
    if not news:
        return
    
    # Remover duplicatas por news_key
    seen_keys = set()
    unique_news = []
    
    for n in news:
        key = n.get("news_key")
        if not key:
            key = generate_news_key(n.get("title", ""), n.get("game_slug", ""))
            n["news_key"] = key
        
        if key not in seen_keys:
            seen_keys.add(key)
            unique_news.append(n)
        else:
            log.debug(f"Duplicata ignorada: {key}")
    
    # Garantir campos obrigatórios
    for n in unique_news:
        if "team" not in n or not n["team"]:
            n["team"] = "NBA"
        if "title" not in n or not n["title"]:
            n["title"] = "NBA Update"
        if "url" not in n or not n["url"]:
            n["url"] = "https://www.nba.com"
    
    log.info(f"Salvando {len(unique_news)} notícias únicas...")
    
    # Upsert em lotes menores para evitar problemas
    batch_size = 50
    total_saved = 0
    
    for i in range(0, len(unique_news), batch_size):
        batch = unique_news[i:i + batch_size]
        try:
            result = sb.table("nba_team_news").upsert(batch, on_conflict="news_key").execute()
            total_saved += len(batch)
            log.info(f"  Lote {i//batch_size + 1}: {len(batch)} notícias")
        except Exception as e:
            log.error(f"Erro ao salvar lote: {e}")
    
    log.info(f"✓ Total de {total_saved} notícias salvas")


def run():
    log.info("═══ NBA Scraper - Prévias Detalhadas ═══")
    sb = get_supabase()

    # Buscar jogos
    games = fetch_today_games()
    if not games:
        log.warning("Nenhum jogo encontrado")
        return
    
    upsert_games(sb, games)

    # Buscar notícias para cada jogo
    all_news = []
    
    for game in games:
        home = game["home_team"]
        away = game["away_team"]
        slug = game["slug"]
        
        log.info(f"Buscando notícias: {away} vs {home}")
        
        # Buscar na NewsAPI
        news = fetch_game_news_api(away, home, slug)
        
        # Se não encontrou nada, criar notícia genérica
        if not news:
            news.append({
                "news_key": generate_news_key(f"preview-{slug}", slug),
                "game_slug": slug,
                "team": f"{away} vs {home}",
                "title": f"Game Preview: {away} vs {home}",
                "url": game["nba_game_url"],
                "summary": f"Matchup scheduled for today. Visit NBA.com for injury reports and latest updates.",
                "source": "NBA.com",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
        
        all_news.extend(news)
        time.sleep(0.5)

    # Remover duplicatas globais antes de salvar
    upsert_news(sb, all_news)

    # Resumo
    log.info("\n═══ RESUMO ═══")
    for game in games:
        count = len([n for n in all_news if n.get("game_slug") == game["slug"]])
        log.info(f"{game['game_time_brt']} - {game['away_team']} vs {game['home_team']} ({count} notícias)")
    
    log.info(f"\nTotal: {len(games)} jogos | {len(all_news)} notícias")


if __name__ == "__main__":
    run()
    
