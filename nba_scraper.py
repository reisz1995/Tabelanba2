"""
NBA Daily Scraper - Jogos e Prévias
Usa NewsAPI para notícias reais da NBA
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
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")  # Opcional, mas recomendado

BRT = ZoneInfo("America/Sao_Paulo")
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


# ─── Helper Functions ───────────────────────────────────────────────────────────
def get_team_names() -> dict:
    """Retorna dicionário com nomes dos times em português/inglês"""
    return {
        "ATL": ("Hawks", "Hawks"), "BOS": ("Celtics", "Celtics"),
        "BKN": ("Nets", "Nets"), "CHA": ("Hornets", "Hornets"),
        "CHI": ("Bulls", "Bulls"), "CLE": ("Cavaliers", "Cavaliers"),
        "DAL": ("Mavericks", "Mavericks"), "DEN": ("Nuggets", "Nuggets"),
        "DET": ("Pistons", "Pistões"), "GSW": ("Warriors", "Warriors"),
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
    """Converte horário UTC para BRT (UTC-3)"""
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
            
            home_name_pt = team_names.get(home_tri, (home_tri, home_tri))[1]
            away_name_pt = team_names.get(away_tri, (away_tri, away_tri))[1]
            
            game_info = {
                "slug": slug,
                "game_date": datetime.now(BRT).strftime("%Y-%m-%d"),
                "game_time_brt": game_time_brt,
                "home_team": game["homeTeam"]["teamName"],
                "home_team_pt": home_name_pt,
                "away_team": game["awayTeam"]["teamName"],
                "away_team_pt": away_name_pt,
                "home_tri": home_tri,
                "away_tri": away_tri,
                "game_status": game["gameStatusText"],
                "nba_game_url": f"https://www.nba.com/game/{slug}",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
            games.append(game_info)
        
        log.info(f"Encontrados {len(games)} jogos para hoje")
        return games
        
    except Exception as e:
        log.error(f"Erro ao buscar jogos: {e}")
        return []


# ─── NewsAPI Functions ─────────────────────────────────────────────────────────
def fetch_newsapi_articles(query: str = "NBA", days_back: int = 3) -> list[dict]:
    """Busca notícias da NewsAPI"""
    if not NEWSAPI_KEY:
        log.warning("NEWSAPI_KEY não configurada")
        return []
    
    try:
        log.info(f"Buscando notícias na NewsAPI: '{query}'")
        
        # Calcular data de início (últimos X dias)
        from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": query,
            "from": from_date,
            "language": "en",
            "sortBy": "relevancy",
            "pageSize": 30,
            "apiKey": NEWSAPI_KEY,
        }
        
        with httpx.Client(timeout=30) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("status") != "ok":
                log.error(f"NewsAPI erro: {data.get('message')}")
                return []
            
            news_items = []
            for article in data.get("articles", []):
                title = article.get("title", "")
                if not title or title == "[Removed]":
                    continue
                
                # Extrair times mencionados
                mentioned_teams = extract_teams_from_title(title + " " + article.get("description", ""))
                
                news_items.append({
                    "news_key": f"newsapi-{hash(title) % 10000000}",
                    "title": title,
                    "url": article.get("url"),
                    "summary": article.get("description", "")[:400] if article.get("description") else "",
                    "published_at": article.get("publishedAt"),
                    "mentioned_teams": mentioned_teams,
                    "source": article.get("source", {}).get("name", "NewsAPI"),
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })
            
            log.info(f"NewsAPI: {len(news_items)} notícias encontradas")
            return news_items
            
    except Exception as e:
        log.error(f"Erro NewsAPI: {e}")
        return []


def fetch_top_nba_headlines() -> list[dict]:
    """Busca headlines principais da NBA"""
    if not NEWSAPI_KEY:
        return []
    
    try:
        url = "https://newsapi.org/v2/top-headlines"
        params = {
            "q": "NBA basketball",
            "country": "us",
            "category": "sports",
            "pageSize": 20,
            "apiKey": NEWSAPI_KEY,
        }
        
        with httpx.Client(timeout=30) as client:
            resp = client.get(url, params=params)
            data = resp.json()
            
            if data.get("status") != "ok":
                return []
            
            news_items = []
            for article in data.get("articles", []):
                title = article.get("title", "")
                if not title:
                    continue
                
                news_items.append({
                    "news_key": f"headline-{hash(title) % 10000000}",
                    "title": title,
                    "url": article.get("url"),
                    "summary": article.get("description", "")[:400] if article.get("description") else "",
                    "published_at": article.get("publishedAt"),
                    "mentioned_teams": extract_teams_from_title(title),
                    "source": article.get("source", {}).get("name", "NewsAPI"),
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })
            
            return news_items
            
    except Exception as e:
        log.error(f"Erro headlines: {e}")
        return []


def extract_teams_from_title(text: str) -> list[str]:
    """Extrai nomes de times do texto"""
    team_names = get_team_names()
    found_teams = []
    text_lower = text.lower()
    
    for tri, (en_name, pt_name) in team_names.items():
        if en_name.lower() in text_lower:
            found_teams.append(en_name)
    
    return list(set(found_teams))  # Remover duplicatas


def match_news_to_games(games: list[dict], all_news: list[dict]) -> list[dict]:
    """Associa notícias aos jogos baseado nos times mencionados"""
    matched_news = []
    
    for game in games:
        home_team = game["home_team"]
        away_team = game["away_team"]
        home_tri = game["home_tri"]
        away_tri = game["away_tri"]
        
        game_teams = {
            home_team.lower(), away_team.lower(),
            home_tri.lower(), away_tri.lower()
        }
        
        best_match = None
        best_score = 0
        
        for news in all_news:
            news_teams = set(t.lower() for t in news.get("mentioned_teams", []))
            
            # Calcular score de matching
            score = 0
            if home_team.lower() in news["title"].lower():
                score += 2
            if away_team.lower() in news["title"].lower():
                score += 2
            if home_tri.lower() in news["title"].lower():
                score += 1
            if away_tri.lower() in news["title"].lower():
                score += 1
            
            # Se menciona ambos os times, é uma ótima match
            if len(news_teams) >= 2 and len(news_teams.intersection(game_teams)) >= 2:
                score += 5
            
            if score > best_score:
                best_score = score
                best_match = news
        
        # Se encontrou uma boa notícia, associar ao jogo
        if best_match and best_score >= 2:
            game_news = best_match.copy()
            game_news["game_slug"] = game["slug"]
            game_news["team"] = f"{game['away_team_pt']} vs {game['home_team_pt']}"
            matched_news.append(game_news)
        else:
            # Criar notícia genérica se não encontrou
            matched_news.append({
                "news_key": f"preview-{game['slug']}",
                "game_slug": game["slug"],
                "team": f"{game['away_team_pt']} vs {game['home_team_pt']}",
                "title": f"Prévia: {game['away_team_pt']} vs {game['home_team_pt']}",
                "url": game["nba_game_url"],
                "summary": f"Confronto entre {game['away_team_pt']} e {game['home_team_pt']} às {game['game_time_brt']} BRT.",
                "source": "NBA.com",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
    
    return matched_news


# ─── Supabase Functions ─────────────────────────────────────────────────────────
def upsert_games(sb: Client, games: list[dict]) -> None:
    if not games:
        return
    
    for game in games:
        game["home_team"] = game.get("home_team_pt", game["home_team"])
        game["away_team"] = game.get("away_team_pt", game["away_team"])
        game["game_time_et"] = game.get("game_time_brt")
    
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
    log.info("═══ NBA Scraper - Jogos e Prévias (NewsAPI) ═══")
    sb = get_supabase()

    # 1. Buscar jogos do dia
    games = fetch_today_games()
    if not games:
        log.warning("Nenhum jogo encontrado para hoje")
        return
    
    upsert_games(sb, games)

    # 2. Buscar notícias da NewsAPI
    all_news = []
    
    if NEWSAPI_KEY:
        # Buscar notícias gerais da NBA
        nba_news = fetch_newsapi_articles("NBA basketball", days_back=2)
        all_news.extend(nba_news)
        
        # Buscar headlines
        headlines = fetch_top_nba_headlines()
        all_news.extend(headlines)
        
        # Buscar notícias específicas para cada confronto
        for game in games:
            query = f"{game['away_team']} {game['home_team']} NBA"
            matchup_news = fetch_newsapi_articles(query, days_back=5)
            all_news.extend(matchup_news)
            time.sleep(0.5)  # Rate limiting
        
        # Remover duplicatas
        seen = set()
        unique_news = []
        for n in all_news:
            key = n["title"]
            if key not in seen:
                seen.add(key)
                unique_news.append(n)
        all_news = unique_news
        
        log.info(f"Total de notícias únicas: {len(all_news)}")
    else:
        log.warning("NEWSAPI_KEY não configurada - usando notícias genéricas")

    # 3. Associar notícias aos jogos
    game_news = match_news_to_games(games, all_news)
    upsert_news(sb, game_news)

    # Log resumo
    log.info("\n═══ RESUMO DO DIA ═══")
    for game in games:
        log.info(f"{game['game_time_brt']} - {game['away_team_pt']} vs {game['home_team_pt']}")
    
    log.info(f"\nTotal: {len(games)} jogos | {len(game_news)} notícias")


if __name__ == "__main__":
    run()
