"""
NBA Daily Scraper - Jogos e Prévias
Busca jogos do dia e notícias de confronto
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
from nba_api.stats.static import teams

# ─── Config ────────────────────────────────────────────────────────────────────
def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise EnvironmentError(f"\n\n❌ Missing required secret: {name}")
    return val

SUPABASE_URL = _require_env("SUPABASE_URL")
SUPABASE_SERVICE_KEY = _require_env("SUPABASE_SERVICE_KEY")

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
        return "20:00"  # Default


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
            
            # Criar slug: det-vs-cha-0022501171
            slug = f"{away_tri.lower()}-vs-{home_tri.lower()}-{game_id}"
            
            # Converter horário para BRT
            game_time_brt = convert_to_brt(game.get("gameTimeUTC", ""))
            
            # Nomes em português quando disponível
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


# ─── News Scraping ─────────────────────────────────────────────────────────────
def fetch_nba_com_news() -> list[dict]:
    """Busca notícias da NBA.com"""
    news_items = []
    
    try:
        # Página de notícias da NBA
        url = "https://www.nba.com/news"
        
        with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Buscar artigos de notícias
            articles = soup.find_all("article", class_=re.compile(r"ArticleList|news", re.I))
            
            for article in articles[:15]:
                title_elem = article.find(["h2", "h3", "a"], class_=re.compile(r"title|headline", re.I))
                link_elem = article.find("a", href=True)
                time_elem = article.find("time")
                desc_elem = article.find("p", class_=re.compile(r"description|excerpt", re.I))
                
                if not title_elem or not link_elem:
                    continue
                
                title = title_elem.get_text(strip=True)
                link = link_elem["href"]
                if not link.startswith("http"):
                    link = f"https://www.nba.com{link}"
                
                # Extrair times mencionados
                mentioned_teams = extract_teams_from_title(title)
                
                news_items.append({
                    "news_key": f"nba-{hash(title) % 10000000}",
                    "title": title,
                    "url": link,
                    "published_at": time_elem.get("datetime") if time_elem else None,
                    "summary": desc_elem.get_text(strip=True)[:300] if desc_elem else "",
                    "mentioned_teams": mentioned_teams,
                    "source": "NBA.com",
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })
        
        log.info(f"NBA.com: {len(news_items)} notícias")
        return news_items
        
    except Exception as e:
        log.error(f"Erro ao buscar notícias NBA.com: {e}")
        return []


def fetch_espn_nba_news() -> list[dict]:
    """Busca notícias da ESPN NBA"""
    news_items = []
    
    try:
        url = "https://www.espn.com/nba/"
        
        with httpx.Client(headers=HEADERS, timeout=30) as client:
            resp = client.get(url)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Buscar headlines
            headlines = soup.find_all("a", class_=re.compile(r"headline|story-link", re.I))
            
            for headline in headlines[:10]:
                title = headline.get_text(strip=True)
                link = headline.get("href", "")
                
                if not title or len(title) < 20:
                    continue
                
                if not link.startswith("http"):
                    link = f"https://www.espn.com{link}"
                
                mentioned_teams = extract_teams_from_title(title)
                
                news_items.append({
                    "news_key": f"espn-{hash(title) % 10000000}",
                    "title": title,
                    "url": link,
                    "mentioned_teams": mentioned_teams,
                    "source": "ESPN",
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })
        
        log.info(f"ESPN: {len(news_items)} notícias")
        return news_items
        
    except Exception as e:
        log.error(f"Erro ao buscar notícias ESPN: {e}")
        return []


def extract_teams_from_title(title: str) -> list[str]:
    """Extrai nomes de times do título da notícia"""
    team_names = get_team_names()
    found_teams = []
    title_lower = title.lower()
    
    for tri, (en_name, pt_name) in team_names.items():
        if en_name.lower() in title_lower or pt_name.lower() in title_lower:
            found_teams.append(en_name)
    
    return found_teams


def match_news_to_games(games: list[dict], all_news: list[dict]) -> list[dict]:
    """Associa notícias aos jogos baseado nos times mencionados"""
    matched_news = []
    
    for game in games:
        home_team = game["home_team"]
        away_team = game["away_team"]
        game_teams = {home_team.lower(), away_team.lower()}
        
        for news in all_news:
            news_teams = set(t.lower() for t in news.get("mentioned_teams", []))
            
            # Se a notícia menciona ambos os times do jogo
            if len(news_teams) >= 2 and len(news_teams.intersection(game_teams)) >= 2:
                game_news = news.copy()
                game_news["game_slug"] = game["slug"]
                game_news["team"] = f"{game['away_team']} vs {game['home_team']}"
                matched_news.append(game_news)
            # Ou se menciona um dos times
            elif len(news_teams.intersection(game_teams)) >= 1:
                game_news = news.copy()
                game_news["game_slug"] = game["slug"]
                game_news["team"] = game["home_team"] if home_team.lower() in news_teams else game["away_team"]
                matched_news.append(game_news)
    
    # Remover duplicatas
    seen = set()
    unique_news = []
    for n in matched_news:
        key = n["news_key"]
        if key not in seen:
            seen.add(key)
            unique_news.append(n)
    
    return unique_news


# ─── Supabase Functions ─────────────────────────────────────────────────────────
def upsert_games(sb: Client, games: list[dict]) -> None:
    if not games:
        return
    
    # Preparar dados para o schema
    for game in games:
        game["home_team"] = game.get("home_team_pt", game["home_team"])
        game["away_team"] = game.get("away_team_pt", game["away_team"])
        game["game_time_et"] = game.get("game_time_brt")
    
    result = sb.table("nba_games_schedule").upsert(games, on_conflict="slug").execute()
    log.info(f"✓ {len(games)} jogos salvos")


def upsert_news(sb: Client, news: list[dict]) -> None:
    if not news:
        return
    
    # Garantir que news_key existe
    for n in news:
        if "news_key" not in n:
            n["news_key"] = f"news-{hash(n['title']) % 10000000}"
        if "team" not in n:
            n["team"] = "NBA"
    
    result = sb.table("nba_team_news").upsert(news, on_conflict="news_key").execute()
    log.info(f"✓ {len(news)} notícias salvas")


# ─── Main ──────────────────────────────────────────────────────────────────────
def run():
    log.info("═══ NBA Scraper - Jogos e Prévias ═══")
    sb = get_supabase()

    # 1. Buscar jogos do dia
    games = fetch_today_games()
    if not games:
        log.warning("Nenhum jogo encontrado para hoje")
        return
    
    upsert_games(sb, games)

    # 2. Buscar notícias de múltiplas fontes
    nba_news = fetch_nba_com_news()
    espn_news = fetch_espn_nba_news()
    all_news = nba_news + espn_news
    
    # 3. Associar notícias aos jogos
    game_news = match_news_to_games(games, all_news)
    
    # 4. Se não encontrou notícias específicas, criar entrada genérica
    if not game_news:
        for game in games:
            game_news.append({
                "news_key": f"preview-{game['slug']}",
                "game_slug": game["slug"],
                "team": f"{game['away_team_pt']} vs {game['home_team_pt']}",
                "title": f"Prévia: {game['away_team_pt']} vs {game['home_team_pt']}",
                "url": game["nba_game_url"],
                "summary": f"Confronto entre {game['away_team_pt']} e {game['home_team_pt']} às {game['game_time_brt']}",
                "source": "NBA.com",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
    
    upsert_news(sb, game_news)

    # Log resumo
    log.info("\n═══ RESUMO DO DIA ═══")
    for game in games:
        log.info(f"{game['game_time_brt']} - {game['away_team_pt']} vs {game['home_team_pt']}")
    
    log.info(f"\nTotal: {len(games)} jogos | {len(game_news)} notícias")


if __name__ == "__main__":
    run()
        
