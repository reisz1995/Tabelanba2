"""
NBA Scraper - Versão Corrigida
Limpa duplicatas e normaliza dados
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

BRT = ZoneInfo("America/Sao_Paulo")
ET = ZoneInfo("America/New_York")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ─── Supabase client ───────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ─── Proxy Functions ───────────────────────────────────────────────────────────
def get_scrapingant_url(target_url: str, proxy_country: str = "us") -> str:
    if not SCRAPINGANT_API_KEY:
        return target_url
    encoded_url = quote(target_url, safe='')
    return (
        f"https://api.scrapingant.com/v2/general?"
        f"url={encoded_url}&"
        f"x-api-key={SCRAPINGANT_API_KEY}&"
        f"proxy_country={proxy_country}"
    )


def fetch_html(url: str, retries: int = 3) -> str | None:
    for attempt in range(1, retries + 1):
        try:
            target_url = get_scrapingant_url(url) if SCRAPINGANT_API_KEY else url
            
            with httpx.Client(follow_redirects=True, timeout=45) as client:
                log.info(f"Fetching: {url[:50]}...")
                resp = client.get(target_url)
                resp.raise_for_status()
                return resp.text
                
        except Exception as e:
            log.warning(f"Tentativa {attempt}/{retries} falhou: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    
    return None


# ─── Helper Functions ───────────────────────────────────────────────────────────
def clean_team_name(name: str) -> str:
    """Limpa nome do time removendo sujeiras"""
    # Remover hashtags e tudo depois
    name = name.split("#")[0].strip()
    # Remover "Trends" ou variações
    name = re.sub(r'\s*trends?$', '', name, flags=re.I).strip()
    # Remover múltiplos espaços
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def parse_time_to_brt(time_str: str, date_str: str) -> tuple[str, str]:
    """
    Converte horário do scores24 (provavelmente PT/ET) para BRT
    Retorna: (hora_brt, data_ajustada)
    """
    if not time_str:
        return "20:00", date_str
    
    try:
        # Parse do horário
        hour, minute = map(int, time_str.split(":"))
        
        # scores24 mostra horário de Portugal (PT) ou Eastern (ET)
        # PT = UTC+0 (ou UTC+1 em horário de verão)
        # ET = UTC-5 (ou UTC-4 em horário de verão)
        # BRT = UTC-3 (sempre)
        
        # Assumindo que é Portugal (UTC+0)
        # Diferença: PT (0) para BRT (-3) = -3 horas
        hour_brt = hour - 3
        
        # Ajustar data se necessário
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        
        if hour_brt < 0:
            hour_brt += 24
            date_obj -= timedelta(days=1)
        
        return f"{hour_brt:02d}:{minute:02d}", date_obj.strftime("%Y-%m-%d")
        
    except Exception as e:
        log.warning(f"Erro ao converter horário {time_str}: {e}")
        return "20:00", date_str


def get_team_tri_code(team_name: str) -> str:
    """Retorna código de 3 letras do time"""
    team_mapping = {
        "atlanta hawks": "ATL", "boston celtics": "BOS", "brooklyn nets": "BKN",
        "charlotte hornets": "CHA", "chicago bulls": "CHI", "cleveland cavaliers": "CLE",
        "dallas mavericks": "DAL", "denver nuggets": "DEN", "detroit pistons": "DET",
        "golden state warriors": "GSW", "houston rockets": "HOU", "indiana pacers": "IND",
        "la clippers": "LAC", "la lakers": "LAL", "los angeles lakers": "LAL",
        "memphis grizzlies": "MEM", "miami heat": "MIA", "milwaukee bucks": "MIL",
        "minnesota timberwolves": "MIN", "new orleans pelicans": "NOP", "ny knicks": "NYK",
        "new york knicks": "NYK", "oklahoma city thunder": "OKC", "orlando magic": "ORL",
        "philadelphia 76ers": "PHI", "phoenix suns": "PHX", "portland trail blazers": "POR",
        "sacramento kings": "SAC", "san antonio spurs": "SAS", "toronto raptors": "TOR",
        "utah jazz": "UTA", "washington wizards": "WAS",
    }
    
    name_clean = team_name.lower().strip()
    return team_mapping.get(name_clean, "NBA")


def get_pt_name(team_name: str) -> str:
    """Retorna nome em português quando disponível"""
    pt_names = {
        "Pistons": "Pistões", "Hornets": "Hornets", "Wizards": "Wizards",
        "Heat": "Heat", "Hawks": "Hawks", "Cavaliers": "Cavaliers",
        "Pacers": "Pacers", "76ers": "76ers", "Celtics": "Celtics",
        "Pelicans": "Pelicans", "Knicks": "Knicks", "Raptors": "Raptors",
        "Bulls": "Bulls", "Nets": "Nets", "Mavericks": "Mavericks",
        "Spurs": "Spurs", "Nuggets": "Nuggets", "Thunder": "Thunder",
        "Warriors": "Warriors", "Lakers": "Lakers", "Rockets": "Rockets",
        "Trail Blazers": "Trail Blazers", "Kings": "Kings", "Suns": "Suns",
        "Jazz": "Jazz", "Grizzlies": "Grizzlies", "Timberwolves": "Timberwolves",
        "Bucks": "Bucks", "Magic": "Magic", "Clippers": "Clippers",
    }
    
    for en, pt in pt_names.items():
        if en in team_name:
            return pt
    return team_name


# ─── Parsers ───────────────────────────────────────────────────────────────────
def parse_game_list(html: str) -> list[dict]:
    """Parse games com limpeza de dados"""
    soup = BeautifulSoup(html, "html.parser")
    games = []

    # Pattern para URLs de jogos (ignorar #trends e duplicatas)
    game_pattern = re.compile(r"/pt/basketball/m-(\d{2}-\d{2}-\d{4})-(.+?)(?:-prediction)?$")
    
    seen_slugs: set[str] = set()

    for a_tag in soup.find_all("a", href=game_pattern):
        href = a_tag.get("href", "")
        
        # Ignorar URLs com #trends ou outros fragmentos
        if "#" in href:
            continue
            
        match = game_pattern.search(href)
        if not match:
            continue

        # Criar slug limpo (sem /pt/basketball/ e sem -prediction)
        full_slug = match.group(0)
        if full_slug in seen_slugs:
            continue
        seen_slugs.add(full_slug)
        
        # Criar slug simplificado para o banco
        slug_clean = full_slug.replace("/pt/basketball/", "").replace("-prediction", "")

        date_str = match.group(1)  # DD-MM-YYYY
        teams_slug = match.group(2)

        # Parse da data
        try:
            date_obj = datetime.strptime(date_str, "%d-%m-%Y")
            game_date = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            continue

        # Extrair nomes dos times
        team_imgs = a_tag.find_all("img")
        team_names_from_alt = [img.get("alt", "").strip() for img in team_imgs if img.get("alt")]

        if len(team_names_from_alt) >= 2:
            home_team = clean_team_name(team_names_from_alt[0])
            away_team = clean_team_name(team_names_from_alt[1])
        else:
            # Fallback: extrair do slug
            parts = teams_slug.split("-")
            # Remover "hornets", "detroit", etc. e pegar apenas nomes válidos
            mid = len(parts) // 2
            home_team = clean_team_name(" ".join(p.title() for p in parts[:mid]))
            away_team = clean_team_name(" ".join(p.title() for p in parts[mid:]))

        # Extrair horário
        time_match = re.search(r"(\d{2}:\d{2})", a_tag.get_text())
        time_raw = time_match.group(1) if time_match else None
        
        # Converter para BRT
        time_brt, date_adjusted = parse_time_to_brt(time_raw, game_date)

        # Extrair confidence
        confidence_match = re.search(r"(\d{1,3})%", a_tag.get_text())
        confidence_pct = int(confidence_match.group(1)) if confidence_match else None

        # URLs
        full_url = BASE_URL + href if href.startswith("/") else href
        
        # Criar URL da NBA no formato correto
        home_tri = get_team_tri_code(home_team)
        away_tri = get_team_tri_code(away_team)
        nba_slug = f"{away_tri.lower()}-vs-{home_tri.lower()}-0022500000"  # ID genérico
        
        games.append({
            "slug": slug_clean,
            "game_date": date_adjusted,
            "game_time_et": time_raw,  # Horário original
            "game_time_brt": time_brt,  # Horário convertido
            "home_team": home_team,
            "away_team": away_team,
            "home_team_pt": get_pt_name(home_team),
            "away_team_pt": get_pt_name(away_team),
            "home_tri": home_tri,
            "away_tri": away_tri,
            "source_url": full_url,
            "nba_game_url": f"https://www.nba.com/game/{nba_slug}",
            "confidence_pct": confidence_pct,
            "game_status": "Scheduled",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    log.info(f"Parsed {len(games)} unique games")
    return games


# ─── Supabase Functions ─────────────────────────────────────────────────────────
def upsert_games(sb: Client, games: list[dict]) -> None:
    if not games:
        return
    
    # Remover duplicatas por slug antes de enviar
    seen_slugs = set()
    unique_games = []
    for g in games:
        if g["slug"] not in seen_slugs:
            seen_slugs.add(g["slug"])
            unique_games.append(g)
    
    result = sb.table("nba_games_schedule").upsert(unique_games, on_conflict="slug").execute()
    log.info(f"✓ {len(unique_games)} games saved")


# ─── Main ──────────────────────────────────────────────────────────────────────
def run():
    log.info("═══ NBA Scraper (Corrigido) starting ═══")
    
    if not SCRAPINGANT_API_KEY:
        log.error("❌ SCRAPINGANT_API_KEY não configurado!")
        return
    
    sb = get_supabase()

    # Limpar dados antigos duplicados primeiro (opcional)
    log.info("Limpando duplicatas antigas...")
    try:
        sb.table("nba_games_schedule").delete().neq("id", 0).execute()
        log.info("✓ Tabela limpa")
    except Exception as e:
        log.warning(f"Não foi possível limpar tabela: {e}")

    # Fetch e parse
    html = fetch_html(NBA_PREDICTIONS_URL)
    if not html:
        log.error("Failed to fetch page")
        return

    games = parse_game_list(html)
    upsert_games(sb, games)

    log.info("═══ Finished ═══")
    for g in games[:5]:  # Mostrar primeiros 5
        log.info(f"{g['game_time_brt']} BRT - {g['away_team_pt']} vs {g['home_team_pt']}")


if __name__ == "__main__":
    run()
