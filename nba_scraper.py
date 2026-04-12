"""
NBA Scraper - Arquitetura Replicante
Ingestão de metadados de partidas e extração vetorial JSON-LD para prognósticos.
Otimizado para baixa latência de memória (MX Linux / Android hosts).
"""

import os
import re
import json
import logging
import httpx
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from supabase import create_client, Client
from urllib.parse import quote

# ─── Configuração de Matriz ──────────────────────────────────────────────────
def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise EnvironmentError(f"\n\n❌ Missing required secret in HUD: {name}")
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
    format="%(asctime)s [%(levelname)s] [Estatístico Chefe] %(message)s",
)
log = logging.getLogger(__name__)


# ─── Conectores ─────────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

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
    """I/O de rede com backoff exponencial."""
    for attempt in range(1, retries + 1):
        try:
            target_url = get_scrapingant_url(url) if SCRAPINGANT_API_KEY else url
            with httpx.Client(follow_redirects=True, timeout=45) as client:
                log.info(f"Interceptando: {url[:60]}...")
                resp = client.get(target_url)
                resp.raise_for_status()
                return resp.text
        except Exception as exc:
            log.warning(f"Anomalia de rede (Tentativa {attempt}/{retries}): {exc}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    return None


# ─── Extratores de Dados Estruturados ────────────────────────────────────────
def extract_tactical_prediction(html_payload: str) -> str | None:
    """
    Motor de extração cirúrgica JSON-LD.
    Complexidade: O(S), onde S é o número de blocos <script>.
    """
    if not html_payload:
        return None
        
    soup = BeautifulSoup(html_payload, "html.parser")
    ld_scripts = soup.find_all("script", type="application/ld+json")
    
    for node in ld_scripts:
        if not node.string:
            continue
        try:
            payload = json.loads(node.string.strip())
            if payload.get("@type") == "NewsArticle":
                return payload.get("articleBody", "").strip()
        except json.JSONDecodeError:
            pass
            
    return None


# ─── Parsers e Normalizadores ────────────────────────────────────────────────
def clean_team_name(name: str) -> str:
    name = name.split("#")[0].strip()
    name = re.sub(r'\s*trends?$', '', name, flags=re.I).strip()
    return re.sub(r'\s+', ' ', name).strip()

def parse_time_to_brt(time_str: str, date_str: str) -> tuple[str, str]:
    if not time_str:
        return "20:00", date_str
    try:
        hour, minute = map(int, time_str.split(":"))
        hour_brt = hour - 3
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        if hour_brt < 0:
            hour_brt += 24
            date_obj -= timedelta(days=1)
        return f"{hour_brt:02d}:{minute:02d}", date_obj.strftime("%Y-%m-%d")
    except Exception as exc:
        log.warning(f"Falha na conversão temporal {time_str}: {exc}")
        return "20:00", date_str

def get_team_tri_code(team_name: str) -> str:
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
    return team_mapping.get(team_name.lower().strip(), "NBA")

def get_pt_name(team_name: str) -> str:
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

def parse_game_list(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    games = []
    game_pattern = re.compile(r"/pt/basketball/m-(\d{2}-\d{2}-\d{4})-(.+?)(?:-prediction)?$")
    seen_slugs: set[str] = set()

    for a_tag in soup.find_all("a", href=game_pattern):
        href = a_tag.get("href", "")
        if "#" in href:
            continue
            
        match = game_pattern.search(href)
        if not match:
            continue

        full_slug = match.group(0)
        if full_slug in seen_slugs:
            continue
        seen_slugs.add(full_slug)
        
        slug_clean = full_slug.replace("/pt/basketball/", "").replace("-prediction", "")
        date_str = match.group(1)
        teams_slug = match.group(2)

        try:
            date_obj = datetime.strptime(date_str, "%d-%m-%Y")
            game_date = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            continue

        team_imgs = a_tag.find_all("img")
        team_names_from_alt = [img.get("alt", "").strip() for img in team_imgs if img.get("alt")]

        if len(team_names_from_alt) >= 2:
            home_team = clean_team_name(team_names_from_alt[0])
            away_team = clean_team_name(team_names_from_alt[1])
        else:
            parts = teams_slug.split("-")
            mid = len(parts) // 2
            home_team = clean_team_name(" ".join(p.title() for p in parts[:mid]))
            away_team = clean_team_name(" ".join(p.title() for p in parts[mid:]))

        time_match = re.search(r"(\d{2}:\d{2})", a_tag.get_text())
        time_raw = time_match.group(1) if time_match else None
        time_brt, date_adjusted = parse_time_to_brt(time_raw, game_date)

        confidence_match = re.search(r"(\d{1,3})%", a_tag.get_text())
        confidence_pct = int(confidence_match.group(1)) if confidence_match else None

        full_url = BASE_URL + href if href.startswith("/") else href
        
        home_tri = get_team_tri_code(home_team)
        away_tri = get_team_tri_code(away_team)
        nba_slug = f"{away_tri.lower()}-vs-{home_tri.lower()}-0022500000"
        
        games.append({
            "slug": slug_clean,
            "game_date": date_adjusted,
            "game_time_et": time_raw,
            "game_time_brt": time_brt,
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
            "tactical_prediction": None,  # Placeholder a ser preenchido
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    log.info(f"Parsing inicial concluído: {len(games)} entidades extraídas.")
    return games


# ─── Mutação de Estado ───────────────────────────────────────────────────────
def upsert_games(sb: Client, games: list[dict]) -> None:
    if not games:
        return
    seen_slugs = set()
    unique_games = []
    for g in games:
        if g["slug"] not in seen_slugs:
            seen_slugs.add(g["slug"])
            unique_games.append(g)
    
    sb.table("nba_games_schedule").upsert(unique_games, on_conflict="slug").execute()
    log.info(f"✓ Sincronização atômica finalizada: {len(unique_games)} registros persistidos.")


# ─── Sequenciador Principal ──────────────────────────────────────────────────
def run():
    log.info("═══ Inicializando Sequência Scraper Replicante ═══")
    
    if not SCRAPINGANT_API_KEY:
        log.error("❌ Falha de ignição: SCRAPINGANT_API_KEY ausente.")
        return
    
    sb = get_supabase()

    log.info("Limpando sobreposições de dados obsoletos na interface...")
    try:
        sb.table("nba_games_schedule").delete().neq("id", 0).execute()
        log.info("✓ Espaço de memória relacional otimizado.")
    except Exception as exc:
        log.warning(f"Erro na limpeza de nós da tabela: {exc}")

    html = fetch_html(NBA_PREDICTIONS_URL)
    if not html:
        log.error("Falha crítica no nó raiz (Predictions HTML).")
        return

    # Estágio 1: Extração Estrutural Base
    games = parse_game_list(html)

    # Estágio 2: Enriquecimento Assíncrono/Iterativo (Prognósticos)
    log.info("Iniciando injeção do texto tático preditivo em instâncias individuais...")
    for g in games:
        target_prediction_url = g["source_url"]
        if not target_prediction_url.endswith("-prediction"):
            target_prediction_url += "-prediction"
            
        detail_html = fetch_html(target_prediction_url)
        prediction_text = extract_tactical_prediction(detail_html)
        
        g["tactical_prediction"] = prediction_text
        # Limitador de requisição opcional para evitar banimentos agressivos se não usar proxy rotativo
        time.sleep(0.5)

    # Estágio 3: Persistência
    upsert_games(sb, games)

    log.info("═══ Operação Concluída ═══")
    for g in games[:3]:
        hud_status = "Com Previsão" if g["tactical_prediction"] else "Sem Previsão"
        log.info(f"[{g['game_time_brt']} BRT] {g['away_team_tri']} vs {g['home_team_tri']} | {hud_status}")

if __name__ == "__main__":
    run()
    
