"""
NBA Scraper - Matriz Replicante (Modo D0 - V5 Universal)
Arquitetura de renderização JS nativa e extração por densidade de bytes.
Otimizado para baixa latência e persistência relacional (Supabase).
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

SUPABASE_URL         = _require_env("SUPABASE_URL")
SUPABASE_SERVICE_KEY = _require_env("SUPABASE_SERVICE_KEY")
SCRAPINGANT_API_KEY  = os.environ.get("SCRAPINGANT_API_KEY", "")

BASE_URL             = "https://scores24.live"
NBA_PREDICTIONS_URL  = f"{BASE_URL}/pt/basketball/l-usa-nba/predictions"

BRT = ZoneInfo("America/Sao_Paulo")
ET  = ZoneInfo("America/New_York")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Replicante] %(message)s",
)
log = logging.getLogger(__name__)


# ─── Conectores de Rede ──────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def get_scrapingant_url(target_url: str, proxy_country: str = "us") -> str:
    """Invoca o motor do Chrome (browser=true) para hidratar o DOM (React/Next.js)."""
    if not SCRAPINGANT_API_KEY:
        return target_url
    encoded = quote(target_url, safe="")
    return (
        f"https://api.scrapingant.com/v2/general?"
        f"url={encoded}&"
        f"x-api-key={SCRAPINGANT_API_KEY}&"
        f"proxy_country={proxy_country}&"
        f"browser=true" 
    )

def fetch_html(url: str, retries: int = 3) -> str | None:
    """I/O de rede com backoff exponencial."""
    for attempt in range(1, retries + 1):
        try:
            target = get_scrapingant_url(url) if SCRAPINGANT_API_KEY else url
            with httpx.Client(follow_redirects=True, timeout=60) as client:
                log.info(f"Interceptando nó: {url[:70]}...")
                resp = client.get(target)
                resp.raise_for_status()
                return resp.text
        except Exception as exc:
            log.warning(f"Anomalia de rede (tentativa {attempt}/{retries}): {exc}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    return None


# ─── Extrator de Força Bruta Universal ───────────────────────────────────────
def extract_full_prediction(html: str | None) -> str | None:
    """
    Motor Extrator V5 - Heurística Universal
    Ignora estrutura de tags rígida; concentra-se em massa crítica de bytes.
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # 1. Esquemas JSON-LD (Estatística Pura)
    valid_schemas = {"NewsArticle", "Article", "BlogPosting", "SportsArticle"}
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            payload = json.loads(script.string or "")
            items = payload if isinstance(payload, list) else [payload]
            for item in items:
                if item.get("@type") in valid_schemas:
                    body = item.get("articleBody", "").strip()
                    if len(body) > 150:
                        log.info(f"  [HUD] JSON-LD validado ({len(body)} chars)")
                        return body
        except (json.JSONDecodeError, AttributeError):
            pass

    # 2. Varredura por Strings Oficiais (UI React TestID)
    ui_containers = soup.find_all(
        attrs={"data-testid": re.compile(r"DisplayContent|Article|Prediction", re.I)}
    )
    for container in ui_containers:
        texts = [t for t in container.stripped_strings if len(t) > 30]
        text_block = "\n\n".join(texts)
        if len(text_block) > 150:
            log.info(f"  [HUD] String Box UI ({len(text_block)} chars)")
            return text_block

    # 3. Força Bruta de Densidade Textual (Ghost DOM)
    log.info("  [Matriz] Navegando por nós cegos (Densidade Universal)...")
    for hidden in soup(["script", "style", "nav", "footer", "header", "aside", "noscript", "button"]):
        hidden.decompose()

    max_len = 0
    best_text = None

    for node in soup.find_all(["div", "article", "main", "section"]):
        if node.name == "body": 
            continue
        
        texts = [t for t in node.stripped_strings if len(t) > 40]
        block = "\n\n".join(texts)
        
        if len(block) > max_len:
            max_len = len(block)
            best_text = block

    if best_text and max_len > 150:
        log.info(f"  [HUD] Varredura Cega de Nós ({max_len} chars)")
        return best_text

    # 4. Fallback de Metadados
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and len(meta.get("content", "")) > 80:
        log.info("  [HUD] Fallback de Metadados")
        return meta.get("content").strip()

    log.warning("  Falha sistémica: A estrutura do nó não possui texto detetável válido.")
    return None


# ─── Normalizadores ──────────────────────────────────────────────────────────
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
        log.warning(f"Falha na conversão temporal: {time_str}: {exc}")
        return "20:00", date_str

def get_team_tri_code(team_name: str) -> str:
    mapping = {
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
    return mapping.get(team_name.lower().strip(), "NBA")

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


# ─── Estruturador de Entidades (Parsing Principal) ───────────────────────────
def parse_game_list(html: str, target_date: str = None) -> list[dict]:
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
        date_str   = match.group(1)
        teams_slug = match.group(2)

        try:
            date_obj  = datetime.strptime(date_str, "%d-%m-%Y")
            game_date = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            continue

        time_match   = re.search(r"(\d{2}:\d{2})", a_tag.get_text())
        time_raw     = time_match.group(1) if time_match else None
        time_brt, date_adjusted = parse_time_to_brt(time_raw, game_date)

        # Barreira D0
        if target_date and date_adjusted != target_date:
            continue

        team_imgs = a_tag.find_all("img")
        alts = [img.get("alt", "").strip() for img in team_imgs if img.get("alt")]

        if len(alts) >= 2:
            home_team = clean_team_name(alts[0])
            away_team = clean_team_name(alts[1])
        else:
            parts = teams_slug.split("-")
            mid = len(parts) // 2
            home_team = clean_team_name(" ".join(p.title() for p in parts[:mid]))
            away_team = clean_team_name(" ".join(p.title() for p in parts[mid:]))

        confidence_match = re.search(r"(\d{1,3})%", a_tag.get_text())
        confidence_pct   = int(confidence_match.group(1)) if confidence_match else None

        base_href    = href.replace("-prediction", "")
        source_url   = (BASE_URL + base_href if base_href.startswith("/") else base_href)
        pred_url     = source_url + "-prediction"

        games.append({
            "slug":            slug_clean,
            "game_date":       date_adjusted,
            "game_time_et":    time_raw,
            "game_time_brt":   time_brt,
            "home_team":       home_team,
            "away_team":       away_team,
            "home_team_pt":    get_pt_name(home_team),
            "away_team_pt":    get_pt_name(away_team),
            "home_tri":        get_team_tri_code(home_team),
            "away_tri":        get_team_tri_code(away_team),
            "source_url":      source_url,
            "prediction_url":  pred_url,          
            "confidence_pct":  confidence_pct,
            "game_status":     "Scheduled",
            "tactical_prediction": None,
            "scraped_at":      datetime.now(timezone.utc).isoformat(),
        })

    log.info(f"Jogos extraídos para o vetor temporal {target_date or 'ALL'}: {len(games)}")
    return games


# ─── Transação Relacional ────────────────────────────────────────────────────
DB_FIELDS = [
    "slug", "game_date", "game_time_et", "game_time_brt",
    "home_team", "away_team", "home_team_pt", "away_team_pt",
    "home_tri", "away_tri", "source_url", "confidence_pct",
    "game_status", "tactical_prediction", "scraped_at",
]

def upsert_games(sb: Client, games: list[dict]) -> None:
    if not games:
        return
    seen, rows = set(), []
    for g in games:
        if g["slug"] not in seen:
            seen.add(g["slug"])
            rows.append({k: g[k] for k in DB_FIELDS if k in g})

    sb.table("nba_games_schedule").upsert(rows, on_conflict="slug").execute()
    log.info(f"✓ Sincronização atómica: {len(rows)} registos injetados.")


# ─── Máquina de Estados (Pipeline Sequencial) ────────────────────────────────
def run():
    log.info("═══ Inicialização Replicante V5 — Motor Híbrido ═══")

    if not SCRAPINGANT_API_KEY:
        log.error("❌ SCRAPINGANT_API_KEY omisso. Abortando matriz.")
        return

    sb   = get_supabase()
    html = fetch_html(NBA_PREDICTIONS_URL)

    if not html:
        log.error("Falha no nó raiz.")
        return

    today_brt = datetime.now(BRT).strftime("%Y-%m-%d")
    games     = parse_game_list(html, target_date=today_brt)

    if not games:
        log.info(f"Vácuo estatístico. Nenhum vetor para {today_brt}.")
        return

    # Verificação Incremental (Proteção de Quota de API)
    log.info("Auditoria de memória relacional...")
    existing_data = sb.table("nba_games_schedule").select("slug, tactical_prediction").execute()
    predictions_map = {row["slug"]: row["tactical_prediction"] for row in existing_data.data}

    log.info(f"Iniciando ciclo de hidratação para {len(games)} vetores...")
    for g in games:
        cached_pred = predictions_map.get(g["slug"])
        pred_url = g.pop("prediction_url")
        
        # Bypass de I/O se a táctica já constar no Supabase
        if cached_pred:
            g["tactical_prediction"] = cached_pred
            log.info(f"  [{g['away_tri']} @ {g['home_tri']}] → Dados preservados no cache local.")
            continue

        # Injeção via rede externa
        detail_html = fetch_html(pred_url)
        g["tactical_prediction"] = extract_full_prediction(detail_html)
        
        preview = (g["tactical_prediction"] or "")[:80].replace("\n", " ")
        log.info(f"  [{g['away_tri']} @ {g['home_tri']}] → {preview}...")
        time.sleep(1.0) # Atraso alargado para compensar a renderização JS do browser=true

    upsert_games(sb, games)

    log.info("═══ Operação Concluída ═══")
    for g in games:
        status = "✓ Extração Positiva" if g["tactical_prediction"] else "✗ Sinal Morto"
        log.info(f"  [{g['game_date']} {g['game_time_brt']} BRT] "
                 f"{g['away_tri']} @ {g['home_tri']} — {status}")

if __name__ == "__main__":
    run()
                    
