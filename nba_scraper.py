import os
import re
import json
import logging
import asyncio
import httpx
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional, Dict, Any
from urllib.parse import quote

from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, HttpUrl
from supabase import create_client, Client

# ─── Configurações e Constantes ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [NBA-V6] %(message)s",
)
log = logging.getLogger(__name__)

BRT = ZoneInfo("America/Sao_Paulo")
ET = ZoneInfo("America/New_York")

class Config:
    SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
    SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
    SCRAPINGANT_KEY = os.environ.get("SCRAPINGANT_API_KEY", "")
    BASE_URL = "https://scores24.live"
    PREDICTIONS_URL = f"{BASE_URL}/pt/basketball/l-usa-nba/predictions"
    CONCURRENCY_LIMIT = 5  # Máximo de requisições simultâneas para evitar block

# ─── Modelos de Dados (Pydantic) ────────────────────────────────────────────
class GameData(BaseModel):
    slug: str
    game_date: str
    game_time_et: Optional[str]
    game_time_brt: str
    home_team: str
    away_team: str
    home_team_pt: str
    away_team_pt: str
    home_tri: str
    away_tri: str
    source_url: HttpUrl
    confidence_pct: Optional[int] = None
    game_status: str = "Scheduled"
    tactical_prediction: Optional[str] = None
    scraped_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

# ─── Camada de Rede (Async) ──────────────────────────────────────────────────
class NetworkClient:
    def __init__(self):
        self.client = httpx.AsyncClient(follow_redirects=True, timeout=60)
        self.semaphore = asyncio.Semaphore(Config.CONCURRENCY_LIMIT)

    async def fetch(self, url: str) -> Optional[str]:
        async with self.semaphore:
            try:
                target = self._prepare_url(url)
                log.info(f"Interceptando nó: {url[:60]}...")
                resp = await self.client.get(target)
                resp.raise_for_status()
                return resp.text
            except Exception as e:
                log.warning(f"Anomalia de rede em {url[:50]}: {e}")
                return None

    def _prepare_url(self, url: str) -> str:
        if not Config.SCRAPINGANT_KEY:
            return url
        encoded = quote(url, safe="")
        return (
            f"https://api.scrapingant.com/v2/general?"
            f"url={encoded}&x-api-key={Config.SCRAPINGANT_KEY}&"
            f"proxy_country=us&browser=true"
        )

    async def close(self):
        await self.client.aclose()

# ─── Motor de Extração ──────────────────────────────────────────────────────
class NBAExtractor:
    @staticmethod
    def clean_team(name: str) -> str:
        return re.sub(r'\s*trends?$', '', name.split("#")[0], flags=re.I).strip()

    @staticmethod
    def get_tri_code(team: str) -> str:
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
        return mapping.get(team.lower().strip(), "NBA")

    @staticmethod
    def translate_team(team: str) -> str:
        pt_names = {"Pistons": "Pistões", "Hornets": "Hornets", "Wizards": "Wizards"} # ... expansível
        for en, pt in pt_names.items():
            if en in team: return pt
        return team

    def parse_time(self, time_str: Optional[str], date_str: str) -> tuple[str, str]:
        if not time_str: return "20:00", date_str
        try:
            h, m = map(int, time_str.split(":"))
            h_brt = h - 3
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            if h_brt < 0:
                h_brt += 24
                dt -= timedelta(days=1)
            return f"{h_brt:02d}:{m:02d}", dt.strftime("%Y-%m-%d")
        except:
            return "20:00", date_str

    def extract_games_list(self, html: str, target_date: str) -> List[GameData]:
        soup = BeautifulSoup(html, "html.parser")
        games = []
        pattern = re.compile(r"/pt/basketball/m-(\d{2}-\d{2}-\d{4})-(.+?)(?:-prediction)?$")
        
        for a in soup.find_all("a", href=pattern):
            match = pattern.search(a.get("href", ""))
            if not match: continue

            date_raw = match.group(1)
            teams_slug = match.group(2)
            
            # Conversão de data
            try:
                dt_obj = datetime.strptime(date_raw, "%d-%m-%Y")
                date_iso = dt_obj.strftime("%Y-%m-%d")
            except: continue

            time_match = re.search(r"(\d{2}:\d{2})", a.get_text())
            t_brt, d_adj = self.parse_time(time_match.group(1) if time_match else None, date_iso)

            if d_adj != target_date: continue

            # Extração de times
            imgs = a.find_all("img")
            alts = [img.get("alt", "").strip() for img in imgs if img.get("alt")]
            if len(alts) >= 2:
                home, away = self.clean_team(alts[0]), self.clean_team(alts[1])
            else:
                parts = teams_slug.split("-")
                home, away = " ".join(parts[:len(parts)//2]).title(), " ".join(parts[len(parts)//2:]).title()

            conf_match = re.search(r"(\d{1,3})%", a.get_text())
            
            href = a.get("href", "")
            base_href = href.replace("-prediction", "")
            url = Config.BASE_URL + base_href if base_href.startswith("/") else base_href

            games.append(GameData(
                slug=base_href.replace("/pt/basketball/", ""),
                game_date=d_adj,
                game_time_et=time_match.group(1) if time_match else None,
                game_time_brt=t_brt,
                home_team=home,
                away_team=away,
                home_team_pt=self.translate_team(home),
                away_team_pt=self.translate_team(away),
                home_tri=self.get_tri_code(home),
                away_tri=self.get_tri_code(away),
                source_url=url,
                confidence_pct=int(conf_match.group(1)) if conf_match else None
            ))
        return games

    def extract_prediction_text(self, html: Optional[str]) -> Optional[str]:
        if not html: return None
        soup = BeautifulSoup(html, "html.parser")

        # 1. JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                body = data.get("articleBody") if isinstance(data, dict) else None
                if body and len(body) > 150: return body
            except: pass

        # 2. UI Containers & Density
        for forbidden in soup(["script", "style", "nav", "footer", "header"]):
            forbidden.decompose()

        best_text = ""
        for node in soup.find_all(["div", "article", "main"]):
            text = "\n\n".join([t for t in node.stripped_strings if len(t) > 40])
            if len(text) > len(best_text):
                best_text = text

        return best_text if len(best_text) > 150 else None

# ─── Camada de Persistência ──────────────────────────────────────────────────
class DatabaseManager:
    def __init__(self):
        self.sb: Client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)

    def get_cached_predictions(self) -> Dict[str, str]:
        res = self.sb.table("nba_games_schedule").select("slug, tactical_prediction").execute()
        return {row["slug"]: row["tactical_prediction"] for row in res.data}

    def upsert_games(self, games: List[GameData]):
        rows = [g.model_dump() for g in games]
        self.sb.table("nba_games_schedule").upsert(rows, on_conflict="slug").execute()
        log.info(f"✓ Sincronização atómica: {len(rows)} registos processados.")

# ─── Orquestrador Principal ──────────────────────────────────────────────────
async def main():
    log.info("═══ Inicializando Replicante V6 (Async Engine) ═══")
    
    if not Config.SCRAPINGANT_KEY:
        log.error("API Key ausente. Abortando.")
        return

    net = NetworkClient()
    ext = NBAExtractor()
    db = DatabaseManager()

    try:
        # 1. Coleta de Lista de Jogos
        html_list = await net.fetch(Config.PREDICTIONS_URL)
        if not html_list:
            log.error("Falha ao carregar nó raiz.")
            return

        today_brt = datetime.now(BRT).strftime("%Y-%m-%d")
        games = ext.extract_games_list(html_list, today_brt)
        
        if not games:
            log.info(f"Nenhum vetor encontrado para {today_brt}.")
            return

        # 2. Otimização de Cache
        cache = db.get_cached_predictions()
        
        # 3. Hidratação Assíncrona
        async def hydrate(game: GameData):
            if cache.get(game.slug):
                game.tactical_prediction = cache[game.slug]
                log.info(f"  [{game.away_tri} @ {game.home_tri}] → Cache Hit")
                return game

            pred_url = f"{game.source_url}-prediction"
            html_detail = await net.fetch(pred_url)
            game.tactical_prediction = ext.extract_prediction_text(html_detail)
            
            status = "✓" if game.tactical_prediction else "✗"
            log.info(f"  [{game.away_tri} @ {game.home_tri}] → {status} Extraído")
            return game

        # Processa todos os jogos em paralelo (respeitando o Semaphore)
        processed_games = await asyncio.gather(*(hydrate(g) for g in games))
        
        # 4. Persistência Final
        db.upsert_games(processed_games)
        
    finally:
        await net.close()
        log.info("═══ Operação Concluída ═══")

if __name__ == "__main__":
    asyncio.run(main())
