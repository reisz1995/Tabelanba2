"""
NBA Scraper - Replicante V6.1 (Async Engine - Corrigido)
Correções aplicadas:
  - [FIX] Deduplicação por slug_clean (não slug_raw) — evita duplicatas no Supabase
  - [FIX] Retry com backoff exponencial para erros 409 do ScrapingAnt
  - [FIX] Segurança final na persistência — remove duplicatas antes do upsert
  - [FIX] Tratamento específico de HTTPStatusError no fetch
"""

import os
import re
import json
import logging
import asyncio
import httpx
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional, Dict
from urllib.parse import quote

from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
from supabase import create_client, Client

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [NBA-V6.1] %(message)s",
)
log = logging.getLogger(__name__)

BRT = ZoneInfo("America/Sao_Paulo")
ET  = ZoneInfo("America/New_York")


# ─── Config ───────────────────────────────────────────────────────────────────
def _require_env(name: str) -> str:
    """Falha explícita se variável de ambiente obrigatória estiver ausente."""
    val = os.environ.get(name, "").strip()
    if not val:
        raise EnvironmentError(f"❌ Secret ausente: {name}")
    return val

class Config:
    SUPABASE_URL  = _require_env("SUPABASE_URL")
    SUPABASE_KEY  = _require_env("SUPABASE_SERVICE_KEY")
    SCRAPINGANT_KEY = os.environ.get("SCRAPINGANT_API_KEY", "")
    BASE_URL      = "https://scores24.live"
    PREDICTIONS_URL = f"{BASE_URL}/pt/basketball/l-usa-nba/predictions"
    CONCURRENCY_LIMIT = 5


# ─── Modelos ──────────────────────────────────────────────────────────────────
class GameData(BaseModel):
    slug:               str
    game_date:          str
    game_time_et:       Optional[str]
    game_time_brt:      str
    home_team:          str
    away_team:          str
    home_team_pt:       str
    away_team_pt:       str
    home_tri:           str
    away_tri:           str
    source_url:         str
    confidence_pct:     Optional[int] = None
    game_status:        str = "Scheduled"
    tactical_prediction: Optional[str] = None
    scraped_at:         str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ─── Camada de Rede ───────────────────────────────────────────────────────────
class NetworkClient:
    def __init__(self):
        self.client    = httpx.AsyncClient(follow_redirects=True, timeout=60)
        self.semaphore = asyncio.Semaphore(Config.CONCURRENCY_LIMIT)

    async def fetch(self, url: str, retries: int = 2) -> Optional[str]:
        """Fetch com retry automático para erros 409."""
        async with self.semaphore:
            for attempt in range(retries + 1):
                try:
                    target = self._prepare_url(url)
                    log.info(f"Interceptando nó: {url[:70]}...")
                    resp = await self.client.get(target)
                    
                    # CORREÇÃO: Retry com backoff exponencial para 409
                    if resp.status_code == 409 and attempt < retries:
                        wait = 2 ** attempt  # 1s, 2s
                        log.warning(f"409 Conflict em {url[:50]}..., retry em {wait}s (tentativa {attempt + 1}/{retries})")
                        await asyncio.sleep(wait)
                        continue
                        
                    resp.raise_for_status()
                    return resp.text
                    
                except httpx.HTTPStatusError as e:
                    if attempt == retries:
                        log.warning(f"Anomalia de rede em {url[:60]}: {e}")
                        return None
                    await asyncio.sleep(1)
                except Exception as e:
                    log.warning(f"Anomalia de rede em {url[:60]}: {e}")
                    return None
            return None

    def _prepare_url(self, url: str) -> str:
        if not Config.SCRAPINGANT_KEY:
            return url
        encoded = quote(url, safe="")
        return (
            f"https://api.scrapingant.com/v2/general?"
            f"url={encoded}&x-api-key={Config.SCRAPINGANT_KEY}&"
            f"proxy_country=us&browser=false"
        )

    async def close(self):
        await self.client.aclose()


# ─── Motor de Extração ────────────────────────────────────────────────────────
class NBAExtractor:

    _NICKNAMES = (
        "Hawks|Celtics|Nets|Hornets|Bulls|Cavaliers|Mavericks|Nuggets|Pistons|"
        "Warriors|Rockets|Pacers|Clippers|Lakers|Grizzlies|Heat|Bucks|"
        "Timberwolves|Pelicans|Knicks|Thunder|Magic|76ers|Suns|"
        "Trail Blazers|Kings|Spurs|Raptors|Jazz|Wizards"
    )
    _HEADING_RE = re.compile(
        r"^(Introdução|Conclusão|Pontos-chave"
        rf"|(?:[A-ZÁÉÍÓÚÃÕÂÊÔÇ][a-záéíóúãõâêôçA-ZÁÉÍÓÚÃÕÂÊÔÇ]{{1,20}}"
        rf"(?:\s(?:{_NICKNAMES}))))"
        r"\s+"
    )

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
            if en in team:
                return pt
        return team

    def parse_time(self, time_str: Optional[str], date_str: str) -> tuple[str, str]:
        if not time_str:
            return "20:00", date_str
        try:
            h, m = map(int, time_str.split(":"))
            h_brt = h - 3
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            if h_brt < 0:
                h_brt += 24
                dt -= timedelta(days=1)
            return f"{h_brt:02d}:{m:02d}", dt.strftime("%Y-%m-%d")
        except Exception:
            return "20:00", date_str

    def extract_games_list(self, html: str, target_date: str) -> List[GameData]:
        """Extrai lista de jogos com deduplicação correta por slug_clean."""
        soup = BeautifulSoup(html, "html.parser")
        games = []
        pattern = re.compile(r"/pt/basketball/m-(\d{2}-\d{2}-\d{4})-(.+?)(?:-prediction)?$")
        seen_slugs: set[str] = set()  # Agora armazena slug_clean

        for a in soup.find_all("a", href=pattern):
            href = a.get("href", "")
            if "#" in href:
                continue

            match = pattern.search(href)
            if not match:
                continue

            date_raw = match.group(1)
            teams_slug = match.group(2)

            try:
                dt_obj = datetime.strptime(date_raw, "%d-%m-%Y")
                date_iso = dt_obj.strftime("%Y-%m-%d")
            except ValueError:
                continue

            time_match = re.search(r"(\d{2}:\d{2})", a.get_text())
            t_brt, d_adj = self.parse_time(
                time_match.group(1) if time_match else None, date_iso
            )

            if d_adj != target_date:
                continue

            # CORREÇÃO: Calcula slug_clean ANTES da deduplicação
            base_href = href.replace("-prediction", "")
            slug_clean = base_href.replace("/pt/basketball/", "")
            
            # CORREÇÃO: Deduplica por slug_clean, não pelo href completo
            if slug_clean in seen_slugs:
                log.debug(f"Duplicata ignorada: {slug_clean}")
                continue
            seen_slugs.add(slug_clean)

            # Extrai times
            imgs = a.find_all("img")
            alts = [img.get("alt", "").strip() for img in imgs if img.get("alt")]
            if len(alts) >= 2:
                home, away = self.clean_team(alts[0]), self.clean_team(alts[1])
            else:
                parts = teams_slug.split("-")
                mid = len(parts) // 2
                home = " ".join(parts[:mid]).title()
                away = " ".join(parts[mid:]).title()

            conf_match = re.search(r"(\d{1,3})%", a.get_text())
            source_url = (Config.BASE_URL + base_href if base_href.startswith("/") else base_href)

            games.append(GameData(
                slug=slug_clean,
                game_date=d_adj,
                game_time_et=time_match.group(1) if time_match else None,
                game_time_brt=t_brt,
                home_team=home,
                away_team=away,
                home_team_pt=self.translate_team(home),
                away_team_pt=self.translate_team(away),
                home_tri=self.get_tri_code(home),
                away_tri=self.get_tri_code(away),
                source_url=source_url,
                confidence_pct=int(conf_match.group(1)) if conf_match else None,
            ))

        log.info(f"Jogos extraídos para {target_date}: {len(games)}")
        return games

    def extract_prediction_text(self, html: Optional[str]) -> Optional[str]:
        """Extrai previsão completa estruturada."""
        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")

        # 1. DisplayContent (principal)
        container = soup.find(attrs={"data-testid": "DisplayContent"})
        if container:
            sections, seen = [], set()
            for p in container.find_all("p", recursive=True):
                text = p.get_text(separator=" ", strip=True)
                if not text or len(text) < 20:
                    continue
                key = text[:60]
                if key in seen:
                    continue
                seen.add(key)
                m = self._HEADING_RE.match(text)
                if m:
                    sections.append(f"{m.group(1)}\n{text[m.end():].strip()}")
                else:
                    sections.append(text)

            result = "\n\n".join(sections).strip()
            if len(result) > 200:
                log.info(f"  Previsão via DisplayContent ({len(result)} chars)")
                return result

        # 2. JSON-LD articleBody
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                payload = json.loads(script.string or "")
                items = payload if isinstance(payload, list) else [payload]
                for item in items:
                    if item.get("@type") == "NewsArticle":
                        body = item.get("articleBody", "").strip()
                        if len(body) > 150:
                            log.info(f"  Previsão via JSON-LD ({len(body)} chars)")
                            return body
            except (json.JSONDecodeError, AttributeError):
                pass

        # 3. Meta description (fallback)
        meta = soup.find("meta", attrs={"name": "description"})
        if meta:
            desc = meta.get("content", "").strip()
            if len(desc) > 80:
                log.info(f"  Previsão via meta description ({len(desc)} chars)")
                return desc

        log.warning("  Nenhuma previsão encontrada.")
        return None


# ─── Persistência ─────────────────────────────────────────────────────────────
class DatabaseManager:
    def __init__(self):
        self.sb: Client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)

    def get_cached_predictions(self) -> Dict[str, dict]:
        """Retorna {slug: {prediction, game_date}} para validação de cache."""
        res = (
            self.sb.table("nba_games_schedule")
            .select("slug, tactical_prediction, game_date")
            .execute()
        )
        return {
            row["slug"]: {
                "prediction": row["tactical_prediction"],
                "game_date": row["game_date"],
            }
            for row in res.data
        }

    def upsert_games(self, games: List[GameData]):
        """Persiste jogos com deduplicação de segurança."""
        # CORREÇÃO: Remove duplicatas por slug antes do upsert
        seen = set()
        unique_games = []
        for g in games:
            if g.slug not in seen:
                seen.add(g.slug)
                unique_games.append(g)
            else:
                log.warning(f"Duplicata removida antes do upsert: {g.slug}")
        
        if not unique_games:
            log.info("Nenhum jogo para persistir.")
            return
        
        rows = [g.model_dump(mode="json") for g in unique_games]
        self.sb.table("nba_games_schedule").upsert(rows, on_conflict="slug").execute()
        log.info(f"✓ Sincronização atômica: {len(rows)} registros processados.")


# ─── Orquestrador ─────────────────────────────────────────────────────────────
async def main():
    log.info("═══ Replicante V6.1 (Async) — iniciando ═══")

    if not Config.SCRAPINGANT_KEY:
        log.error("SCRAPINGANT_API_KEY ausente. Abortando.")
        return

    net = NetworkClient()
    ext = NBAExtractor()
    db = DatabaseManager()

    try:
        # 1. Lista de jogos
        html_list = await net.fetch(Config.PREDICTIONS_URL)
        if not html_list:
            log.error("Falha ao carregar página de previsões.")
            return

        today_brt = datetime.now(BRT).strftime("%Y-%m-%d")
        games = ext.extract_games_list(html_list, today_brt)

        if not games:
            log.info(f"Nenhum jogo encontrado para {today_brt}.")
            return

        # 2. Cache com validação de data
        cache = db.get_cached_predictions()

        # 3. Hidratação assíncrona
        async def hydrate(game: GameData) -> GameData:
            cached = cache.get(game.slug, {})
            if cached.get("prediction") and cached.get("game_date") == game.game_date:
                game.tactical_prediction = cached["prediction"]
                log.info(f"  [{game.away_tri} @ {game.home_tri}] → Cache Hit")
                return game

            pred_url = f"{game.source_url}-prediction"
            html_detail = await net.fetch(pred_url)
            game.tactical_prediction = ext.extract_prediction_text(html_detail)

            status = "✓" if game.tactical_prediction else "✗"
            log.info(f"  [{game.away_tri} @ {game.home_tri}] → {status} Extraído")
            return game

        results = await asyncio.gather(
            *(hydrate(g) for g in games),
            return_exceptions=True,
        )

        # Filtra exceções
        valid_games = []
        for g, r in zip(games, results):
            if isinstance(r, Exception):
                log.error(f"  [{g.away_tri} @ {g.home_tri}] → Falha: {r}")
            else:
                valid_games.append(r)

        # 4. Persistência
        db.upsert_games(valid_games)

    finally:
        await net.close()
        log.info("═══ Operação Concluída ═══")


if __name__ == "__main__":
    asyncio.run(main())
          
