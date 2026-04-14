"""
NBA Scraper - Replicante V6.2 (Async Engine - Completo)
Correções:
  - [FIX] Método extract_prediction_text completo e funcional
  - [FIX] Captura texto estruturado: Introdução, Times, Pontos-chave, Conclusão
  - [FIX] Deduplicação por slug_clean
  - [FIX] Retry com backoff para 409
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
    format="%(asctime)s [%(levelname)s] [NBA-V6.2] %(message)s",
)
log = logging.getLogger(__name__)

BRT = ZoneInfo("America/Sao_Paulo")
ET  = ZoneInfo("America/New_York")


# ─── Config ───────────────────────────────────────────────────────────────────
def _require_env(name: str) -> str:
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
        async with self.semaphore:
            for attempt in range(retries + 1):
                try:
                    target = self._prepare_url(url)
                    log.info(f"Interceptando nó: {url[:70]}...")
                    resp = await self.client.get(target)
                    
                    if resp.status_code == 409 and attempt < retries:
                        wait = 2 ** attempt
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
        soup = BeautifulSoup(html, "html.parser")
        games = []
        pattern = re.compile(r"/pt/basketball/m-(\d{2}-\d{2}-\d{4})-(.+?)(?:-prediction)?$")
        seen_slugs: set[str] = set()

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

            base_href = href.replace("-prediction", "")
            slug_clean = base_href.replace("/pt/basketball/", "")
            
            if slug_clean in seen_slugs:
                continue
            seen_slugs.add(slug_clean)

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
        """
        Extrai previsão completa estruturada do DisplayContent.
        Captura: Introdução, análise dos times, Pontos-chave, Conclusão.
        """
        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")

        # 1. DisplayContent - conteúdo principal completo
        container = soup.find(attrs={"data-testid": "DisplayContent"})
        if container:
            sections = []
            last_text = ""
            
            # Remove elementos de propaganda/botões
            for unwanted in container.find_all(["button", "a", "script", "style"]):
                unwanted.decompose()
            
            # Extrai todos os elementos de texto relevantes
            for elem in container.find_all(["h1", "h2", "h3", "h4", "p", "li"], recursive=True):
                text = elem.get_text(strip=True)
                
                # Filtros de qualidade
                if not text or len(text) < 10:
                    continue
                if text == last_text:  # Evita duplicatas exatas
                    continue
                if "APOSTAR" in text or "odds" in text.lower() and "1." in text:
                    continue  # Remove linhas de apostas
                    
                last_text = text
                
                # Formatação por tipo de elemento
                if elem.name in ["h1", "h2", "h3", "h4"]:
                    sections.append(f"\n{text}\n{'=' * len(text)}")
                elif elem.name == "li":
                    sections.append(f"• {text}")
                else:
                    sections.append(text)

            result = "\n\n".join(sections).strip()
            result = re.sub(r'\n{4,}', '\n\n\n', result)  # Normaliza quebras
            
            if len(result) > 300:
                log.info(f"  Previsão extraída: {len(result)} caracteres")
                return result

        # 2. JSON-LD articleBody (fallback)
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                payload = json.loads(script.string or "")
                items = payload if isinstance(payload, list) else [payload]
                for item in items:
                    if item.get("@type") == "NewsArticle":
                        body = item.get("articleBody", "").strip()
                        if len(body) > 200:
                            log.info(f"  Previsão via JSON-LD: {len(body)} caracteres")
                            return body
            except (json.JSONDecodeError, AttributeError):
                pass

        # 3. Meta description (fallback mínimo)
        meta = soup.find("meta", attrs={"name": "description"})
        if meta:
            desc = meta.get("content", "").strip()
            if len(desc) > 80:
                return desc

        log.warning("  Nenhuma previsão encontrada.")
        return None


# ─── Persistência ─────────────────────────────────────────────────────────────
class DatabaseManager:
    def __init__(self):
        self.sb: Client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)

    def get_cached_predictions(self) -> Dict[str, dict]:
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
        seen = set()
        unique_games = []
        for g in games:
            if g.slug not in seen:
                seen.add(g.slug)
                unique_games.append(g)
            else:
                log.warning(f"Duplicata removida: {g.slug}")
        
        if not unique_games:
            log.info("Nenhum jogo para persistir.")
            return
        
        rows = [g.model_dump(mode="json") for g in unique_games]
        self.sb.table("nba_games_schedule").upsert(rows, on_conflict="slug").execute()
        log.info(f"✓ Sincronização atômica: {len(rows)} registros processados.")


# ─── Orquestrador ─────────────────────────────────────────────────────────────
async def main():
    log.info("═══ Replicante V6.2 (Async) — iniciando ═══")

    if not Config.SCRAPINGANT_KEY:
        log.error("SCRAPINGANT_API_KEY ausente. Abortando.")
        return

    net = NetworkClient()
    ext = NBAExtractor()
    db = DatabaseManager()

    try:
        html_list = await net.fetch(Config.PREDICTIONS_URL)
        if not html_list:
            log.error("Falha ao carregar página de previsões.")
            return

        today_brt = datetime.now(BRT).strftime("%Y-%m-%d")
        games = ext.extract_games_list(html_list, today_brt)

        if not games:
            log.info(f"Nenhum jogo encontrado para {today_brt}.")
            return

        cache = db.get_cached_predictions()

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

        valid_games = []
        for g, r in zip(games, results):
            if isinstance(r, Exception):
                log.error(f"  [{g.away_tri} @ {g.home_tri}] → Falha: {r}")
            else:
                valid_games.append(r)

        db.upsert_games(valid_games)

    finally:
        await net.close()
        log.info("═══ Operação Concluída ═══")


if __name__ == "__main__":
    asyncio.run(main())
          
