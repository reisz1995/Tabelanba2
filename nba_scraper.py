"""
NBA Scraper - Kimi/Replicante V7.0.0 (Dissecação HUD e DB Minimalista)
Correcções e Optimizacões:
  - [UPDATE] Expurgo total de modelos de IA (Groq/Gemini).
  - [UPDATE] Extracção de equipas via dissecação de interface (DOM text block) em vez de URL.
  - [FIX] Mapeamento estrito do DatabaseManager com o esquema SQL relacional simplificado.
  - [MAINTAIN] Topologia Temporal (Fuso Cruzado ET/BRT) e purificação de vectores textuais.
"""

import os
import re
import logging
import asyncio
import httpx
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional, Dict, Any, Set
from urllib.parse import quote

from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
from supabase import create_client, Client

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [NBA-V7.0.0] %(message)s",
)
log = logging.getLogger(__name__)

BRT = ZoneInfo("America/Sao_Paulo")
ET  = ZoneInfo("America/New_York")


# ─── Configuração ─────────────────────────────────────────────────────────────
def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise EnvironmentError(f"❌ Secret ausente: {name}")
    return val

class Config:
    SUPABASE_URL     = _require_env("SUPABASE_URL")
    SUPABASE_KEY     = _require_env("SUPABASE_SERVICE_KEY")
    SCRAPINGANT_KEY  = os.environ.get("SCRAPINGANT_API_KEY", "")
    BASE_URL         = "https://scores24.live"
    PREDICTIONS_URL  = f"{BASE_URL}/pt/basketball/l-usa-nba"
    CONCURRENCY_LIMIT = 3 # Limite rigoroso para poupar CPU/RAM


# ─── Modelos ──────────────────────────────────────────────────────────────────
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
    source_url: str
    confidence_pct: Optional[int] = None
    game_status: str = "Scheduled"
    scraped_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    tactical_prediction: Optional[str] = None


# ─── Rede ─────────────────────────────────────────────────────────────────────
class NetworkClient:
    def __init__(self):
        self.client    = httpx.AsyncClient(follow_redirects=True, timeout=60)
        self.semaphore = asyncio.Semaphore(Config.CONCURRENCY_LIMIT)

    async def fetch(self, url: str, retries: int = 2, use_browser: bool = False) -> Optional[str]:
        async with self.semaphore:
            for attempt in range(retries + 1):
                try:
                    target = self._prepare_url(url, use_browser=use_browser)
                    log.info(f"Fetch: {url[:60]}... (browser={use_browser})")
                    resp = await self.client.get(target)
                    
                    if resp.status_code == 409 and attempt < retries:
                        wait = 2 ** attempt
                        log.warning(f"409 retry em {wait}s...")
                        await asyncio.sleep(wait)
                        continue
                        
                    resp.raise_for_status()
                    return resp.text
                    
                except httpx.HTTPStatusError as e:
                    if attempt == retries:
                        log.warning(f"Erro HTTP {e.response.status_code}")
                        return None
                    await asyncio.sleep(1)
                except Exception as e:
                    log.warning(f"Erro rede: {e}")
                    return None
            return None

    def _prepare_url(self, url: str, use_browser: bool = False) -> str:
        if not Config.SCRAPINGANT_KEY:
            return url
        encoded = quote(url, safe="")
        browser_param = "true" if use_browser else "false"
        return (
            f"https://api.scrapingant.com/v2/general?"
            f"url={encoded}&x-api-key={Config.SCRAPINGANT_KEY}&"
            f"proxy_country=us&browser={browser_param}"
        )

    async def close(self):
        await self.client.aclose()


# ─── Retry Helper ─────────────────────────────────────────────────────────────
async def fetch_with_retry(net, url: str) -> Optional[str]:
    log.info(f"[FETCH] {url[-60:]}")
    html = await net.fetch(url, use_browser=True)
    if html:
        return html

    await asyncio.sleep(2)
    log.warning(f"[RETRY] browser=False → {url[-40:]}")
    html = await net.fetch(url, use_browser=False)
    if html:
        return html

    await asyncio.sleep(3)
    log.warning(f"[RETRY FINAL] browser=True → {url[-40:]}")
    html = await net.fetch(url, use_browser=True)

    if not html:
        log.error(f"[FAIL TOTAL] {url[-60:]}")

    return html


# ─── Extracção ────────────────────────────────────────────────────────────────
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

    def extract_games_list(self, html: str, target_date: str) -> List[GameData]:
        """
        Extracção Vectorial V7.0.0: Dissecação de Interface e Topologia Temporal.
        """
        soup = BeautifulSoup(html, "html.parser")
        games = []
        
        pattern = re.compile(r"/pt/basketball/m-(\d{2}-\d{2}-\d{4})-([^/?#]+)")
        seen_slugs: set[str] = set()

        dt_target = datetime.strptime(target_date, "%Y-%m-%d").date()
        
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/m-" not in href:
                continue

            match = pattern.search(href)
            if not match:
                continue

            url_date_str = match.group(1)
            raw_slug = match.group(2)
            url_date_obj = datetime.strptime(url_date_str, "%d-%m-%Y").date()
            
            node_text = a.get_text(separator="|", strip=True)
            node_text_lower = node_text.lower()
            time_match = re.search(r"(\d{2}:\d{2})", node_text_lower)
            
            if time_match:
                h, m = map(int, time_match.group(1).split(':'))
                et_hour = h
                
                if et_hour + 4 >= 24:
                    game_et_date = url_date_obj - timedelta(days=1)
                else:
                    game_et_date = url_date_obj
                    
                brt_h = (et_hour + 1) % 24
                game_brt_time = f"{brt_h:02d}:{m:02d}"
                
                if brt_h < et_hour:
                    game_brt_date = game_et_date + timedelta(days=1)
                else:
                    game_brt_date = game_et_date
            else:
                game_brt_date = url_date_obj - timedelta(days=1)
                game_brt_time = "20:00"

            if game_brt_date != dt_target:
                if not time_match and url_date_obj == dt_target:
                    pass
                else:
                    continue

            clean_teams = raw_slug.replace("-prediction", "")
            slug_clean = f"m-{url_date_str}-{clean_teams}"
            
            if slug_clean in seen_slugs:
                continue
            seen_slugs.add(slug_clean)

            source_url = f"{Config.BASE_URL}/pt/basketball/{slug_clean}"

            imgs = a.find_all("img")
            alts = [img.get("alt", "").strip() for img in imgs if img.get("alt")]
            
            if len(alts) >= 2:
                home, away = self.clean_team(alts[0]), self.clean_team(alts[1])
            else:
                raw_fragments = node_text.split("|")
                valid_names = [
                    t.strip() for t in raw_fragments 
                    if not re.search(r'\d|previsão|prognóstico|nossa escolha', t, re.I) and len(t.strip()) > 3
                ]
                if len(valid_names) >= 2:
                    home = self.clean_team(valid_names[0]).title()
                    away = self.clean_team(valid_names[1]).title()
                else:
                    home, away = "Equipa Casa", "Equipa Visitante"

            conf_match = re.search(r"(\d{1,3})%", node_text_lower)

            games.append(GameData(
                slug=slug_clean,
                game_date=target_date,
                game_time_et=time_match.group(1) if time_match else None,
                game_time_brt=game_brt_time,
                home_team=home,
                away_team=away,
                home_team_pt=self.translate_team(home),
                away_team_pt=self.translate_team(away),
                home_tri=self.get_tri_code(home),
                away_tri=self.get_tri_code(away),
                source_url=source_url,
                confidence_pct=int(conf_match.group(1)) if conf_match else None,
            ))

        log.info(f"  → Matriz Matemática (V7.0.0): {len(games)} partidas isoladas.")
        return games

    def extract_full_prediction(self, html: str, game: GameData) -> None:
        if not html:
            return
            
        soup = BeautifulSoup(html, "html.parser")
        game.tactical_prediction = self._extract_text_v3(soup)
        log.info(f"  → Texto extraído: {len(game.tactical_prediction) if game.tactical_prediction else 0} chars")

    def _process_text_container(self, container, min_length=150) -> Optional[str]:
        if not container:
            return None
            
        for elem in container.find_all(["button", "script", "style", "nav", "footer", "aside", "table", "form", "iframe", "ul", "ol", "a"]):
            try:
                elem.decompose()
            except:
                pass
        
        sections = []
        last_text = ""
        capture_immunity = False
        
        blacklist = {
            "registre", "bônus", "clique aqui", "cadastre-se", "promoção", 
            "termos e condições", "lucro garantido", "telegram", "whatsapp",
            "1xbit", "bet365", "betano", "1xbet", "pin-up",
            "palpite pago", "vip", "cookie"
        }
        
        stop_triggers = [
            "esta previsão vai ser correta", "total de votos", "bónus", "bônus", 
            "odds para o jogo", "posição na tabela", "estatísticas h2h", 
            "últimos jogos", "classificação", "outras previsões", "calcule seus",
            "melhores odds", "welcome bonus"
        ]
        
        for elem in container.find_all(["p", "h2", "h3", "div", "span"]):
            
            if elem.name in ["div", "span"] and elem.find(["p", "h2", "h3", "div"]):
                continue
                
            text = elem.get_text(separator=" ", strip=True)
            if not text: 
                continue
                
            text_lower = text.lower()
            is_header = elem.name in ["h2", "h3"]
            
            if any(stop in text_lower for stop in stop_triggers):
                capture_immunity = False
                continue
                
            if is_header and any(trigger in text_lower for trigger in ["previsão da redação", "nossa escolha", "prognóstico", "palpite"]):
                capture_immunity = True
                sections.append(f"\n{text}")
                last_text = text
                continue
                
            if not capture_immunity and not is_header:
                if len(text) < 45: 
                    continue
                if any(b in text_lower for b in blacklist):
                    continue
                num_count = sum(c.isdigit() for c in text)
                density = num_count / len(text) if len(text) > 0 else 0
                if density > 0.12: 
                    continue
            
            if text == last_text or text in sections:
                continue
                
            last_text = text
            
            if is_header and not capture_immunity:
                sections.append(f"\n{text}")
            else:
                sections.append(text)
        
        result = "\n\n".join(sections).strip()
        result = re.sub(r'\n{3,}', '\n\n', result)
        
        return result if len(result) >= min_length else None

    def _extract_text_v3(self, soup: BeautifulSoup) -> Optional[str]:
        main_content = soup.find("main") or soup.find("body")
        if main_content:
            text = self._process_text_container(main_content, min_length=300)
            if text:
                return text
        return None


# ─── Persistência ─────────────────────────────────────────────────────────────
class DatabaseManager:
    def __init__(self):
        self.sb: Client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)
        # V7.0.1: Remoção de 'game_time_et'. Sincronização estrita com o esquema SQL minimalista.
        self.target_columns = {
            "slug", "game_date", "game_time_brt",
            "home_team", "away_team", "home_team_pt", "away_team_pt",
            "home_tri", "away_tri", "source_url", "confidence_pct",
            "game_status", "scraped_at", "tactical_prediction"
        }

    def get_cached(self) -> Dict[str, dict]:
        res = self.sb.table("nba_games_schedule").select("slug,game_date,tactical_prediction").execute()
        return {
            row["slug"]: {
                "game_date": row.get("game_date"),
                "has_text": bool(row.get("tactical_prediction")),
            }
            for row in res.data
        }

    def upsert_games(self, games: List[GameData]):
        seen = set()
        unique = []
        for g in games:
            if g.slug not in seen:
                seen.add(g.slug)
                unique.append(g)
        
        if not unique:
            log.info("Nenhum registo persistente válido.")
            return
        
        rows = []
        for g in unique:
            row: Dict[str, Any] = {}
            for field in self.target_columns:
                row[field] = getattr(g, field, None)
            
            if not row.get("tactical_prediction"):
                log.warning(f"  → {g.slug}: SEM tactical_prediction processada.")
            
            rows.append(row)
        
        try:
            self.sb.table("nba_games_schedule").upsert(rows, on_conflict="slug").execute()
            for r in rows:
                text_ok = "✓" if r.get("tactical_prediction") else "✗"
                log.info(f"  → Registado: {r['slug'][:40]} | Vector Textual: {text_ok}")
        except Exception as e:
            log.error(f"Erro na matriz de persistência de dados: {e}")
            raise


# ─── Orquestrador ─────────────────────────────────────────────────────────────
async def main():
    log.info("═══ Motor Activo: Kimi/Replicante V7.0.0 (Base Minimalista) ═══")

    if not Config.SCRAPINGANT_KEY:
        log.error("SCRAPINGANT_API_KEY crítica não fornecida.")
        return

    net = NetworkClient()
    ext = NBAExtractor()
    db = DatabaseManager()

    try:
        html_list = await fetch_with_retry(net, Config.PREDICTIONS_URL)
        if not html_list:
            log.error("Excepção não resolvida na captura da matriz raiz.")
            return

        today = datetime.now(BRT).strftime("%Y-%m-%d")
        games = ext.extract_games_list(html_list, today)

        if not games:
            log.info(f"Aguardando novos eventos agendados para {today}.")
            return

        cache = db.get_cached()

        async def process(game: GameData) -> GameData:
            cached = cache.get(game.slug, {})
            needs_update = not cached.get("has_text")
            
            if not needs_update and cached.get("game_date") == game.game_date:
                log.info(f"[{game.away_tri} @ {game.home_tri}] → Ciclo ignorado. Cache preenchido.")
                return game

            pred_url = f"{game.source_url}-prediction"
            log.info(f"[{game.away_tri} @ {game.home_tri}] → Executando injecção de rede para vector táctico...")
            
            html = await fetch_with_retry(net, pred_url)
            
            if html:
                ext.extract_full_prediction(html, game)
                if not game.tactical_prediction:
                    log.warning(f"[{game.away_tri} @ {game.home_tri}] → Vector não passível de reconstrução. Excluído.")
            else:
                log.warning(f"[{game.away_tri} @ {game.home_tri}] → 404/Timeout no processamento do HTML.")

            return game

        results = []
        for g in games:
            try:
                result = await process(g)
                results.append(result)
            except Exception as e:
                results.append(e)
                log.error(f"[{g.away_tri} @ {g.home_tri}] → Falha Sistémica: {e}")
            # Estrangulamento forçado para evitar 409 Too Many Requests e pico de RAM
            await asyncio.sleep(4)

        valid = []
        for g, r in zip(games, results):
            if isinstance(r, Exception):
                log.error(f"[{g.away_tri} @ {g.home_tri}] → Reporte de Falha: {r}")
            else:
                valid.append(r)

        if valid:
            db.upsert_games(valid)

        with_text = sum(1 for g in valid if g.tactical_prediction)
        
        log.info(f"═══ Auditoria de Saída: {len(valid)} nodes | Vectores Limpos: {with_text} ═══")

    finally:
        await net.close()

if __name__ == "__main__":
    asyncio.run(main())
