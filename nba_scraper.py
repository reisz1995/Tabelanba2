"""
NBA Scraper - Replicante V6.6.2 (FIX: Isolamento da Previsão da Redação)
Correções:
  - [FIX-CRITICAL] Estratégia 1 isola APENAS o texto entre "Previsão da Redação" 
    e o próximo separador (NOSSA ESCOLHA, votação, etc). Não pega o container pai.
  - [FIX-1] Ordem dos times: alts[0]=away, alts[1]=home
  - [FIX-2] parse_time retorna data ajustada para BRT
  - [FIX-3] _extract_form com seletores específicos por time
  - [FIX-4] _extract_news captura nomes compostos
  - [FIX-5] _extract_stats com posição relativa
  - [FIX-6] H2H com deduplicação
  - [FIX-7] Junk filter contextual
  - [FIX-8] Não pula divs pais indevidamente
  - [FIX-9] Processamento paralelo real
  - [FIX-10] Hornets = CHO
  - [FIX-11] Confidence mais granular
  - [FIX-12] Delay adaptativo com jitter
  - [FIX-13] Groq prompt limitado
  - [FIX-14] Regex slug robusto
  - [FIX-16] Log de rejeitados
  - [FIX-17] Quality score não negativo
"""

import os
import re
import json
import logging
import asyncio
import random
import httpx
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional, Dict, Any, Set
from urllib.parse import quote

from bs4 import BeautifulSoup, NavigableString
from pydantic import BaseModel, Field
from supabase import create_client, Client

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [NBA-V6.6.2] %(message)s",
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
    GROQ_API_KEY     = os.environ.get("GROQ_API_KEY", "")
    GROQ_MODEL       = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
    BASE_URL         = "https://scores24.live"
    PREDICTIONS_URL  = f"{BASE_URL}/pt/basketball/l-usa-nba/predictions"
    CONCURRENCY_LIMIT = 5
    MAX_GROQ_TOKENS   = 3500


# ─── Modelos ──────────────────────────────────────────────────────────────────

class OddsData(BaseModel):
    v1: Optional[float] = None
    x: Optional[float] = None
    v2: Optional[float] = None

class H2HMatch(BaseModel):
    date: str
    home_team: str
    away_team: str
    home_score: int
    away_score: int

class H2HData(BaseModel):
    total_matches: int = 0
    home_wins: int = 0
    away_wins: int = 0
    home_win_pct: float = 0.0
    recent_matches: List[H2HMatch] = []

class RecentForm(BaseModel):
    date: str
    opponent: str
    is_home: bool
    result: str
    team_score: int
    opponent_score: int

class TeamForm(BaseModel):
    team_name: str
    position_conference: Optional[str] = None
    wins_last_10: Optional[int] = None
    recent_matches: List[RecentForm] = []

class Injury(BaseModel):
    player: str
    status: str
    details: Optional[str] = None

class Lineup(BaseModel):
    probable: List[str] = []
    confirmed: List[str] = []
    doubts: List[str] = []

class TeamNews(BaseModel):
    injuries: List[Injury] = []
    lineup: Lineup = Field(default_factory=Lineup)

class TopScorer(BaseModel):
    player: str
    ppg: float

class ThreePointStats(BaseModel):
    pct: float
    rank: Optional[str] = None

class TeamStats(BaseModel):
    points_scored_avg: Optional[float] = None
    points_allowed_avg: Optional[float] = None
    top_scorer: Optional[TopScorer] = None
    three_point: Optional[ThreePointStats] = None

class EditorialPick(BaseModel):
    recommendation: str
    handicap_line: Optional[str] = None
    odds: Optional[float] = None
    explanation: Optional[str] = None
    confidence_level: Optional[str] = None
    market_type: Optional[str] = None

class GroqInsight(BaseModel):
    confidence_score: float = Field(..., ge=0, le=5)
    fair_line: str
    edge_percentage: float
    key_factors: List[str]
    recommendation: str
    stake_units: float = Field(..., ge=0.5, le=5)
    reasoning: str
    generated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

class GameData(BaseModel):
    slug: str
    game_date: str
    game_date_brt: str
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

    odds: Optional[OddsData] = None
    h2h: Optional[H2HData] = None
    home_form: Optional[TeamForm] = None
    away_form: Optional[TeamForm] = None
    home_news: Optional[TeamNews] = None
    away_news: Optional[TeamNews] = None
    home_stats: Optional[TeamStats] = None
    away_stats: Optional[TeamStats] = None
    editorial_pick: Optional[EditorialPick] = None

    tactical_prediction: Optional[str] = None
    groq_insight: Optional[GroqInsight] = None

    extraction_method: Optional[str] = None
    text_quality_score: Optional[float] = None

    def to_groq_prompt(self) -> str:
        def implied_prob(odds: float) -> float:
            return 100 / odds if odds > 0 else 0

        home_implied = implied_prob(self.odds.v1) if self.odds and self.odds.v1 else 0
        away_implied = implied_prob(self.odds.v2) if self.odds and self.odds.v2 else 0

        home_injuries = ", ".join([f"{i.player} ({i.status})" for i in self.home_news.injuries]) if self.home_news else "Nenhuma"
        away_injuries = ", ".join([f"{i.player} ({i.status})" for i in self.away_news.injuries]) if self.away_news else "Nenhuma"

        home_form_str = f"{self.home_form.wins_last_10}/10" if self.home_form and self.home_form.wins_last_10 else "N/A"
        away_form_str = f"{self.away_form.wins_last_10}/10" if self.away_form and self.away_form.wins_last_10 else "N/A"

        editorial_hint = ""
        if self.editorial_pick:
            expl = (self.editorial_pick.explanation or "N/A")[:200]
            editorial_hint = f"""\n## ESCOLHA DA REDAÇÃO\n- Recomendação: {self.editorial_pick.recommendation}\n- Handicap: {self.editorial_pick.handicap_line or \'N/A\'}\n- Odds: {self.editorial_pick.odds or \'N/A\'}\n- Confiança: {self.editorial_pick.confidence_level or \'N/A\'}\n- Explicação: {expl}"""

        max_analysis = min(2000, Config.MAX_GROQ_TOKENS * 3)
        analysis = (self.tactical_prediction or "N/A")[:max_analysis]

        return f"""Você é o Estatístico Chefe de NBA. Analise este jogo.

## DADOS
{self.away_team} @ {self.home_team} | {self.game_date}

## MERCADO
- V1: {self.odds.v1 if self.odds else \'N/A\'} ({home_implied:.1f}%)
- V2: {self.odds.v2 if self.odds else \'N/A\'} ({away_implied:.1f}%)

## CONTEXTO
H2H: {self.h2h.total_matches if self.h2h else \'N/A\'} jogos
Forma: Casa {home_form_str} vs Visitante {away_form_str}
Lesões Casa: {home_injuries}
Lesões Visitante: {away_injuries}{editorial_hint}

## ANÁLISE COMPLETA
{analysis}

---
DIRETRIZ: Use modelo linear-pessimista. Desconte margem de segurança nas médias ofensivas.

Retorne JSON:
{{
  "confidence_score": 0.0 a 5.0,
  "fair_line": "ex: O/U 215.5",
  "edge_percentage": 0.0 a 50.0,
  "key_factors": ["fator 1", "fator 2"],
  "recommendation": "OVER/UNDER/FAVORITE/DOG/PASS",
  "stake_units": 0.5 a 5.0,
  "reasoning": "explicação em português"
}}"""


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

                    if resp.status_code == 429 and attempt < retries:
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        log.warning(f"429 retry em {retry_after}s...")
                        await asyncio.sleep(retry_after)
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

    async def post_groq(self, prompt: str) -> Optional[GroqInsight]:
        if not Config.GROQ_API_KEY:
            log.warning("GROQ_API_KEY não configurada")
            return None

        try:
            async with self.semaphore:
                log.info("  → Groq processando...")
                response = await self.client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {Config.GROQ_API_KEY}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": Config.GROQ_MODEL,
                        "messages": [
                            {"role": "system", "content": "Você é um analista de NBA especializado em betting. Responda APENAS em JSON válido."},
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.3,
                        "max_tokens": 1500,
                        "response_format": {"type": "json_object"}
                    },
                    timeout=30
                )

                response.raise_for_status()
                data = response.json()
                content = data["choices"][0]["message"]["content"]
                parsed = json.loads(content)

                insight = GroqInsight(**parsed)
                log.info(f"  ← Groq: {insight.recommendation} (conf: {insight.confidence_score}/5)")
                return insight

        except Exception as e:
            log.error(f"Erro Groq: {e}")
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


# ─── Fetch com retry adaptativo ───────────────────────────────────────────────
async def fetch_with_retry(net, url: str) -> Optional[str]:
    log.info(f"[FETCH] {url[-60:]}")

    html = await net.fetch(url, use_browser=True)
    if html:
        return html

    await asyncio.sleep(1.5 + random.uniform(0, 0.5))

    log.warning(f"[RETRY] browser=False → {url[-40:]}")
    html = await net.fetch(url, use_browser=False)
    if html:
        return html

    await asyncio.sleep(2.5 + random.uniform(0, 1.0))

    log.warning(f"[RETRY FINAL] browser=True → {url[-40:]}")
    html = await net.fetch(url, use_browser=True)

    if not html:
        log.error(f"[FAIL TOTAL] {url[-60:]}")

    return html


# ─── Extração ─────────────────────────────────────────────────────────────────
class NBAExtractor:

    JUNK_KEYWORDS_HARD = [
        "registre", "bônus", "cadastre-se", "promoção", "termos e condições",
        "ganhe até", "cashback", "freebet", "depósito", "regulamento",
        "publicidade", "anuncie", "patrocinador", "parceiro oficial",
        "compartilhe", "menu", "início", "cookies", "privacidade",
        "política de uso", "lgpd", "copyright", "todos os direitos", "©", "®", "™",
    ]

    JUNK_KEYWORDS_SOFT = [
        "apostar", "bet", "clique aqui", "odds", "resultados ao vivo", 
        "ao vivo", "live", "streaming", "veja também", "leia mais",
        "artigos relacionados", "facebook", "twitter", "instagram",
    ]

    ODDS_PATTERNS = re.compile(r'\d+\.\d+\*?|\d+\.\d+\s*(?:%|por cento)')

    @staticmethod
    def clean_team(name: str) -> str:
        return re.sub(r'\s*trends?$', '', name.split("#")[0], flags=re.I).strip()

    @staticmethod
    def get_tri_code(team: str) -> str:
        mapping = {
            "atlanta hawks": "ATL", "boston celtics": "BOS", "brooklyn nets": "BKN",
            "charlotte hornets": "CHO", "chicago bulls": "CHI", "cleveland cavaliers": "CLE",
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
        pattern = re.compile(r"/pt/basketball/m-(\d{2}-\d{2}-\d{4})-(.+?)(?:-prediction)?(?:[?#]|$)")
        seen_slugs: set[str] = set()

        for a in soup.find_all("a", href=pattern):
            href = a.get("href", "").split("?")[0].split("#")[0]
            if not href:
                continue

            match = pattern.search(href)
            if not match:
                continue

            slug_date_raw = match.group(1)
            try:
                slug_dt = datetime.strptime(slug_date_raw, "%d-%m-%Y")
                slug_date = slug_dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

            if slug_date != target_date:
                continue

            teams_slug = match.group(2)
            node_text = a.get_text(separator=" ", strip=True).lower()
            time_match = re.search(r"(\d{2}:\d{2})", node_text)

            t_brt, date_brt = self.parse_time(time_match.group(1) if time_match else None, target_date)

            base_href = href.replace("-prediction", "")
            slug_clean = base_href.replace("/pt/basketball/", "")

            if slug_clean in seen_slugs:
                continue
            seen_slugs.add(slug_clean)

            imgs = a.find_all("img")
            alts = [img.get("alt", "").strip() for img in imgs if img.get("alt")]

            if len(alts) >= 2:
                if "@" in node_text or "vs" in node_text:
                    if "@" in node_text:
                        parts = node_text.split("@")
                        if len(parts) == 2:
                            away_text = parts[0].strip()
                            home_text = parts[1].strip()
                            away = self.clean_team(alts[0]) if alts[0].lower() in away_text else self.clean_team(alts[1])
                            home = self.clean_team(alts[1]) if alts[1].lower() in home_text else self.clean_team(alts[0])
                        else:
                            away, home = self.clean_team(alts[0]), self.clean_team(alts[1])
                    else:
                        away, home = self.clean_team(alts[0]), self.clean_team(alts[1])
                else:
                    away, home = self.clean_team(alts[0]), self.clean_team(alts[1])
            else:
                parts = teams_slug.split("-")
                mid = len(parts) // 2
                away = " ".join(parts[:mid]).title()
                home = " ".join(parts[mid:]).title()

            conf_match = re.search(r"(\d{1,3})%", node_text)
            source_url = (Config.BASE_URL + base_href if base_href.startswith("/") else base_href)

            games.append(GameData(
                slug=slug_clean,
                game_date=target_date,
                game_date_brt=date_brt,
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

        log.info(f"Jogos extraídos: {len(games)}")
        return games

    def extract_full_prediction(self, html: str, game: GameData) -> None:
        if not html:
            return

        soup = BeautifulSoup(html, "html.parser")

        game.odds = self._extract_odds(soup)
        game.h2h = self._extract_h2h(soup)
        game.home_form, game.away_form = self._extract_form(soup, game)
        game.home_news, game.away_news = self._extract_news(soup, game)
        game.home_stats, game.away_stats = self._extract_stats(soup, game)
        game.editorial_pick = self._extract_editorial(soup)

        game.tactical_prediction, game.extraction_method = self._extract_editorial_text(soup, game)
        game.text_quality_score = self._calculate_text_quality(game.tactical_prediction)

        log.info(f"  → Texto: {len(game.tactical_prediction) if game.tactical_prediction else 0} chars | "
                f"Método: {game.extraction_method} | "
                f"Qualidade: {game.text_quality_score:.2f} | "
                f"Odds: {bool(game.odds)} | H2H: {bool(game.h2h)}")

    def _extract_odds(self, soup: BeautifulSoup) -> Optional[OddsData]:
        try:
            odds = OddsData()
            all_text = soup.get_text()
            pattern = r'V1.*?(\d+\.\d+).*?X.*?(\d+\.\d+).*?V2.*?(\d+\.\d+)'
            match = re.search(pattern, all_text, re.DOTALL)
            if match:
                odds.v1, odds.x, odds.v2 = float(match.group(1)), float(match.group(2)), float(match.group(3))
            return odds if odds.v1 else None
        except:
            return None

    def _extract_h2h(self, soup: BeautifulSoup) -> Optional[H2HData]:
        try:
            h2h = H2HData()
            section = soup.find(string=re.compile(r"Confrontos diretos|Estatísticas H2H", re.I))
            if not section:
                return None
            container = section.find_parent(["div", "section"])
            if not container:
                return None

            text = container.get_text()
            pcts = re.findall(r'(\d+)%', text)
            if len(pcts) >= 2:
                h2h.home_win_pct = float(pcts[0])
            wins = re.findall(r'(\d+)\s*Vitórias?', text)
            if len(wins) >= 2:
                h2h.home_wins, h2h.away_wins = int(wins[0]), int(wins[1])
                h2h.total_matches = h2h.home_wins + h2h.away_wins

            seen_matches: set[str] = set()

            for row in container.find_all("tr")[:5]:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 4:
                    scores = re.findall(r'(\d+)', cells[3].get_text())
                    if len(scores) >= 2:
                        match_key = f"{cells[0].get_text(strip=True)}|{cells[1].get_text(strip=True)}|{cells[2].get_text(strip=True)}|{scores[0]}|{scores[1]}"
                        if match_key in seen_matches:
                            continue
                        seen_matches.add(match_key)

                        h2h.recent_matches.append(H2HMatch(
                            date=cells[0].get_text(strip=True),
                            home_team=cells[1].get_text(strip=True),
                            away_team=cells[2].get_text(strip=True),
                            home_score=int(scores[0]),
                            away_score=int(scores[1])
                        ))
            return h2h
        except:
            return None

    def _extract_form(self, soup: BeautifulSoup, game: GameData) -> tuple:
        home_form = TeamForm(team_name=game.home_team)
        away_form = TeamForm(team_name=game.away_team)

        try:
            for section in soup.find_all(string=re.compile(r"Resultados dos jogos|Últimos jogos|Forma", re.I)):
                container = section.find_parent(["div", "section"])
                if not container:
                    continue

                container_text = container.get_text()
                home_key = game.home_team.split()[-1]
                away_key = game.away_team.split()[-1]

                team_sections = container.find_all(["h3", "h4", "div", "p"], 
                    string=re.compile(f"{home_key}|{away_key}", re.I))

                if team_sections:
                    for ts in team_sections:
                        ts_text = ts.get_text()
                        target = home_form if home_key in ts_text and away_key not in ts_text else \
                                 away_form if away_key in ts_text and home_key not in ts_text else None

                        if target:
                            win_match = re.search(r'(\d+)\s*vitórias?\s*nos\s*últimos\s*dez', ts_text, re.I)
                            if win_match:
                                target.wins_last_10 = int(win_match.group(1))
                            pos_match = re.search(r'(\d+)[º°o].*?lugar.*?Conferência', ts_text, re.I)
                            if pos_match:
                                target.position_conference = pos_match.group(1) + "º"
                else:
                    text = container_text
                    win_match = re.search(r'(\d+)\s*vitórias?\s*nos\s*últimos\s*dez', text, re.I)
                    if win_match:
                        home_form.wins_last_10 = int(win_match.group(1))
                    pos_match = re.search(r'(\d+)[º°o].*?lugar.*?Conferência', text, re.I)
                    if pos_match:
                        home_form.position_conference = pos_match.group(1) + "º"

            return home_form, away_form
        except:
            return home_form, away_form

    def _extract_news(self, soup: BeautifulSoup, game: GameData) -> tuple:
        home_news = TeamNews()
        away_news = TeamNews()

        try:
            section = soup.find(string=re.compile(r"Últimas notícias|Lesões|Injuries", re.I))
            if not section:
                return home_news, away_news

            container = section.find_parent(["div", "section"])
            if not container:
                return home_news, away_news

            text = container.get_text()

            patterns = [
                r'([A-Z][a-zA-Z\s\.]+?)\s+está\s+(fora|duvida|dúvida|provável|questionável)',
                r'participação\s+de\s+([A-Z][a-zA-Z\s\.]+?)\s+([\w\s]+?)(?:\.|;|$)',
                r'([A-Z][a-zA-Z\s\.]{2,30}?)\s*[-–—]\s*(fora|duvida|dúvida|provável|questionável)',
            ]

            for pattern in patterns:
                for match in re.finditer(pattern, text):
                    player = match.group(1).strip()
                    status_raw = match.group(2).lower()

                    player = re.sub(r'^(o |a |os |as )\s*', '', player, flags=re.I).strip()
                    if len(player) < 3 or len(player) > 30:
                        continue

                    status = "fora" if "fora" in status_raw else \
                             "dúvida" if any(x in status_raw for x in ["duvida", "dúvida", "questionável"]) else \
                             "provável"

                    injury = Injury(player=player, status=status)

                    text_before = text[:text.find(player)]
                    home_mentions = text_before.lower().count(game.home_team.split()[-1].lower())
                    away_mentions = text_before.lower().count(game.away_team.split()[-1].lower())

                    if home_mentions > away_mentions:
                        home_news.injuries.append(injury)
                    elif away_mentions > home_mentions:
                        away_news.injuries.append(injury)
                    else:
                        if len(home_news.injuries) <= len(away_news.injuries):
                            home_news.injuries.append(injury)
                        else:
                            away_news.injuries.append(injury)

            return home_news, away_news
        except:
            return home_news, away_news

    def _extract_stats(self, soup: BeautifulSoup, game: GameData) -> tuple:
        home_stats = TeamStats()
        away_stats = TeamStats()

        try:
            section = soup.find(string=re.compile(r"Artilheiros|Top Scorers|Estatísticas", re.I))
            if section and section.find_parent(["div", "section"]):
                container = section.find_parent(["div", "section"])
                text = container.get_text()

                scorers = []
                for match in re.finditer(r'([A-Z][a-zA-Z\s\.]+?)[^,]*?(\d+\.\d+)\s*pontos?', text):
                    player = match.group(1).strip()
                    ppg = float(match.group(2))
                    pos = match.start()
                    scorers.append((pos, player, ppg))

                mid_point = len(text) // 2

                for pos, player, ppg in scorers:
                    scorer = TopScorer(player=player, ppg=ppg)
                    if pos < mid_point:
                        home_stats.top_scorer = scorer
                    else:
                        away_stats.top_scorer = scorer

            analysis = soup.find(attrs={"data-testid": "DisplayContent"})
            if analysis:
                pts = re.findall(r'(\d+\.\d+)\s*pontos', analysis.get_text())
                if len(pts) >= 4:
                    home_stats.points_scored_avg = float(pts[0])
                    home_stats.points_allowed_avg = float(pts[1])
                    away_stats.points_scored_avg = float(pts[2])
                    away_stats.points_allowed_avg = float(pts[3])

            return home_stats, away_stats
        except:
            return home_stats, away_stats

    def _extract_editorial(self, soup: BeautifulSoup) -> Optional[EditorialPick]:
        try:
            section = soup.find(string=re.compile(r"Previsão da Redação|NOSSA ESCOLHA|Escolha do Editor", re.I))
            if not section:
                return None

            container = section.find_parent(["div", "section", "article"])
            if not container:
                return None

            text = container.get_text()
            text_lower = text.lower()
            pick = EditorialPick(recommendation="")

            if "vitória dos visitantes" in text_lower or "vitória do visitante" in text_lower:
                pick.recommendation = "vitoria_visitante"
                pick.market_type = "moneyline"
            elif "vitória" in text_lower and ("casa" in text_lower or "mandante" in text_lower):
                pick.recommendation = "vitoria_casa"
                pick.market_type = "moneyline"
            elif "handicap" in text_lower or "spread" in text_lower or "linha" in text_lower:
                pick.recommendation = "handicap"
                pick.market_type = "spread"
            elif "over" in text_lower or "acima" in text_lower or "mais de" in text_lower:
                pick.recommendation = "over"
                pick.market_type = "total"
            elif "under" in text_lower or "abaixo" in text_lower or "menos de" in text_lower:
                pick.recommendation = "under"
                pick.market_type = "total"
            elif "pass" in text_lower or "ficar de fora" in text_lower or "não apostar" in text_lower:
                pick.recommendation = "pass"
                pick.market_type = "none"
            else:
                if "favorito" in text_lower and "visitante" in text_lower:
                    pick.recommendation = "vitoria_visitante"
                    pick.market_type = "moneyline"
                elif "favorito" in text_lower:
                    pick.recommendation = "vitoria_casa"
                    pick.market_type = "moneyline"
                else:
                    pick.recommendation = "analise_neutra"
                    pick.market_type = "analysis"

            handicap = re.search(r'([\+-]?\d+\.?\d*)\s*(?:handicap|spread|linha)', text_lower)
            if not handicap:
                handicap = re.search(r'handicap\s*([\+-]?\d+\.?\d*)', text_lower)
            if not handicap:
                handicap = re.search(r'([\+-]\d+\.\d+)', text)
            if handicap:
                pick.handicap_line = handicap.group(1)

            odds = re.search(r'(\d+\.\d+)\*?', text)
            if odds:
                pick.odds = float(odds.group(1))

            high_signals = ["muito confiante", "forte", "ótima", "excelente", "convicção", "certeza"]
            medium_signals = ["moderado", "razoável", "interessante", "pode valer", "oportunidade"]
            low_signals = ["arriscado", "cuidado", "incerto", "evitar", "foge", "perigoso", "instável"]
            avoid_signals = ["não apostar", "ficar de fora", "pass", "evitar", "sem valor"]

            if any(w in text_lower for w in avoid_signals):
                pick.confidence_level = "evitar"
            elif any(w in text_lower for w in high_signals):
                pick.confidence_level = "alta"
            elif any(w in text_lower for w in low_signals):
                pick.confidence_level = "baixa"
            elif any(w in text_lower for w in medium_signals):
                pick.confidence_level = "média"
            else:
                pick.confidence_level = "média"

            paragraphs = container.find_all("p")
            for p in paragraphs:
                p_text = p.get_text(strip=True)
                if len(p_text) > 50 and not self._is_junk_text(p_text):
                    pick.explanation = p_text
                    break

            if not pick.explanation:
                clean_text = self._clean_text(text)
                if len(clean_text) > 50:
                    pick.explanation = clean_text[:500]

            return pick if pick.recommendation else None
        except Exception as e:
            log.warning(f"Erro extraindo editorial: {e}")
            return None

    # ═══════════════════════════════════════════════════════════════════════════
    # [FIX-CRITICAL] Extrai APENAS o texto da "Previsão da Redação"
    # ═══════════════════════════════════════════════════════════════════════════
    def _extract_editorial_text(self, soup: BeautifulSoup, game: GameData) -> tuple[Optional[str], Optional[str]]:
        """
        Extrai APENAS o texto da "Previsão da Redação" — o conteúdo entre o header
        e o próximo elemento significativo (NOSSA ESCOLHA, votação, etc).
        Retorna (texto, método_usado).
        """

        # ESTRATÉGIA 1: Header "Previsão da Redação" → extrai APENAS o conteúdo 
        # entre ele e o próximo header/separador significativo
        header = soup.find(string=re.compile(r"Previsão da Redação", re.I))

        if header:
            header_elem = header.find_parent(["h2", "h3", "h4", "div", "section"])
            if not header_elem:
                header_elem = header.parent

            if header_elem:
                editorial_content = []
                current = header_elem.find_next_sibling()

                stop_selectors = [
                    re.compile(r"NOSSA ESCOLHA", re.I),
                    re.compile(r"Esta previsão vai ser correta", re.I),
                    re.compile(r"Bônus de jogos", re.I),
                    re.compile(r"Previsões estatísticas", re.I),
                    re.compile(r"^\s*APOSTAR\s*$", re.I),
                ]

                while current:
                    current_text = current.get_text(strip=True)
                    is_stop = False

                    if current.name in ["div", "section"]:
                        if any(pattern.search(current_text) for pattern in stop_selectors):
                            is_stop = True
                        classes = " ".join(current.get("class", [])).lower()
                        if any(x in classes for x in ["bet", "odds", "bonus", "vote", "poll"]):
                            is_stop = True

                    if current.name in ["h2", "h3", "h4"]:
                        header_text = current_text.lower()
                        if any(pattern.search(header_text) for pattern in stop_selectors):
                            is_stop = True
                        if len(current_text) > 0 and current_text != header.get_text(strip=True):
                            is_stop = True

                    if is_stop:
                        break

                    if current.name in ["p", "div", "section", "article", "span"]:
                        text = self._process_single_element(current, game)
                        if text:
                            editorial_content.append(text)

                    current = current.find_next_sibling()

                if editorial_content:
                    result = "\n\n".join(editorial_content).strip()
                    if len(result) >= 100:
                        log.info(f"  → [ESTRATÉGIA 1] Previsão da Redação isolada: {len(result)} chars")
                        return result, "editorial_isolated"

        # ESTRATÉGIA 2: Containers por classe
        for cls in ["prediction-content", "match-preview", "analysis-content", 
                    "previsao", "editorial-pick", "redacao", "preview-content",
                    "match-analysis", "game-preview", "expert-pick"]:
            container = soup.find(["div", "section", "article"], 
                                  class_=re.compile(cls, re.I))
            if container:
                text = self._process_editorial_container(container, game)
                if text and len(text) >= 400:
                    log.info(f"  → [ESTRATÉGIA 2] Container .{cls}")
                    return text, f"class_{cls}"

        # ESTRATÉGIA 3: data-testid
        container = soup.find(attrs={"data-testid": "DisplayContent"})
        if container:
            text = self._process_editorial_container(container, game)
            if text and len(text) >= 400:
                log.info(f"  → [ESTRATÉGIA 3] data-testid=DisplayContent")
                return text, "display_content"

        # ESTRATÉGIA 4: JSON-LD
        json_text = self._extract_json_ld(soup)
        if json_text and len(json_text) >= 500:
            if game.home_team.split()[-1].lower() in json_text.lower() or \
               game.away_team.split()[-1].lower() in json_text.lower():
                log.info(f"  → [ESTRATÉGIA 4] JSON-LD validado com nomes dos times")
                return json_text, "json_ld"

        # ESTRATÉGIA 5: article/main validado
        for tag in ["article", "main"]:
            container = soup.find(tag)
            if container:
                text = self._process_editorial_container(container, game)
                if text and len(text) >= 600:
                    if (game.home_team.split()[-1].lower() in text.lower() or 
                        game.away_team.split()[-1].lower() in text.lower()):
                        log.info(f"  → [ESTRATÉGIA 5] {tag} com validação de times")
                        return text, f"{tag}_validated"

        log.warning(f"  → [FALHA] Nenhuma estratégia encontrou texto de qualidade")
        return None, None

    def _process_single_element(self, elem, game: GameData) -> Optional[str]:
        """Processa um único elemento HTML extraindo texto limpo."""
        if not elem:
            return None

        elem_copy = BeautifulSoup(str(elem), "html.parser").find()
        for junk in elem_copy.find_all(["button", "script", "style", "nav", "svg"]):
            junk.decompose()

        text = elem_copy.get_text(strip=True)

        if not text or len(text) < 20:
            return None
        if self._is_junk_text(text):
            return None

        basketball_terms = ["pontos", "cesta", "rebote", "assistência", "defesa", 
                           "ataque", "jogador", "quarto", "tempo", "vitória", "derrota",
                           "arremesso", "cesta", "jogo", "partida", "confronto"]
        has_basketball = any(term in text.lower() for term in basketball_terms)
        has_team = (game.home_team.split()[-1].lower() in text.lower() or 
                   game.away_team.split()[-1].lower() in text.lower())

        if not has_basketball and not has_team and len(text) < 100:
            return None

        return text

    def _process_editorial_container(self, container, game: GameData) -> Optional[str]:
        if not container:
            return None

        container_copy = BeautifulSoup(str(container), "html.parser").find()

        for elem in container_copy.find_all([
            "button", "script", "style", "nav", "footer", "aside", 
            "header", "form", "iframe", "noscript", "svg", "canvas"
        ]):
            elem.decompose()

        for div in container_copy.find_all("div"):
            div_text = div.get_text(strip=True).lower()
            if any(kw in div_text for kw in ["apostar agora", "bet", "bônus", "cadastre", "odds"]):
                if len(div_text) < 150:
                    div.decompose()

        sections = []
        last_text = ""

        for elem in container_copy.find_all([
            "h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "blockquote"
        ]):
            text = elem.get_text(strip=True)

            if not text or len(text) < 20:
                continue
            if text == last_text:
                continue
            if self._is_junk_text(text):
                continue

            last_text = text

            if elem.name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                sections.append(f"\n{text}\n{"," * min(len(text), 40)}")
            elif elem.name == "li":
                sections.append(f"  • {text}")
            elif elem.name == "blockquote":
                sections.append(f"  > {text}")
            else:
                sections.append(text)

        for div in container_copy.find_all("div"):
            if not div.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "blockquote"]):
                text = div.get_text(strip=True)
                if text and len(text) >= 40 and text != last_text and not self._is_junk_text(text):
                    sections.append(text)
                    last_text = text

        result = "\n\n".join(sections).strip()
        result = re.sub(r'\n{3,}', \'\n\n', result)

        if len(result) < 200:
            return None

        basketball_terms = ["pontos", "cesta", "rebote", "assistência", "defesa", 
                           "ataque", "jogador", "quarto", "tempo", "vitória", "derrota"]
        has_basketball = any(term in result.lower() for term in basketball_terms)
        has_team = (game.home_team.split()[-1].lower() in result.lower() or 
                   game.away_team.split()[-1].lower() in result.lower())

        if not has_basketball and not has_team:
            log.warning(f"    → Texto rejeitado: sem termos de basquete ou times")
            return None

        return result

    def _is_junk_text(self, text: str) -> bool:
        text_lower = text.lower()
        text_len = len(text)

        if any(kw in text_lower for kw in self.JUNK_KEYWORDS_HARD):
            hard_count = sum(1 for kw in self.JUNK_KEYWORDS_HARD if kw in text_lower)
            if text_len > 200 and hard_count <= 1:
                pass
            else:
                return True

        soft_count = sum(1 for kw in self.JUNK_KEYWORDS_SOFT if kw in text_lower)
        if soft_count > 0:
            junk_chars = sum(len(kw) for kw in self.JUNK_KEYWORDS_SOFT if kw in text_lower)
            if junk_chars / text_len > 0.3:
                return True

        if re.match(r'^[\d\.\s\-\+\*]+$', text.strip()):
            return True

        if text_len < 50 and re.search(r'\d+\.\d+', text):
            return True

        return False

    def _clean_text(self, text: str) -> str:
        lines = text.split("\n")
        clean_lines = []
        for line in lines:
            line = line.strip()
            if not line or len(line) < 15:
                continue
            if self._is_junk_text(line):
                continue
            clean_lines.append(line)
        return "\n".join(clean_lines)

    def _calculate_text_quality(self, text: Optional[str]) -> float:
        if not text:
            return 0.0

        score = 0.0
        length = len(text)

        if length >= 2000:
            score += 0.3
        elif length >= 1000:
            score += 0.25
        elif length >= 500:
            score += 0.2
        elif length >= 200:
            score += 0.1
        else:
            return 0.0

        if "===" in text or "---" in text:
            score += 0.15
        if "•" in text:
            score += 0.1
        if ">" in text:
            score += 0.05

        analytical_terms = ["porque", "devido", "estatística", "média", "percentual",
                         "tendência", "fator", "vantagem", "desvantagem", "probabilidade",
                         "análise", "dado", "comparativo", "histórico"]
        analytical_count = sum(1 for term in analytical_terms if term in text.lower())
        score += min(analytical_count * 0.03, 0.15)

        if re.search(r'\d+\.\d+', text):
            score += 0.05

        junk_count = sum(1 for kw in self.JUNK_KEYWORDS_HARD if kw in text.lower())
        junk_count += sum(1 for kw in self.JUNK_KEYWORDS_SOFT if kw in text.lower())
        penalty = min(junk_count * 0.03, 0.2)
        score -= penalty

        return max(0.0, min(1.0, score))

    def _extract_json_ld(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    if isinstance(data, list):
                        data = data[0]

                    if data.get("@type") in ["NewsArticle", "Article", "SportsEvent"]:
                        article_body = data.get("articleBody", "")
                        description = data.get("description", "")

                        text = article_body if len(article_body) > len(description) else description

                        if len(text) > 200:
                            return text

                except (json.JSONDecodeError, AttributeError):
                    continue

            return None
        except Exception as e:
            log.error(f"Erro JSON-LD: {e}")
            return None


# ─── Persistência ─────────────────────────────────────────────────────────────
class DatabaseManager:
    def __init__(self):
        self.sb: Client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)
        self._known_columns: Optional[Set[str]] = None

    def _get_table_columns(self) -> Set[str]:
        if self._known_columns is not None:
            return self._known_columns

        try:
            result = self.sb.table("nba_games_schedule").select("*").limit(1).execute()

            if result.data:
                columns = set(result.data[0].keys())
            else:
                columns = {
                    "id", "slug", "game_date", "game_date_brt", "game_time_et", "game_time_brt",
                    "home_team", "away_team", "home_team_pt", "away_team_pt",
                    "home_tri", "away_tri", "source_url", "confidence_pct",
                    "game_status", "scraped_at", "tactical_prediction", "groq_insight",
                    "extraction_method", "text_quality_score", "editorial_pick"
                }

            self._known_columns = columns
            log.info(f"Colunas detectadas: {len(columns)}")
            return columns

        except Exception as e:
            log.warning(f"Não foi possível detectar colunas: {e}")
            return {
                "slug", "game_date", "game_date_brt", "game_time_et", "game_time_brt",
                "home_team", "away_team", "home_team_pt", "away_team_pt",
                "home_tri", "away_tri", "source_url", "tactical_prediction"
            }

    def get_cached(self) -> Dict[str, dict]:
        columns = self._get_table_columns()
        select_cols = ["slug", "game_date"]

        if "tactical_prediction" in columns:
            select_cols.append("tactical_prediction")
        if "groq_insight" in columns:
            select_cols.append("groq_insight")
        if "text_quality_score" in columns:
            select_cols.append("text_quality_score")

        res = self.sb.table("nba_games_schedule").select(",".join(select_cols)).execute()

        return {
            row["slug"]: {
                "game_date": row.get("game_date"),
                "has_text": bool(row.get("tactical_prediction")),
                "has_groq": bool(row.get("groq_insight")),
                "quality": row.get("text_quality_score", 0),
            }
            for row in res.data
        }

    def upsert_games(self, games: List[GameData]):
        columns = self._get_table_columns()

        seen = set()
        unique = []
        for g in games:
            if g.slug not in seen:
                seen.add(g.slug)
                unique.append(g)

        if not unique:
            log.info("Nenhum jogo para persistir")
            return

        rows = []
        skipped = 0
        rejected_slugs: list[str] = []

        for g in unique:
            if not g.tactical_prediction or g.text_quality_score < 0.15:
                log.warning(f"  → {g.slug}: REJEITADO (texto={bool(g.tactical_prediction)}, qualidade={g.text_quality_score:.2f})")
                skipped += 1
                rejected_slugs.append(g.slug)
                continue

            row: Dict[str, Any] = {}

            basic_fields = {
                "slug", "game_date", "game_date_brt", "game_time_et", "game_time_brt",
                "home_team", "away_team", "home_team_pt", "away_team_pt",
                "home_tri", "away_tri", "source_url", "confidence_pct",
                "game_status", "scraped_at", "tactical_prediction",
                "extraction_method", "text_quality_score"
            }

            for field in basic_fields:
                if field in columns:
                    value = getattr(g, field)
                    row[field] = value

            json_fields = {
                "odds": g.odds,
                "h2h": g.h2h,
                "home_form": g.home_form,
                "away_form": g.away_form,
                "home_news": g.home_news,
                "away_news": g.away_news,
                "home_stats": g.home_stats,
                "away_stats": g.away_stats,
                "editorial_pick": g.editorial_pick,
                "groq_insight": g.groq_insight,
            }

            for field, value in json_fields.items():
                if field in columns and value is not None:
                    row[field] = json.dumps(value, ensure_ascii=False, default=str)

            rows.append(row)

        if not rows:
            log.warning(f"Nenhum jogo passou no quality gate. {skipped} rejeitados: {rejected_slugs[:5]}")
            return

        try:
            self.sb.table("nba_games_schedule").upsert(rows, on_conflict="slug").execute()

            for r in rows:
                text_ok = "✓" if r.get("tactical_prediction") else "✗"
                groq_ok = "✓" if r.get("groq_insight") else "✗"
                quality = r.get("text_quality_score", 0)
                log.info(f"  → Persistido: {r['slug'][:40]} | Texto:{text_ok} | Groq:{groq_ok} | Q:{quality:.2f}")

            log.info(f"Total: {len(rows)} persistidos, {skipped} rejeitados")

        except Exception as e:
            log.error(f"Erro ao persistir: {e}")
            raise


# ─── Orquestrador ─────────────────────────────────────────────────────────────
async def main():
    log.info("═══ Replicante V6.6.2 (FIX: Isolamento Previsão da Redação) ═══")

    if not Config.SCRAPINGANT_KEY:
        log.error("SCRAPINGANT_API_KEY ausente")
        return
    if not Config.GROQ_API_KEY:
        log.warning("GROQ_API_KEY ausente - insights desativados")

    net = NetworkClient()
    ext = NBAExtractor()
    db = DatabaseManager()

    try:
        html_list = await net.fetch(Config.PREDICTIONS_URL, use_browser=False)
        if not html_list:
            log.error("Falha ao carregar lista")
            return

        today = datetime.now(BRT).strftime("%Y-%m-%d")
        games = ext.extract_games_list(html_list, today)

        if not games:
            log.info(f"Sem jogos para {today}")
            return

        cache = db.get_cached()

        async def process(game: GameData) -> Optional[GameData]:
            cached = cache.get(game.slug, {})

            low_quality = cached.get("quality", 0) < 0.2
            needs_update = not cached.get("has_text") or not cached.get("has_groq") or low_quality

            if not needs_update and cached.get("game_date") == game.game_date:
                log.info(f"[{game.away_tri} @ {game.home_tri}] → Cache OK (Q:{cached.get('quality', 0):.2f})")
                return None

            pred_url = f"{game.source_url}-prediction"

            log.info(f"[{game.away_tri} @ {game.home_tri}] → Fetching...")

            html = await fetch_with_retry(net, pred_url)

            if html:
                ext.extract_full_prediction(html, game)

                has_text = bool(game.tactical_prediction)
                quality = game.text_quality_score or 0

                log.info(f"[{game.away_tri} @ {game.home_tri}] → "
                        f"Texto:{has_text} ({len(game.tactical_prediction) if game.tactical_prediction else 0} chars) | "
                        f"Método:{game.extraction_method} | "
                        f"Qualidade:{quality:.2f}")

                if Config.GROQ_API_KEY and game.tactical_prediction and quality >= 0.15:
                    prompt = game.to_groq_prompt()
                    insight = await net.post_groq(prompt)
                    if insight:
                        game.groq_insight = insight
                elif not game.tactical_prediction:
                    log.warning(f"[{game.away_tri} @ {game.home_tri}] → Sem texto para Groq!")
                elif quality < 0.15:
                    log.warning(f"[{game.away_tri} @ {game.home_tri}] → Qualidade baixa, pulando Groq")
            else:
                log.warning(f"[{game.away_tri} @ {game.home_tri}] → Sem HTML")

            return game

        semaphore = asyncio.Semaphore(Config.CONCURRENCY_LIMIT)

        async def process_with_limit(game: GameData) -> Optional[GameData]:
            async with semaphore:
                result = await process(game)
                await asyncio.sleep(1.0 + random.uniform(0, 0.5))
                return result

        tasks = [process_with_limit(g) for g in games]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid = []
        for r in results:
            if isinstance(r, Exception):
                log.error(f"Erro no processamento: {r}")
                continue
            if r is not None:
                valid.append(r)

        if valid:
            db.upsert_games(valid)

        with_text = sum(1 for g in valid if g.tactical_prediction)
        with_groq = sum(1 for g in valid if g.groq_insight)
        high_quality = sum(1 for g in valid if (g.text_quality_score or 0) >= 0.3)

        log.info(f"═══ Resumo: {len(valid)} processados | Texto:{with_text} | Groq:{with_groq} | Alta Q:{high_quality} ═══")

    finally:
        await net.close()


if __name__ == "__main__":
    asyncio.run(main())
