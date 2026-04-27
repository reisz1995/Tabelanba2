"""
NBA Scraper - Replicante V6.6.1 (Purificação Estrutural e DOM Profundo)
Correcções:
  - [UPDATE] Inversão de hierarquia: DOM profundo primário, JSON-LD como fallback.
  - [FIX] Motor heurístico focado exclusivamente em tags semânticas (p, h2, h3).
  - [FIX] Expurgo agressivo de nós parasitas e controlo rigoroso de densidade numérica.
"""

import os
import re
import json
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
    format="%(asctime)s [%(levelname)s] [NBA-V6.6.1] %(message)s",
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

    def to_groq_prompt(self) -> str:
        """Gera prompt de sistema estruturado para inferência Groq."""
        def implied_prob(odds: float) -> float:
            return 100 / odds if odds > 0 else 0
        
        home_implied = implied_prob(self.odds.v1) if self.odds and self.odds.v1 else 0
        away_implied = implied_prob(self.odds.v2) if self.odds and self.odds.v2 else 0
        
        home_injuries = ", ".join([f"{i.player} ({i.status})" for i in self.home_news.injuries]) if self.home_news else "Nenhuma"
        away_injuries = ", ".join([f"{i.player} ({i.status})" for i in self.away_news.injuries]) if self.away_news else "Nenhuma"
        
        home_form_str = f"{self.home_form.wins_last_10}/10" if self.home_form and self.home_form.wins_last_10 else "N/A"
        away_form_str = f"{self.away_form.wins_last_10}/10" if self.away_form and self.away_form.wins_last_10 else "N/A"
        
        return f"""Você é o Estatístico Chefe de NBA. Analise este jogo.

## DADOS
{self.away_team} @ {self.home_team} | {self.game_date}

## MERCADO
- V1: {self.odds.v1 if self.odds else 'N/A'} ({home_implied:.1f}%)
- V2: {self.odds.v2 if self.odds else 'N/A'} ({away_implied:.1f}%)

## CONTEXTO
H2H: {self.h2h.total_matches if self.h2h else 'N/A'} jogos
Forma: Casa {home_form_str} vs Visitante {away_form_str}
Lesões Casa: {home_injuries}
Lesões Visitante: {away_injuries}

## ANÁLISE COMPLETA
{self.tactical_prediction[:2500] if self.tactical_prediction else 'N/A'}

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
                            {"role": "system", "content": "Você é um analista de NBA especializado em modelos estocásticos. Responda APENAS em JSON válido."},
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


# ─── Retry Helper ─────────────────────────────────────────────────────────────
async def fetch_with_retry(net, url: str) -> Optional[str]:
    """Fluxo robusto de injecção e alternância de browser."""
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

# ─── Extração ─────────────────────────────────────────────────────────────────
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

        dt_target = datetime.strptime(target_date, "%Y-%m-%d")
        hoje_visual = dt_target.strftime("%d.%m.%y")
        meses_pt = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]
        hoje_extenso = f"{dt_target.day:02d} {meses_pt[dt_target.month - 1]}"
        
        for a in soup.find_all("a", href=pattern):
            href = a.get("href", "")
            if "#" in href:
                continue

            match = pattern.search(href)
            if not match:
                continue

            node_text = a.get_text(separator=" ", strip=True).lower()
            is_valid_date = False
            
            if "hoje" in node_text or hoje_visual in node_text or hoje_extenso in node_text:
                is_valid_date = True
            else:
                regex_data = re.compile(r'\d{2}\.\d{2}\.\d{2}|\d{2} (jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)|hoje|amanhã', re.I)
                prev_date = a.find_previous(string=regex_data)
                if prev_date:
                    prev_text = prev_date.strip().lower()
                    if "hoje" in prev_text or hoje_visual in prev_text or hoje_extenso in prev_text:
                        is_valid_date = True

            if not is_valid_date:
                continue

            teams_slug = match.group(2)
            time_match = re.search(r"(\d{2}:\d{2})", node_text)
            t_brt, _ = self.parse_time(time_match.group(1) if time_match else None, target_date)

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

            conf_match = re.search(r"(\d{1,3})%", node_text)
            source_url = (Config.BASE_URL + base_href if base_href.startswith("/") else base_href)

            games.append(GameData(
                slug=slug_clean,
                game_date=target_date,
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

        log.info(f"Jogos mapeados: {len(games)}")
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
        
        # O novo fluxo de extracção narrativa V6.6.1
        game.tactical_prediction = self._extract_text_v3(soup)
        
        log.info(f"  → Texto: {len(game.tactical_prediction) if game.tactical_prediction else 0} chars | "
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
            
            for row in container.find_all("tr")[:5]:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 4:
                    scores = re.findall(r'(\d+)', cells[3].get_text())
                    if len(scores) >= 2:
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
            for section in soup.find_all(string=re.compile(r"Resultados dos jogos", re.I)):
                container = section.find_parent(["div", "section"])
                if not container:
                    continue
                text = container.get_text()
                target = home_form if game.home_team.split()[-1] in text else away_form
                win_match = re.search(r'(\d+)\s*vitórias?\s*nos\s*últimos\s*dez', text, re.I)
                if win_match:
                    target.wins_last_10 = int(win_match.group(1))
                pos_match = re.search(r'(\d+)[º°o].*?lugar.*?Conferência', text, re.I)
                if pos_match:
                    target.position_conference = pos_match.group(1) + "º"
            return home_form, away_form
        except:
            return home_form, away_form

    def _extract_news(self, soup: BeautifulSoup, game: GameData) -> tuple:
        home_news = TeamNews()
        away_news = TeamNews()
        try:
            section = soup.find(string=re.compile(r"Últimas notícias", re.I))
            if not section:
                return home_news, away_news
            container = section.find_parent(["div", "section"])
            if not container:
                return home_news, away_news
            text = container.get_text()
            
            for pattern in [
                r'([A-Z][a-z]+)\s+está\s+(fora|duvida|dúvida|provável)',
                r'participação\s+de\s+([A-Z][a-z]+)\s+([\w\s]+)'
            ]:
                for match in re.finditer(pattern, text, re.I):
                    player, status = match.group(1), match.group(2).lower()
                    injury = Injury(
                        player=player,
                        status="fora" if "fora" in status else "dúvida" if "duvida" in status or "dúvida" in status else "provável"
                    )
                    if game.home_team.split()[-1] in text[:text.find(player)]:
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
            section = soup.find(string=re.compile(r"Artilheiros", re.I))
            if section and section.find_parent(["div", "section"]):
                text = section.find_parent(["div", "section"]).get_text()
                for match in re.finditer(r'([A-Z][a-z]+)[^,]+?(\d+\.\d+)\s*pontos?', text):
                    player, ppg = match.group(1), float(match.group(2))
                    scorer = TopScorer(player=player, ppg=ppg)
                    if game.home_team.split()[-1] in text[:text.find(player)]:
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
            section = soup.find(string=re.compile(r"Previsão da Redação|NOSSA ESCOLHA", re.I))
            if not section:
                return None
            container = section.find_parent(["div", "section"])
            if not container:
                return None
            text = container.get_text()
            pick = EditorialPick(recommendation="")
            
            if "vitória dos visitantes" in text.lower():
                pick.recommendation = "vitoria_visitante"
            elif "vitória" in text.lower() and "casa" in text.lower():
                pick.recommendation = "vitoria_casa"
            elif "handicap" in text.lower():
                pick.recommendation = "handicap"
                
            handicap = re.search(r'([\+-]\d+\.?\d*)', text)
            if handicap:
                pick.handicap_line = handicap.group(1)
            odds = re.search(r'(\d+\.\d+)\*?', text)
            if odds:
                pick.odds = float(odds.group(1))
            p = container.find("p")
            if p:
                pick.explanation = p.get_text(strip=True)
            return pick if pick.recommendation else None
        except:
            return None

    def _extract_json_ld(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrai texto de JSON-LD (structured data)."""
        try:
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    if isinstance(data, list):
                        data = data[0]
                    
                    if data.get("@type") == "NewsArticle" or data.get("@type") == "Article":
                        article_body = data.get("articleBody", "")
                        description = data.get("description", "")
                        
                        text = article_body if len(article_body) > len(description) else description
                        
                        if text and len(text) > 150:
                            clean_text = BeautifulSoup(text, "html.parser").get_text(separator=" ", strip=True)
                            return clean_text
                            
                except (json.JSONDecodeError, AttributeError):
                    continue
            return None
        except Exception as e:
            log.error(f"Erro na extracção de JSON-LD: {e}")
            return None

    def _extract_text_v3(self, soup: BeautifulSoup) -> Optional[str]:
        """
        EXTRACÇÃO REVISADA (V6.6.1): Varredura total do DOM.
        JSON-LD rebaixado para fallback devido à limitação de payload estrutural.
        """
        
        main_content = soup.find("main") or soup.find("body")
        if main_content:
            text = self._process_text_container(main_content, min_length=300)
            if text:
                log.info(f"  → Extracção DOM profunda bem-sucedida ({len(text)} caracteres).")
                return text

        text_from_json = self._extract_json_ld(soup)
        if text_from_json and len(text_from_json) > 100:
            log.warning("  → Alerta: Retornando apenas fragmento introdutório (Fallback JSON-LD).")
            return text_from_json

        return None

    def _process_text_container(self, container, min_length=200) -> Optional[str]:
        """
        Processador vectorial rigoroso anti-ruído.
        Isola blocos narrativos puros e aniquila H2H, propagandas e linhas de odds.
        """
        if not container:
            return None
            
        for elem in container.find_all(["button", "script", "style", "nav", "footer", "aside", "table", "form", "iframe", "a", "img", "ul", "ol"]):
            try:
                elem.decompose()
            except:
                pass
        
        sections = []
        last_text = ""
        
        blacklist = {
            "apostar", "registre", "bônus", "bet", "clique aqui", 
            "cadastre-se", "promoção", "termos e condições", 
            "handicap", "lucro", "cashout", "telegram", "whatsapp",
            "1xbit", "bet365", "betano", "1xbet", "pin-up",
            "últimos jogos", "confrontos diretos", "vitórias", "derrotas",
            "escala de força", "palpite pago", "vip", "cookie"
        }
        
        for elem in container.find_all(["p", "h2", "h3"]):
            text = elem.get_text(separator=" ", strip=True)
            
            if not text or len(text) < 45: 
                continue
            
            if text == last_text:
                continue
                
            text_lower = text.lower()
            
            if any(b in text_lower for b in blacklist):
                continue
                
            num_count = sum(c.isdigit() for c in text)
            density = num_count / len(text)
            if density > 0.08: 
                continue
                
            if re.search(r'\b[1-9]\.\d{2}\b', text) and len(text) < 150:
                continue
            
            last_text = text
            sections.append(text)
        
        result = "\n\n".join(sections).strip()
        result = re.sub(r'\n{3,}', '\n\n', result)
        
        log.info(f"  → Heurística de DOM: {len(sections)} blocos retidos | {len(result)} caracteres totais.")
        
        return result if len(result) >= min_length else None


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
                    "id", "slug", "game_date", "game_time_et", "game_time_brt",
                    "home_team", "away_team", "home_team_pt", "away_team_pt",
                    "home_tri", "away_tri", "source_url", "confidence_pct",
                    "game_status", "scraped_at", "tactical_prediction", "groq_insight"
                }
            self._known_columns = columns
            return columns
        except Exception as e:
            log.warning(f"Não foi possível detectar colunas: {e}")
            return {
                "slug", "game_date", "game_time_et", "game_time_brt",
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
        
        res = self.sb.table("nba_games_schedule").select(",".join(select_cols)).execute()
        
        return {
            row["slug"]: {
                "game_date": row.get("game_date"),
                "has_text": bool(row.get("tactical_prediction")),
                "has_groq": bool(row.get("groq_insight")),
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
        for g in unique:
            row: Dict[str, Any] = {}
            
            basic_fields = {
                "slug", "game_date", "game_time_et", "game_time_brt",
                "home_team", "away_team", "home_team_pt", "away_team_pt",
                "home_tri", "away_tri", "source_url", "confidence_pct",
                "game_status", "scraped_at", "tactical_prediction"
            }
            
            for field in basic_fields:
                if field in columns:
                    row[field] = getattr(g, field)
            
            json_fields = {
                "odds": g.odds, "h2h": g.h2h, "home_form": g.home_form,
                "away_form": g.away_form, "home_news": g.home_news,
                "away_news": g.away_news, "home_stats": g.home_stats,
                "away_stats": g.away_stats, "editorial_pick": g.editorial_pick,
                "groq_insight": g.groq_insight,
            }
            
            for field, value in json_fields.items():
                if field in columns and value is not None:
                    row[field] = json.dumps(value, ensure_ascii=False, default=str)
            
            if not row.get("tactical_prediction"):
                log.warning(f"  → {g.slug}: SEM tactical_prediction")
            
            rows.append(row)
        
        try:
            self.sb.table("nba_games_schedule").upsert(rows, on_conflict="slug").execute()
            for r in rows:
                text_ok = "✓" if r.get("tactical_prediction") else "✗"
                groq_ok = "✓" if r.get("groq_insight") else "✗"
                log.info(f"  → Persistido: {r['slug'][:40]} | Texto:{text_ok} | Groq:{groq_ok}")
        except Exception as e:
            log.error(f"Erro ao persistir: {e}")
            raise


# ─── Orquestrador ─────────────────────────────────────────────────────────────
async def main():
    log.info("═══ Replicante V6.6.1 (Arquitectura Heurística e DOM Profundo) ═══")

    if not Config.SCRAPINGANT_KEY:
        log.error("SCRAPINGANT_API_KEY ausente")
        return
    if not Config.GROQ_API_KEY:
        log.warning("GROQ_API_KEY ausente - injecção estocástica desactivada")

    net = NetworkClient()
    ext = NBAExtractor()
    db = DatabaseManager()

    try:
        html_list = await net.fetch(Config.PREDICTIONS_URL, use_browser=False)
        if not html_list:
            log.error("Falha ao carregar lista matriz")
            return

        today = datetime.now(BRT).strftime("%Y-%m-%d")
        games = ext.extract_games_list(html_list, today)

        if not games:
            log.info(f"Sem jogos configurados para {today}")
            return

        cache = db.get_cached()

        async def process(game: GameData) -> GameData:
            cached = cache.get(game.slug, {})
            has_empty_text = cached.get("has_text") and not cached.get("text_length", 0)
            needs_update = not cached.get("has_text") or not cached.get("has_groq") or has_empty_text
            
            if not needs_update and cached.get("game_date") == game.game_date:
                log.info(f"[{game.away_tri} @ {game.home_tri}] → Leitura em Cache Concluída")
                return game

            pred_url = f"{game.source_url}-prediction"
            log.info(f"[{game.away_tri} @ {game.home_tri}] → Iniciando extracção")
            
            html = await fetch_with_retry(net, pred_url)
            
            if html:
                ext.extract_full_prediction(html, game)
                
                # Execução obrigatória do fallback estrutural de segurança
                if not game.tactical_prediction:
                    log.warning(f"[{game.away_tri} @ {game.home_tri}] → Acionando fallback estrutural seguro")
                    soup = BeautifulSoup(html, "html.parser")
                    fallback_text = ext._extract_text_v3(soup)
                    if fallback_text:
                        game.tactical_prediction = fallback_text
                    else:
                        log.warning(f"[{game.away_tri} @ {game.home_tri}] → Ausência de vector narrativo. Bloqueio para IA.")
                
                has_text = bool(game.tactical_prediction)
                log.info(f"[{game.away_tri} @ {game.home_tri}] → Vector de Texto:{has_text} ({len(game.tactical_prediction) if game.tactical_prediction else 0} chars)")

                if Config.GROQ_API_KEY and game.tactical_prediction:
                    prompt = game.to_groq_prompt()
                    insight = await net.post_groq(prompt)
                    if insight:
                        game.groq_insight = insight
                elif not game.tactical_prediction:
                    log.warning(f"[{game.away_tri} @ {game.home_tri}] → Vector isolado insuficiente para análise Groq!")
            else:
                log.warning(f"[{game.away_tri} @ {game.home_tri}] → Falha de rede: Sem dados HTML")

            return game

        results = []
        for g in games:
            try:
                result = await process(g)
                results.append(result)
            except Exception as e:
                results.append(e)
                log.error(f"[{g.away_tri} @ {g.home_tri}] → Falha crítica operacional: {e}")
            await asyncio.sleep(3)

        valid = []
        for g, r in zip(games, results):
            if isinstance(r, Exception):
                log.error(f"[{g.away_tri} @ {g.home_tri}] → Erro de Processamento: {r}")
            else:
                valid.append(r)

        if valid:
            db.upsert_games(valid)

        with_text = sum(1 for g in valid if g.tactical_prediction)
        with_groq = sum(1 for g in valid if g.groq_insight)
        
        log.info(f"═══ Eficiência de Execução: {len(valid)} jogos | Integridade de Texto: {with_text} | Inferências Groq: {with_groq} ═══")

    finally:
        await net.close()

if __name__ == "__main__":
    asyncio.run(main())
