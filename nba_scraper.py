"""
NBA Scraper - Replicante V6.4 (Async Engine + Groq Insights)
Integração completa:
  - Captura dados estruturados da scores24
  - Envia para Groq API para análise inteligente
  - Armazena insights gerados no Supabase
"""

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
from pydantic import BaseModel, Field
from supabase import create_client, Client

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [NBA-V6.4] %(message)s",
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
    SUPABASE_URL     = _require_env("SUPABASE_URL")
    SUPABASE_KEY     = _require_env("SUPABASE_SERVICE_KEY")
    SCRAPINGANT_KEY  = os.environ.get("SCRAPINGANT_API_KEY", "")
    GROQ_API_KEY     = os.environ.get("GROQ_API_KEY", "")
    GROQ_MODEL       = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
    BASE_URL         = "https://scores24.live"
    PREDICTIONS_URL  = f"{BASE_URL}/pt/basketball/l-usa-nba/predictions"
    CONCURRENCY_LIMIT = 5


# ─── Modelos de Dados ─────────────────────────────────────────────────────────

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
    confidence_score: float = Field(..., ge=0, le=5)  # 0-5 estrelas
    fair_line: str  # Linha justa calculada
    edge_percentage: float  # Edge vs mercado (%)
    key_factors: List[str]  # Fatores decisivos
    recommendation: str  # "OVER", "UNDER", "FAVORITE", "DOG", "PASS"
    stake_units: float = Field(..., ge=0.5, le=5)  # 0.5 a 5 unidades
    reasoning: str  # Explicação completa
    generated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())



class GameData(BaseModel):
    # Identificação
    slug: str
    game_date: str
    game_time_et: Optional[str]
    game_time_brt: str
    
    # Times
    home_team: str
    away_team: str
    home_team_pt: str
    away_team_pt: str
    home_tri: str
    away_tri: str
    
    # Metadados
    source_url: str
    confidence_pct: Optional[int] = None
    game_status: str = "Scheduled"
    scraped_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    # Dados estruturados
    odds: Optional[OddsData] = None
    h2h: Optional[H2HData] = None
    home_form: Optional[TeamForm] = None
    away_form: Optional[TeamForm] = None
    home_news: Optional[TeamNews] = None
    away_news: Optional[TeamNews] = None
    home_stats: Optional[TeamStats] = None
    away_stats: Optional[TeamStats] = None
    editorial_pick: Optional[EditorialPick] = None
    
    # Texto e IA
    tactical_prediction: Optional[str] = None
    groq_insight: Optional[GroqInsight] = None

    def to_groq_prompt(self) -> str:
        """Gera prompt otimizado para Groq analisar o jogo com viés linear-pessimista."""
        
        # Calcula odds implícitas
        def implied_prob(odds: float) -> float:
            return 100 / odds if odds > 0 else 0
        
        home_implied = implied_prob(self.odds.v1) if self.odds and self.odds.v1 else 0
        away_implied = implied_prob(self.odds.v2) if self.odds and self.odds.v2 else 0
        
        # Formata lesões
        home_injuries = ", ".join([f"{i.player} ({i.status})" for i in self.home_news.injuries]) if self.home_news else "Nenhuma"
        away_injuries = ", ".join([f"{i.player} ({i.status})" for i in self.away_news.injuries]) if self.away_news else "Nenhuma"
        
        # Formata forma recente
        home_form_str = f"{self.home_form.wins_last_10}/10 vitórias" if self.home_form and self.home_form.wins_last_10 else "N/A"
        away_form_str = f"{self.away_form.wins_last_10}/10 vitórias" if self.away_form and self.away_form.wins_last_10 else "N/A"
        
        return f"""Você é o Estatístico Chefe de NBA. Analise este jogo e forneça recomendação profissional.

## DADOS DO JOGO
{self.away_team} @ {self.home_team} | {self.game_date}

## MERCADO
- Odds Casa (V1): {self.odds.v1 if self.odds else 'N/A'} (implícita: {home_implied:.1f}%)
- Odds Visitante (V2): {self.odds.v2 if self.odds else 'N/A'} (implícita: {away_implied:.1f}%)

## CONTEXTO
**H2H:** {self.h2h.total_matches if self.h2h else 'N/A'} jogos, casa vence {self.h2h.home_win_pct if self.h2h else 'N/A'}%
**Forma Recente:**
- Casa: {home_form_str}, Posição: {self.home_form.position_conference if self.home_form else 'N/A'}
- Visitante: {away_form_str}, Posição: {self.away_form.position_conference if self.away_form else 'N/A'}
**Lesões:**
- Casa: {home_injuries}
- Visitante: {away_injuries}
**Estatísticas:**
- Casa: {self.home_stats.points_scored_avg if self.home_stats else 'N/A'} marcados / {self.home_stats.points_allowed_avg if self.home_stats else 'N/A'} sofridos
- Visitante: {self.away_stats.points_scored_avg if self.away_stats else 'N/A'} marcados / {self.away_stats.points_allowed_avg if self.away_stats else 'N/A'} sofridos

---
ATENÇÃO - DIRETRIZ DE CÁLCULO (ENTROPIA):
Aplique um modelo linear-pessimista para as projeções de pontos (Over/Under). Assuma a entropia natural do jogo (cansaço acumulado, desfalques repentinos, e o clássico blowout no 4º quarto). Você deve descontar uma margem de segurança pessimista nas médias ofensivas brutas antes de validar a linha justa.

Forneça sua análise em JSON estrito:
{{
  "confidence_score": 0.0 a 5.0,
  "fair_line": "ex: +3.5 ou -2.5 ou O/U 225.5",
  "edge_percentage": 0.0 a 50.0,
  "key_factors": ["fator 1", "fator 2", "fator 3"],
  "recommendation": "OVER ou UNDER ou FAVORITE ou DOG ou PASS",
  "stake_units": 0.5 a 5.0,
  "reasoning": "explicação tática/matemática em português"
}}"""
      

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
                    log.info(f"Fetch: {url[:60]}...")
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
        """Envia prompt para Groq API e retorna insight estruturado."""
        if not Config.GROQ_API_KEY:
            log.warning("GROQ_API_KEY não configurada")
            return None
        
        try:
            async with self.semaphore:
                log.info("  → Enviando para Groq...")
                
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
                log.info(f"  ← Groq: {insight.recommendation} (confiança: {insight.confidence_score}/5)")
                return insight
                
        except Exception as e:
            log.error(f"Erro Groq API: {e}")
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


# ─── Motor de Extração (Simplificado V6.3) ────────────────────────────────────
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

        log.info(f"Jogos extraídos: {len(games)}")
        return games

    def extract_full_prediction(self, html: str, game: GameData) -> None:
        """Extrai todos os dados da página de previsão."""
        if not html:
            return

        soup = BeautifulSoup(html, "html.parser")
        
        # 1. Odds
        game.odds = self._extract_odds(soup)
        
        # 2. H2H
        game.h2h = self._extract_h2h(soup)
        
        # 3. Forma
        game.home_form, game.away_form = self._extract_form(soup, game)
        
        # 4. Lesões
        game.home_news, game.away_news = self._extract_news(soup, game)
        
        # 5. Stats
        game.home_stats, game.away_stats = self._extract_stats(soup, game)
        
        # 6. Editorial
        game.editorial_pick = self._extract_editorial(soup)
        
        # 7. Texto
        game.tactical_prediction = self._extract_text(soup)

    def _extract_odds(self, soup: BeautifulSoup) -> Optional[OddsData]:
        try:
            odds = OddsData()
            all_text = soup.get_text()
            
            # Padrão: V1 1.73 X 15.5 V2 2.39
            pattern = r'V1.*?(\d+\.\d+).*?X.*?(\d+\.\d+).*?V2.*?(\d+\.\d+)'
            match = re.search(pattern, all_text, re.DOTALL)
            if match:
                odds.v1 = float(match.group(1))
                odds.x = float(match.group(2))
                odds.v2 = float(match.group(3))
            
            return odds if odds.v1 else None
        except Exception:
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
            
            # Percentuais
            pcts = re.findall(r'(\d+)%', text)
            if len(pcts) >= 2:
                h2h.home_win_pct = float(pcts[0])
            
            # Vitórias
            wins = re.findall(r'(\d+)\s*Vitórias?', text)
            if len(wins) >= 2:
                h2h.home_wins = int(wins[0])
                h2h.away_wins = int(wins[1])
                h2h.total_matches = h2h.home_wins + h2h.away_wins
            
            # Jogos recentes
            rows = container.find_all("tr")
            for row in rows[:5]:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 4:
                    try:
                        scores = re.findall(r'(\d+)', cells[3].get_text())
                        if len(scores) >= 2:
                            h2h.recent_matches.append(H2HMatch(
                                date=cells[0].get_text(strip=True),
                                home_team=cells[1].get_text(strip=True),
                                away_team=cells[2].get_text(strip=True),
                                home_score=int(scores[0]),
                                away_score=int(scores[1])
                            ))
                    except Exception:
                        continue
            
            return h2h
        except Exception:
            return None

    def _extract_form(self, soup: BeautifulSoup, game: GameData) -> tuple:
        home_form = TeamForm(team_name=game.home_team)
        away_form = TeamForm(team_name=game.away_team)
        
        try:
            sections = soup.find_all(string=re.compile(r"Resultados dos jogos", re.I))
            
            for section in sections:
                container = section.find_parent(["div", "section"])
                if not container:
                    continue
                
                text = container.get_text()
                is_home = game.home_team.split()[-1] in text
                
                target = home_form if is_home else away_form
                
                # Vitórias últimos 10
                win_match = re.search(r'(\d+)\s*vitórias?\s*nos\s*últimos\s*dez', text, re.I)
                if win_match:
                    target.wins_last_10 = int(win_match.group(1))
                
                # Posição
                pos_match = re.search(r'(\d+)[º°o].*?lugar.*?Conferência', text, re.I)
                if pos_match:
                    target.position_conference = pos_match.group(1) + "º"
            
            return home_form, away_form
        except Exception:
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
            
            # Extrai lesões por time
            text = container.get_text()
            
            # Padrões de lesão
            patterns = [
                r'([A-Z][a-z]+)\s+está\s+(fora|duvida|dúvida|provável)',
                r'participação\s+de\s+([A-Z][a-z]+)\s+([\w\s]+)',
            ]
            
            for pattern in patterns:
                for match in re.finditer(pattern, text, re.I):
                    player = match.group(1)
                    status = match.group(2).lower()
                    
                    injury = Injury(
                        player=player,
                        status="fora" if "fora" in status else "dúvida" if "duvida" in status or "dúvida" in status else "provável"
                    )
                    
                    # Determina time pelo contexto
                    is_home = game.home_team.split()[-1] in text[:text.find(player)]
                    if is_home:
                        home_news.injuries.append(injury)
                    else:
                        away_news.injuries.append(injury)
            
            return home_news, away_news
        except Exception:
            return home_news, away_news

    def _extract_stats(self, soup: BeautifulSoup, game: GameData) -> tuple:
        home_stats = TeamStats()
        away_stats = TeamStats()
        
        try:
            # Artilheiros
            section = soup.find(string=re.compile(r"Artilheiros", re.I))
            if section:
                container = section.find_parent(["div", "section"])
                if container:
                    text = container.get_text()
                    
                    # Booker... 26.1 pontos
                    matches = re.finditer(r'([A-Z][a-z]+)[^,]+?(\d+\.\d+)\s*pontos?', text)
                    for match in matches:
                        player = match.group(1)
                        ppg = float(match.group(2))
                        
                        is_home = game.home_team.split()[-1] in text[:text.find(player)]
                        scorer = TopScorer(player=player, ppg=ppg)
                        
                        if is_home:
                            home_stats.top_scorer = scorer
                        else:
                            away_stats.top_scorer = scorer
            
            # 3PT
            section = soup.find(string=re.compile(r"Arremessos de Três", re.I))
            if section:
                container = section.find_parent(["div", "section"])
                if container:
                    text = container.get_text()
                    pcts = re.findall(r'(\d+\.\d+)%', text)
                    ranks = re.findall(r'(\d+)[º°o].*?posição', text, re.I)
                    
                    if len(pcts) >= 2:
                        home_stats.three_point = ThreePointStats(
                            pct=float(pcts[0]),
                            rank=ranks[0] + "º" if len(ranks) > 0 else None
                        )
                        away_stats.three_point = ThreePointStats(
                            pct=float(pcts[1]),
                            rank=ranks[1] + "º" if len(ranks) > 1 else None
                        )
            
            # Pontos do texto de análise
            analysis = soup.find(attrs={"data-testid": "DisplayContent"})
            if analysis:
                text = analysis.get_text()
                pts = re.findall(r'(\d+\.\d+)\s*pontos', text)
                if len(pts) >= 4:
                    home_stats.points_scored_avg = float(pts[0])
                    home_stats.points_allowed_avg = float(pts[1])
                    away_stats.points_scored_avg = float(pts[2])
                    away_stats.points_allowed_avg = float(pts[3])
            
            return home_stats, away_stats
        except Exception:
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
            
            # Handicap
            handicap = re.search(r'([\+-]\d+\.?\d*)', text)
            if handicap:
                pick.handicap_line = handicap.group(1)
            
            # Odds
            odds = re.search(r'(\d+\.\d+)\*?', text)
            if odds:
                pick.odds = float(odds.group(1))
            
            # Explicação
            p = container.find("p")
            if p:
                pick.explanation = p.get_text(strip=True)
            
            return pick if pick.recommendation else None
        except Exception:
            return None

    def _extract_text(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            container = soup.find(attrs={"data-testid": "DisplayContent"}) or \
                       soup.find("article") or soup.find("main")
            
            if not container:
                return None
            
            # Remove lixo
            for elem in container.find_all(["button", "script", "style"]):
                elem.decompose()
            
            sections = []
            last = ""
            
            for elem in container.find_all(["h2", "h3", "h4", "p", "li"]):
                text = elem.get_text(strip=True)
                
                if not text or len(text) < 15 or text == last:
                    continue
                if any(x in text.lower() for x in ["apostar", "registre", "bônus", "bet"]):
                    continue
                
                last = text
                
                if elem.name in ["h2", "h3", "h4"]:
                    sections.append(f"\n{text}\n{'=' * len(text)}")
                else:
                    sections.append(text)
            
            result = "\n\n".join(sections)
            return result if len(result) > 200 else None
        except Exception:
            return None


# ─── Persistência ─────────────────────────────────────────────────────────────
class DatabaseManager:
    def __init__(self):
        self.sb: Client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)

    def get_cached(self) -> Dict[str, dict]:
        res = (
            self.sb.table("nba_games_schedule")
            .select("slug, game_date, groq_insight")
            .execute()
        )
        return {
            row["slug"]: {
                "game_date": row.get("game_date"),
                "has_groq": bool(row.get("groq_insight")),
            }
            for row in res.data
        }


    def upsert_games(self, games: List[GameData]):
        # O filtro de exclusão bloqueia as anomalias dimensionais que não existem na tabela
        rows = [
            game.model_dump(
                mode="json",
                exclude={
                    "odds", 
                    "h2h", 
                    "home_form", 
                    "away_form", 
                    "home_news", 
                    "away_news", 
                    "home_stats", 
                    "away_stats", 
                    "editorial_pick"
                }
            ) for game in games
        ]
        
        try:
            self.sb.table("nba_games_schedule").upsert(rows, on_conflict="slug").execute()
            log.info("Persistência matriz-relacional concluída com sucesso.")
        except Exception as e:
            log.error(f"Falha de sincronia: {e}")


# ─── Orquestrador Principal ───────────────────────────────────────────────────
async def main():
    log.info("═══ Replicante V6.4 (Scraper + Groq Insights) ═══")

    # Valida secrets
    if not Config.SCRAPINGANT_KEY:
        log.error("SCRAPINGANT_API_KEY ausente")
        return
    if not Config.GROQ_API_KEY:
        log.warning("GROQ_API_KEY ausente - insights desativados")

    net = NetworkClient()
    ext = NBAExtractor()
    db = DatabaseManager()

    try:
        # 1. Lista jogos
        html_list = await net.fetch(Config.PREDICTIONS_URL)
        if not html_list:
            log.error("Falha ao carregar lista")
            return

        today = datetime.now(BRT).strftime("%Y-%m-%d")
        games = ext.extract_games_list(html_list, today)

        if not games:
            log.info(f"Sem jogos para {today}")
            return

        cache = db.get_cached()

        # 2. Processa cada jogo
        async def process(game: GameData) -> GameData:
            cached = cache.get(game.slug, {})
            
            # Verifica se precisa reprocessar
            if cached.get("has_groq") and cached.get("game_date") == game.game_date:
                log.info(f"[{game.away_tri} @ {game.home_tri}] → Cache completo")
                return game

            # Extrai dados da página
            pred_url = f"{game.source_url}-prediction"
            html = await net.fetch(pred_url)
            
            if html:
                ext.extract_full_prediction(html, game)
                log.info(f"[{game.away_tri} @ {game.home_tri}] → "
                        f"Dados: odds={bool(game.odds)}, h2h={bool(game.h2h)}, "
                        f"news={bool(game.home_news)}")

                # Gera insight com Groq (se tiver dados mínimos)


                # Gera insight com Groq (permite projeção cega caso as odds atrasem)
                if Config.GROQ_API_KEY and (game.tactical_prediction or (game.home_form and game.away_form)):
                    log.info(f"[{game.away_tri} @ {game.home_tri}] → Iniciando inferência (Odds no mercado: {bool(game.odds)})")
                    prompt = game.to_groq_prompt()
                    insight = await net.post_groq(prompt)
                    if insight:
                        game.groq_insight = insight
            else:
                log.warning(f"[{game.away_tri} @ {game.home_tri}] → Sem HTML")
            
            return game

        # Executa em paralelo com limitação
        results = await asyncio.gather(
            *(process(g) for g in games),
            return_exceptions=True
        )

        # 3. Filtra e persiste
        valid = []
        for g, r in zip(games, results):
            if isinstance(r, Exception):
                log.error(f"[{g.away_tri} @ {g.home_tri}] → Erro: {r}")
            else:
                valid.append(r)

        db.upsert_games(valid)

        # 4. Resumo
        with_groq = sum(1 for g in valid if g.groq_insight)
        log.info(f"═══ Concluído: {len(valid)} jogos, {with_groq} com insights Groq ═══")

    finally:
        await net.close()


if __name__ == "__main__":
    asyncio.run(main())
