"""
NBA Scraper - Replicante V6.6.3 (Focus: Previsao da Redacao no Final)
Melhoria: Identifica o paragrafo de conclusao/recomendacao da redacao e 
          posiciona como secao final "PREVISAO DA REDACAO".
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [NBA-V6.6.3] %(message)s",
)
log = logging.getLogger(__name__)

BRT = ZoneInfo("America/Sao_Paulo")
ET  = ZoneInfo("America/New_York")


def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise EnvironmentError(f"Secret ausente: {name}")
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

class TeamForm(BaseModel):
    team_name: str
    position_conference: Optional[str] = None
    wins_last_10: Optional[int] = None

class Injury(BaseModel):
    player: str
    status: str
    details: Optional[str] = None

class TeamNews(BaseModel):
    injuries: List[Injury] = []

class TopScorer(BaseModel):
    player: str
    ppg: float

class TeamStats(BaseModel):
    points_scored_avg: Optional[float] = None
    points_allowed_avg: Optional[float] = None
    top_scorer: Optional[TopScorer] = None

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
        def implied_prob(odds: float) -> float:
            return 100 / odds if odds > 0 else 0

        home_implied = implied_prob(self.odds.v1) if self.odds and self.odds.v1 else 0
        away_implied = implied_prob(self.odds.v2) if self.odds and self.odds.v2 else 0

        home_injuries = ", ".join([f"{i.player} ({i.status})" for i in self.home_news.injuries]) if self.home_news else "Nenhuma"
        away_injuries = ", ".join([f"{i.player} ({i.status})" for i in self.away_news.injuries]) if self.away_news else "Nenhuma"

        home_form_str = f"{self.home_form.wins_last_10}/10" if self.home_form and self.home_form.wins_last_10 else "N/A"
        away_form_str = f"{self.away_form.wins_last_10}/10" if self.away_form and self.away_form.wins_last_10 else "N/A"

        return f"""Voce e o Estatistico Chefe de NBA. Analise este jogo.

## DADOS
{self.away_team} @ {self.home_team} | {self.game_date}

## MERCADO
- V1: {self.odds.v1 if self.odds else 'N/A'} ({home_implied:.1f}%)
- V2: {self.odds.v2 if self.odds else 'N/A'} ({away_implied:.1f}%)

## CONTEXTO
H2H: {self.h2h.total_matches if self.h2h else 'N/A'} jogos
Forma: Casa {home_form_str} vs Visitante {away_form_str}
Lesoes Casa: {home_injuries}
Lesoes Visitante: {away_injuries}

## ANALISE DA REDACAO
{self.tactical_prediction[:3000] if self.tactical_prediction else 'N/A'}

---
DIRETRIZ: Use modelo linear-pessimista. Desconte margem de seguranca nas medias ofensivas.

Retorne JSON:
{{
  "confidence_score": 0.0 a 5.0,
  "fair_line": "ex: O/U 215.5",
  "edge_percentage": 0.0 a 50.0,
  "key_factors": ["fator 1", "fator 2"],
  "recommendation": "OVER/UNDER/FAVORITE/DOG/PASS",
  "stake_units": 0.5 a 5.0,
  "reasoning": "explicacao em portugues"
}}"""


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
            log.warning("GROQ_API_KEY nao configurada")
            return None

        try:
            async with self.semaphore:
                log.info("  -> Groq processando...")
                response = await self.client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {Config.GROQ_API_KEY}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": Config.GROQ_MODEL,
                        "messages": [
                            {"role": "system", "content": "Voce e um analista de NBA especializado em betting. Responda APENAS em JSON valido."},
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
                log.info(f"  <- Groq: {insight.recommendation} (conf: {insight.confidence_score}/5)")
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


async def fetch_with_retry(net, url: str) -> Optional[str]:
    log.info(f"[FETCH] {url[-60:]}")

    html = await net.fetch(url, use_browser=True)
    if html:
        return html
    await asyncio.sleep(1.5)

    log.warning(f"[RETRY] browser=False -> {url[-40:]}")
    html = await net.fetch(url, use_browser=False)
    if html:
        return html
    await asyncio.sleep(2.5)

    log.warning(f"[RETRY FINAL] browser=True -> {url[-40:]}")
    html = await net.fetch(url, use_browser=True)

    if not html:
        log.error(f"[FAIL TOTAL] {url[-60:]}")
    return html


class NBAExtractor:

    # Padroes que indicam LIXO
    JUNK_PATTERNS = [
        r'\d+\.\d+\*?',
        r'[\+-]\d+\.?\d*',
        r'\d{2}\.\d{2}\.\d{2}.*NBA',
        r'Vitórias?\s*\d+%',
        r'jogou\s+\d+',
        r'BONUS|bovada|JOIN NOW',
        r'FIRST DEPOSIT',
        r'FOR NEW PLAYERS',
        r'Esta previsão vai ser correta',
        r'Previsões estatísticas',
        r'Posição na tabela',
        r'Group stage',
        r'Sem partidas|Sem dados',
        r'Resultados dos jogos',
        r'Estatísticas H2H',
        r'Palpites$',
        r'Detalhes$',
        r'O principal$',
    ]

    # Termos que indicam CONCLUSAO da redacao (recomendacao final)
    CONCLUSION_MARKERS = [
        "nossa redacao", "previsao da redacao", "escolha da redacao",
        "recomendamos", "sugerimos", "apostar em", "faz sentido apostar",
        "nossa aposta", "palpite final", "conclusao", "resumindo",
        "portanto", "em resumo", "para concluir", "dessa forma",
        "considerando tudo", "levando em conta", "por isso",
        "acreditamos que", "esperamos", "prevemos", "anticipamos",
        "total abaixo", "total acima", "handicap", "vitoria de",
        "under", "over", "favorito", "azarão", "dog",
    ]

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
            "Pistons": "Pistoes", "Hornets": "Hornets", "Wizards": "Wizards",
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

        log.info(f"Jogos extraidos: {len(games)}")
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

        # NOVO: Extrair texto com PREVISAO DA REDACAO no final
        game.tactical_prediction = self._extract_with_editorial_conclusion(soup, game)

        log.info(f"  -> Texto: {len(game.tactical_prediction) if game.tactical_prediction else 0} chars")

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
            elif "vitória" in text_lower and ("casa" in text_lower or "mandante" in text_lower):
                pick.recommendation = "vitoria_casa"
            elif "handicap" in text_lower or "spread" in text_lower or "linha" in text_lower:
                pick.recommendation = "handicap"
            elif "over" in text_lower or "acima" in text_lower or "mais de" in text_lower:
                pick.recommendation = "over"
            elif "under" in text_lower or "abaixo" in text_lower or "menos de" in text_lower:
                pick.recommendation = "under"
            elif "pass" in text_lower or "ficar de fora" in text_lower or "não apostar" in text_lower:
                pick.recommendation = "pass"
            else:
                pick.recommendation = "analise_neutra"

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

            paragraphs = container.find_all("p")
            for p in paragraphs:
                p_text = p.get_text(strip=True)
                if len(p_text) > 50:
                    pick.explanation = p_text
                    break

            return pick if pick.recommendation else None
        except Exception as e:
            log.warning(f"Erro extraindo editorial: {e}")
            return None

    # ========================================================================
    # V6.6.3: Extrair texto com PREVISAO DA REDACAO no final
    # ========================================================================
    def _extract_with_editorial_conclusion(self, soup: BeautifulSoup, game: GameData) -> Optional[str]:
        """
        Extrai texto analitico puro e posiciona a conclusao da redacao no final.
        """
        # Passo 1: Coletar todos os paragrafos analiticos
        all_paragraphs = []
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)

            if len(text) < 80:
                continue
            if self._is_junk(text):
                continue
            if not self._is_analytical(text):
                continue
            if re.search(r'\d{2}\.\d{2}\.\d{2}.*?(NBA|NBB).*?\d{3}', text):
                continue

            # Limpa emojis
            text = re.sub(r'^[\U0001F300-\U0001F9FF\s]+', '', text).strip()

            all_paragraphs.append(text)

        if not all_paragraphs:
            return None

        # Passo 2: Separar paragrafos de CONCLUSAO dos demais
        conclusion_paragraphs = []
        body_paragraphs = []

        for para in all_paragraphs:
            if self._is_conclusion(para):
                conclusion_paragraphs.append(para)
            else:
                body_paragraphs.append(para)

        # Passo 3: Montar o texto final
        parts = []

        # Corpo da analise (sem conclusao)
        if body_paragraphs:
            parts.append("ANALISE TATICA\n" + "="*40)
            parts.extend(body_paragraphs)

        # Previsao da Redacao (conclusao)
        if conclusion_paragraphs:
            if parts:
                parts.append("")  # linha em branco
            parts.append("PREVISAO DA REDACAO\n" + "="*40)
            parts.extend(conclusion_paragraphs)

        if not parts:
            return None

        result = "\n\n".join(parts)

        # Remove duplicatas
        lines = result.split("\n")
        seen = set()
        unique_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped and stripped not in seen:
                seen.add(stripped)
                unique_lines.append(stripped)

        result = "\n\n".join(unique_lines)

        if len(result) < 200:
            return None

        return result

    def _is_conclusion(self, text: str) -> bool:
        """Verifica se o paragrafo e uma conclusao/recomendacao da redacao."""
        text_lower = text.lower()

        # Deve ter pelo menos 1 marcador de conclusao
        has_marker = any(marker in text_lower for marker in self.CONCLUSION_MARKERS)

        # E deve ter conteudo analitico
        has_analytical = self._is_analytical(text)

        return has_marker and has_analytical

    def _is_junk(self, text: str) -> bool:
        text_lower = text.lower()

        junk_words = [
            "bovada", "join now", "bonus", "first deposit", "for new players",
            "cadastre-se", "apostar", "bonus", "bet", "promocao", "odds",
            "esta previsao vai ser correta", "previsoes estatisticas",
            "posicao na tabela", "group stage", "sem partidas", "sem dados",
        ]
        if any(w in text_lower for w in junk_words):
            return True

        for pattern in self.JUNK_PATTERNS:
            if re.search(pattern, text, re.I):
                return True

        if re.match(r'^[\d\.\s\-\+\*\$\%\(\)\[\]]+$', text.strip()):
            return True

        return False

    def _is_analytical(self, text: str) -> bool:
        text_lower = text.lower()

        analytical_terms = [
            "equipe", "jogo", "ataque", "defesa", "pontos", "vitoria", "derrota",
            "serie", "playoff", "temporada", "media", "desempenho", "treinador",
            "jogador", "quadra", "confronto", "partida", "resultado", "tecnico",
            "estrategia", "tatica", "ritmo", "transicao", "rebote", "assistencia",
            "arremesso", "cesta", "pivo", "ala", "armador", "substituicao",
            "vantagem", "desvantagem", "fator", "tendencia", "probabilidade",
            "analise", "previsao", "fundamentada", "argumento", "contexto",
            "dominar", "controlar", "superioridade", "pressao", "momentum",
            "lesao", "ausencia", "escalacao", "titular", "banco", "reserva",
            "nossa redacao", "recomendamos", "sugerimos", "apostar em",
            "faz sentido", "palpite", "conclusao", "portanto", "em resumo",
        ]

        matches = sum(1 for term in analytical_terms if term in text_lower)
        return matches >= 2


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
                    "game_status", "scraped_at", "tactical_prediction", "groq_insight",
                }

            self._known_columns = columns
            log.info(f"Colunas detectadas: {len(columns)}")
            return columns

        except Exception as e:
            log.warning(f"Nao foi possivel detectar colunas: {e}")
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
        skipped = 0

        for g in unique:
            if not g.tactical_prediction or len(g.tactical_prediction) < 200:
                log.warning(f"  -> {g.slug}: REJEITADO (sem texto analitico)")
                skipped += 1
                continue

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
            log.warning(f"Nenhum jogo passou no quality gate. {skipped} rejeitados.")
            return

        try:
            self.sb.table("nba_games_schedule").upsert(rows, on_conflict="slug").execute()

            for r in rows:
                text_ok = "OK" if r.get("tactical_prediction") else "FALHA"
                groq_ok = "OK" if r.get("groq_insight") else "FALHA"
                log.info(f"  -> Persistido: {r['slug'][:40]} | Texto:{text_ok} | Groq:{groq_ok}")

            log.info(f"Total: {len(rows)} persistidos, {skipped} rejeitados")

        except Exception as e:
            log.error(f"Erro ao persistir: {e}")
            raise


async def main():
    log.info("=== Replicante V6.6.3 (Previsao da Redacao no Final) ===")

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
            needs_update = not cached.get("has_text") or not cached.get("has_groq")

            if not needs_update and cached.get("game_date") == game.game_date:
                log.info(f"[{game.away_tri} @ {game.home_tri}] -> Cache OK")
                return None

            pred_url = f"{game.source_url}-prediction"

            log.info(f"[{game.away_tri} @ {game.home_tri}] -> Fetching...")

            html = await fetch_with_retry(net, pred_url)

            if html:
                ext.extract_full_prediction(html, game)

                has_text = bool(game.tactical_prediction)
                text_len = len(game.tactical_prediction) if game.tactical_prediction else 0

                log.info(f"[{game.away_tri} @ {game.home_tri}] -> Texto:{has_text} ({text_len} chars)")

                if Config.GROQ_API_KEY and game.tactical_prediction and text_len >= 200:
                    prompt = game.to_groq_prompt()
                    insight = await net.post_groq(prompt)
                    if insight:
                        game.groq_insight = insight
                elif not game.tactical_prediction:
                    log.warning(f"[{game.away_tri} @ {game.home_tri}] -> Sem texto analitico!")
            else:
                log.warning(f"[{game.away_tri} @ {game.home_tri}] -> Sem HTML")

            return game

        results = []
        for g in games:
            try:
                result = await process(g)
                if result is not None:
                    results.append(result)
            except Exception as e:
                log.error(f"[{g.away_tri} @ {g.home_tri}] -> Erro: {e}")

            await asyncio.sleep(1.5)

        valid = [g for g in results if g is not None]

        if valid:
            db.upsert_games(valid)

        with_text = sum(1 for g in valid if g.tactical_prediction)
        with_groq = sum(1 for g in valid if g.groq_insight)

        log.info(f"=== Resumo: {len(valid)} processados | Texto:{with_text} | Groq:{with_groq} ===")

    finally:
        await net.close()


if __name__ == "__main__":
    asyncio.run(main())
