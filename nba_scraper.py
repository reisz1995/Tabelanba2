import asyncio
import httpx
import logging
import random
from datetime import datetime
from typing import Optional, List
from bs4 import BeautifulSoup
from supabase import create_client

# ================= CONFIG =================
class Config:
    SCRAPINGANT_KEY = "SUA_API_KEY"
    GROQ_API_KEY = "SUA_GROQ_KEY"
    SUPABASE_URL = "SUA_URL"
    SUPABASE_KEY = "SUA_KEY"
    PREDICTIONS_URL = "https://scores24.live/pt/basketball/l-usa-nba/predictions"

# ================= LOG =================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("NBA-V6.5.5")

# ================= MODEL =================
class GameData:
    def __init__(self, slug, home, away, url):
        self.slug = slug
        self.home_team = home
        self.away_team = away
        self.source_url = url
        self.tactical_prediction = None
        self.groq_insight = None
        self.game_date = datetime.utcnow().date().isoformat()

        self.home_tri = home[:3].upper()
        self.away_tri = away[:3].upper()

    def to_dict(self):
        return {
            "slug": self.slug,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "game_date": self.game_date,
            "tactical_prediction": self.tactical_prediction,
            "groq_insight": self.groq_insight,
            "scraped_at": datetime.utcnow().isoformat()
        }

    def to_groq_prompt(self):
        return f"Analise o jogo {self.away_team} vs {self.home_team} e gere insight objetivo."

# ================= NETWORK =================
class NetworkClient:
    def __init__(self):
        self.base_url = "https://api.scrapingant.com/v2/general"

    async def fetch(self, url, use_browser=True):
        params = {
            "url": url,
            "x-api-key": Config.SCRAPINGANT_KEY,
            "browser": "true" if use_browser else "false",
            "proxy_country": "us"
        }

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(self.base_url, params=params)

            if r.status_code == 200:
                return r.text

            if r.status_code in [403, 409, 429]:
                raise Exception(f"HTTP {r.status_code}")

            return None

    async def post_groq(self, prompt):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {Config.GROQ_API_KEY}"
                    },
                    json={
                        "model": "llama3-70b-8192",
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.2
                    }
                )

                if r.status_code == 200:
                    return r.json()["choices"][0]["message"]["content"]

        except Exception as e:
            log.error(f"[GROQ] {e}")

        return None

    async def close(self):
        pass

# ================= RETRY (PATCH PRINCIPAL) =================
async def fetch_with_retry(net, url: str) -> Optional[str]:
    log.info(f"[FETCH] {url[-60:]}")

    try:
        html = await net.fetch(url, use_browser=True)
        if html:
            return html
    except Exception as e:
        log.warning(f"[RETRY1] {e}")

    await asyncio.sleep(2)

    try:
        html = await net.fetch(url, use_browser=False)
        if html:
            return html
    except Exception as e:
        log.warning(f"[RETRY2] {e}")

    await asyncio.sleep(3)

    try:
        html = await net.fetch(url, use_browser=True)
        return html
    except Exception as e:
        log.error(f"[FAIL] {url[-50:]} → {e}")

    return None

# ================= EXTRACTOR =================
class NBAExtractor:
    def extract_games_list(self, html):
        soup = BeautifulSoup(html, "html.parser")
        links = soup.find_all("a", href=True)

        games = []

        for link in links:
            href = link["href"]

            if "m-" in href:
                slug = href.split("/")[-1]

                games.append(
                    GameData(
                        slug,
                        "Home",
                        "Away",
                        f"https://scores24.live{href}"
                    )
                )

        return games[:5]

    def extract_full_prediction(self, html, game):
        soup = BeautifulSoup(html, "html.parser")

        article = soup.find("article")
        if article:
            text = article.get_text(" ", strip=True)
            if len(text) > 1000:
                game.tactical_prediction = text
                return

# ================= DATABASE =================
class DatabaseManager:
    def __init__(self):
        if not Config.SUPABASE_URL or not Config.SUPABASE_KEY:
            log.warning("Sem Supabase configurado")
            self.client = None
        else:
            self.client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)

    def upsert_games(self, games):
        if not self.client:
            return

        payload = [g.to_dict() for g in games]

        try:
            self.client.table("nba_games_schedule") \
                .upsert(payload, on_conflict="slug") \
                .execute()

            log.info(f"Supabase OK: {len(payload)} jogos")

        except Exception as e:
            log.error(f"Supabase erro: {e}")

# ================= MAIN =================
async def main():
    log.info("═══ NBA SCRAPER ORIGINAL CORRIGIDO ═══")

    net = NetworkClient()
    ext = NBAExtractor()
    db = DatabaseManager()

    try:
        html_list = await net.fetch(Config.PREDICTIONS_URL, use_browser=False)

        if not html_list:
            log.error("Falha ao carregar lista")
            return

        games = ext.extract_games_list(html_list)

        if not games:
            log.info("Sem jogos")
            return

        async def process(game):
            log.info(f"[{game.away_tri} @ {game.home_tri}] → start")

            pred_url = f"{game.source_url}-prediction"

            html = await fetch_with_retry(net, pred_url)

            if not html:
                log.warning(f"[{game.away_tri} @ {game.home_tri}] → SEM HTML")
                return game

            ext.extract_full_prediction(html, game)

            # 🔥 fallback
            if not game.tactical_prediction:
                soup = BeautifulSoup(html, "html.parser")
                body = soup.get_text(" ", strip=True)

                if len(body) > 2000:
                    log.warning(f"[{game.away_tri} @ {game.home_tri}] → fallback BODY")
                    game.tactical_prediction = body[:15000]

            if game.tactical_prediction:
                game.groq_insight = await net.post_groq(game.to_groq_prompt())

            log.info(
                f"[{game.away_tri} @ {game.home_tri}] "
                f"Texto:{bool(game.tactical_prediction)}"
            )

            return game

        results = []

        for g in games:
            result = await process(g)
            results.append(result)

            # 🔥 delay aumentado (ANTI-409)
            await asyncio.sleep(2.5)

        db.upsert_games(results)

        log.info(f"Finalizado: {len(results)} jogos")

    finally:
        await net.close()

# ================= RUN =================
if __name__ == "__main__":
    asyncio.run(main())
