import asyncio
import random
import httpx
import logging
import os
from datetime import datetime
from bs4 import BeautifulSoup
from typing import Optional, List
from supabase import create_client

# ================= CONFIG =================
class Config:
    SCRAPINGANT_KEY = os.getenv("SCRAPINGANT_KEY")
    GROQ_API_KEY = os.getenv("GROQ_API_KEY")
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    PREDICTIONS_URL = "https://scores24.live/pt/basketball/l-usa-nba/predictions"

# ================= LOG =================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("NBA-V6.8")

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

    async def fetch(self, url, use_browser=True, retries=3):
        for attempt in range(retries):
            try:
                html = await self._request(url, use_browser)
                if html:
                    return html
            except Exception as e:
                log.warning(f"[FETCH] tentativa {attempt+1}: {e}")

            await asyncio.sleep((2**attempt) + random.uniform(0.5,1.5))
            use_browser = not use_browser

        return None

    async def _request(self, url, use_browser):
        params = {
            "url": url,
            "x-api-key": Config.SCRAPINGANT_KEY,
            "browser": "true" if use_browser else "false"
        }

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(self.base_url, params=params)

            if r.status_code == 200:
                return r.text
            if r.status_code in [403,409,429]:
                raise Exception(f"Bloqueio {r.status_code}")

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
                        "messages": [{"role":"user","content":prompt}]
                    }
                )
                if r.status_code == 200:
                    return r.json()["choices"][0]["message"]["content"]
        except Exception as e:
            log.error(e)

        return None

# ================= DB =================
class DatabaseManager:
    def __init__(self):
        self.client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)

    def upsert_games(self, games: List[GameData]):
        payload = [g.to_dict() for g in games]

        try:
            res = self.client.table("nba_games_schedule") \
                .upsert(payload, on_conflict="slug") \
                .execute()

            log.info(f"Supabase: {len(payload)} registros enviados")

        except Exception as e:
            log.error(f"Supabase erro: {e}")

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

        body = soup.get_text(" ", strip=True)
        if len(body) > 2000:
            game.tactical_prediction = body[:15000]

# ================= MAIN =================
async def main():
    log.info("═══ NBA SCRAPER V6.8 SUPABASE ═══")

    net = NetworkClient()
    ext = NBAExtractor()
    db = DatabaseManager()

    html = await net.fetch(Config.PREDICTIONS_URL)

    games = ext.extract_games_list(html)

    async def process(game):
        html = await net.fetch(f"{game.source_url}-prediction")

        if not html:
            return game

        ext.extract_full_prediction(html, game)

        if game.tactical_prediction:
            game.groq_insight = await net.post_groq(game.to_groq_prompt())

        return game

    results = []

    for g in games:
        r = await process(g)
        results.append(r)
        await asyncio.sleep(1.5)

    db.upsert_games(results)

# ================= RUN =================
if __name__ == "__main__":
    asyncio.run(main())
