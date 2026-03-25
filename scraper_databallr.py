#!/usr/bin/env python3
"""
Módulo Extrator NBA [Databallr -> Supabase]
Versão: 4.1 (Hybrid Resilient Engine - Teams Topology)
Estética: Replicante / Architect-Engineer
"""

import os
import json
import logging
import re
from datetime import datetime, timezone
from json import JSONDecodeError
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from bs4 import BeautifulSoup
from supabase import create_client, Client

# Configuração de Telemetria HUD
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class DataballrScraper:
    PERIOD_MAP = {
        "last14": "last_14_days",
        "last30": "last_30_days",
        "full_season": "full_season",
    }

    def __init__(self, period: str = "last14", season: str = "2025-26"):
        self.base_url = "https://databallr.com"
        self.period = os.getenv("DATABALLR_PERIOD", period)
        self.period_label = self.PERIOD_MAP.get(self.period, "last_14_days")
        self.season = os.getenv("DATABALLR_SEASON", season)
        
        now_utc = datetime.now(timezone.utc)
        self.current_date = now_utc.date().isoformat()
        self.current_timestamp = now_utc.isoformat()
        
        # Protocolo de Rede: Camuflagem e Resiliência
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[403, 429, 500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Referer': f"{self.base_url}/teams",
            'X-Requested-With': 'XMLHttpRequest'
        })
        
        # Injeção de Dependência: Supabase Auth
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            raise EnvironmentError("[SYS-ERR] Matriz de credenciais incompleta.")
        self.supabase: Client = create_client(url, key)

    def _clean_numeric(self, text: str) -> float:
        """Expurgar caracteres não-numéricos (rankings, sinais, whitespace)."""
        if not text or text.strip() in ["-", ""]: return 0.0
        try:
            clean = re.sub(r'#\d+|[+% \n\r]', '', text)
            return float(clean)
        except (ValueError, TypeError):
            return 0.0

    def _extract_teams_from_next_data(self, soup: BeautifulSoup) -> list:
        """Extrair payload de times do bloco __NEXT_DATA__ com tolerância a falhas."""
        script_tag = soup.find('script', id='__NEXT_DATA__')
        if not script_tag or not script_tag.string:
            return []

        try:
            payload = json.loads(script_tag.string)
        except JSONDecodeError:
            logger.warning("[VAL-WARN] __NEXT_DATA__ inválido. Fallback para parser DOM.")
            return []

        page_props = payload.get('props', {}).get('pageProps', {})
        teams = page_props.get('teams') or page_props.get('initialData', {}).get('teams', [])
        return teams if isinstance(teams, list) else []

    def fetch_data(self) -> pd.DataFrame:
        """Execução da Malha Híbrida de Extração."""
        logger.info(f"[NET-FETCH] Alvo: {self.base_url}/teams")
        
        try:
            response = self.session.get(f"{self.base_url}/teams", timeout=20)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # ESTRATÉGIA A: Desestruturação de Hidratação Next.js
            teams = self._extract_teams_from_next_data(soup)
            if teams:
                logger.info("[VAL-OK] Bloco __NEXT_DATA__ interceptado.")
                return pd.DataFrame([{
                    'team_id': int(t.get('teamId') or t.get('id', 0)),
                    'team_name': t.get('teamName') or t.get('name'),
                    'team_abbreviation': (t.get('teamAbbr') or t.get('abbr') or "NBA").upper(),
                    'ortg': float(t.get('oRtg') or 0),
                    'drtg': float(t.get('dRtg') or 0),
                    'net_rating': float(t.get('netRtg') or 0),
                    'offense_rating': float(t.get('offense') or 0),
                    'defense_rating': float(t.get('defense') or 0),
                    'record_date': self.current_date,
                    'period': self.period_label,
                    'created_at': self.current_timestamp
                } for t in teams])

            # ESTRATÉGIA B: Fallback de Tabela DOM (Resiliência)
            logger.warning("[VAL-WARN] Hidratação falhou. Acionando Motor de Parsing DOM.")
            table = soup.find('table')
            if not table:
                logger.error("[VAL-ERR] Nenhuma estrutura tabular localizada.")
                return pd.DataFrame()

            rows = table.find_all('tr')[1:]
            teams_data = []
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 5: continue
                
                name = cols[1].get_text(strip=True)
                img = cols[1].find('img')
                t_id_match = re.search(r'/(\d+)\.', img['src']) if img else None
                t_id = int(t_id_match.group(1)) if t_id_match else 0

                teams_data.append({
                    'team_id': t_id,
                    'team_name': name,
                    'team_abbreviation': name[:3].upper(),
                    'ortg': self._clean_numeric(cols[2].text),
                    'drtg': self._clean_numeric(cols[3].text),
                    'net_rating': self._clean_numeric(cols[4].text),
                    'offense_rating': self._clean_numeric(cols[5].text) if len(cols) > 5 else 0.0,
                    'defense_rating': self._clean_numeric(cols[6].text) if len(cols) > 6 else 0.0,
                    'record_date': self.current_date,
                    'period': self.period_label,
                    'created_at': self.current_timestamp
                })
            
            return pd.DataFrame(teams_data)

        except Exception as e:
            logger.error(f"[FATAL] Colapso do motor de busca: {str(e)}")
            return pd.DataFrame()

    def run(self):
        """Orquestração de Fluxo e Persistência."""
        summary = {'status': 'FAILED', 'execution_date': self.current_timestamp}
        try:
            df = self.fetch_data()
            if df.empty:
                raise ValueError("Vetor de dados nulo após varredura completa.")
            
            # Sincronização Supabase (Upsert)
            records = df.to_dict('records')
            self.supabase.table('databallr_team_stats').upsert(
                records, on_conflict='team_id,record_date,period'
            ).execute()
            
            summary.update({
                'status': 'SUCCESS', 
                'teams_processed': len(df),
                'avg_net_rating': float(df['net_rating'].mean())
            })
            logger.info(f"[SYS-OP] Sincronia concluída: {len(df)} registros.")
            
        except Exception as e:
            summary['error'] = str(e)
            logger.error(f"[SYS-ERR] Interrupção crítica: {str(e)}")
            raise
        finally:
            with open("execution_summary.json", "w") as f:
                json.dump(summary, f, indent=2)

if __name__ == "__main__":
    DataballrScraper().run()
                      
