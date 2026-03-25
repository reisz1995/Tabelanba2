#!/usr/bin/env python3
"""
Módulo Extrator V2 [Databallr -> Supabase]
Especificação: Web Scraping de HTML (Fallback de API), Alta Performance.
"""

import os
import re
import json
import logging
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from bs4 import BeautifulSoup
from supabase import create_client, Client

# Configuração de Telemetria (HUD)
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
        self.period = period
        self.period_label = self.PERIOD_MAP.get(period, "last_14_days")
        self.season = season
        
        now_utc = datetime.now(timezone.utc)
        self.current_date = now_utc.date().isoformat()
        self.current_timestamp = now_utc.isoformat()
        
        # Configuração de Rede: Simulação de Navegador Real
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Referer': f"{self.base_url}/stats",
            'X-Requested-With': 'XMLHttpRequest'
        })
        
        # Injeção de Segredos
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            raise EnvironmentError("[SYS-ERR] Credenciais Supabase ausentes.")
        self.supabase: Client = create_client(url, key)

    def _clean_numeric(self, text: str) -> float:
        """Limpa ruídos de texto (rankings, sinais, quebras) para conversão float."""
        if not text or text.strip() == "-": return 0.0
        # Remove rankings (#1), sinais de mais e espaços
        clean = re.sub(r'#\d+|[+% \n\r]', '', text)
        try:
            return float(clean)
        except ValueError:
            return 0.0

    def fetch_team_stats(self) -> pd.DataFrame:
        """Extração via Parsing DOM da tabela principal."""
        logger.info(f"[NET-FETCH] Raspagem de HTML em curso: {self.base_url}/stats")
        
        try:
            response = self.session.get(f"{self.base_url}/stats", timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            table = soup.find('table')
            
            if not table:
                logger.error("[VAL-ERR] Tabela não localizada no DOM.")
                return pd.DataFrame()

            rows = table.find_all('tr')[1:] # Skip Header
            teams_data = []

            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 5: continue
                
                # Extração de Metadados do Time
                team_cell = cols[1]
                img_tag = team_cell.find('img')
                # Tenta capturar o ID do time via URL da logo (padrão NBA ID)
                team_id_match = re.search(r'/(\d+)\.', img_tag['src']) if img_tag else None
                team_id = int(team_id_match.group(1)) if team_id_match else 0
                
                name = team_cell.get_text(strip=True)
                
                teams_data.append({
                    'team_id': team_id,
                    'team_name': name,
                    'team_abbreviation': name[:3].upper(), # Fallback de Abreviação
                    'ortg': self._clean_numeric(cols[2].text),
                    'drtg': self._clean_numeric(cols[3].text),
                    'net_rating': self._clean_numeric(cols[4].text),
                    'offense_rating': self._clean_numeric(cols[5].text) if len(cols) > 5 else 0.0,
                    'defense_rating': self._clean_numeric(cols[6].text) if len(cols) > 6 else 0.0,
                    'record_date': self.current_date,
                    'period': self.period_label,
                    'created_at': self.current_timestamp
                })

            df = pd.DataFrame(teams_data)
            logger.info(f"[SYS-OP] Matriz gerada: {len(df)} vetores.")
            return df
            
        except Exception as e:
            logger.error(f"[NET-ERR] Falha crítica no parsing: {str(e)}")
            return pd.DataFrame()

    def save_to_supabase(self, df: pd.DataFrame, table_name: str):
        """Persistência via Upsert atômico."""
        if df.empty: return
        records = df.to_dict('records')
        try:
            self.supabase.table(table_name).upsert(
                records, on_conflict='team_id,record_date,period'
            ).execute()
            logger.info(f"[DB-SYNC] {table_name}: OK.")
        except Exception as e:
            logger.error(f"[DB-ERR] Falha na persistência: {e}")

    def run(self):
        df_stats = self.fetch_team_stats()
        if df_stats.empty:
            raise ValueError("[SYS-STOP] Abortando: Dados insuficientes.")
            
        self.save_to_supabase(df_stats, 'databallr_team_stats')
        
        summary = {
            'execution_date': self.current_timestamp,
            'teams_processed': len(df_stats),
            'status': 'SUCCESS'
        }
        
        with open("execution_summary.json", "w") as f:
            json.dump(summary, f)
            
        return summary

if __name__ == "__main__":
    DataballrScraper().run()
        
