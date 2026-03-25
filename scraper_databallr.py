#!/usr/bin/env python3
"""
Módulo Extrator NBA [Databallr -> Supabase]
Versão: 3.0 (Next.js Hydration Engine)
Estética: Replicante / Architect-Engineer
"""

import os
import json
import logging
import re
from datetime import datetime, timezone
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
        self.period = period
        self.period_label = self.PERIOD_MAP.get(period, "last_14_days")
        self.season = season
        
        now_utc = datetime.now(timezone.utc)
        self.current_date = now_utc.date().isoformat()
        self.current_timestamp = now_utc.isoformat()
        
        # Configuração de Rede: Stealth Mode
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Referer': f"{self.base_url}/stats"
        })
        
        # Gatekeeper: Validação de Credenciais
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            raise EnvironmentError("[SYS-ERR] Credenciais Supabase não detectadas no ambiente.")
        self.supabase: Client = create_client(url, key)

    def fetch_team_stats(self) -> pd.DataFrame:
        """Extrai dados estruturados do bloco de hidratação do Next.js."""
        logger.info(f"[NET-FETCH] Alvo: {self.base_url}/stats | Motor: __NEXT_DATA__")
        
        try:
            response = self.session.get(f"{self.base_url}/stats", timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            script_tag = soup.find('script', id='__NEXT_DATA__')
            
            if not script_tag:
                logger.error("[VAL-ERR] Bloco de hidratação ausente. Possível bloqueio Cloudflare.")
                return pd.DataFrame()

            payload = json.loads(script_tag.string)
            
            # Navegação na árvore de propriedades do Next.js
            page_props = payload.get('props', {}).get('pageProps', {})
            # Tenta múltiplos caminhos de resolução de dados
            teams_list = page_props.get('teams') or page_props.get('initialData', {}).get('teams', [])

            if not teams_list:
                logger.warning("[VAL-WARN] Estrutura reconhecida, mas matriz de dados nula.")
                return pd.DataFrame()

            teams_data = [{
                'team_id': t.get('teamId') or t.get('id'),
                'team_name': t.get('teamName') or t.get('name'),
                'team_abbreviation': t.get('teamAbbr') or t.get('abbr'),
                'ortg': float(t.get('oRtg') or 0),
                'drtg': float(t.get('dRtg') or 0),
                'net_rating': float(t.get('netRtg') or 0),
                'offense_rating': float(t.get('offense') or 0),
                'defense_rating': float(t.get('defense') or 0),
                'record_date': self.current_date,
                'period': self.period_label,
                'created_at': self.current_timestamp
            } for t in teams_list]

            df = pd.DataFrame(teams_data)
            logger.info(f"[SYS-OP] Sucesso: {len(df)} vetores NBA extraídos.")
            return df
            
        except Exception as e:
            logger.error(f"[NET-ERR] Falha na desestruturação: {str(e)}")
            return pd.DataFrame()

    def save_to_supabase(self, df: pd.DataFrame, table_name: str):
        """Injeção atômica com resolução de conflito."""
        if df.empty: return
        records = df.to_dict('records')
        try:
            self.supabase.table(table_name).upsert(
                records, on_conflict='team_id,record_date,period'
            ).execute()
            logger.info(f"[DB-SYNC] Tabela {table_name} sincronizada.")
        except Exception as e:
            logger.error(f"[DB-ERR] Falha de persistência: {e}")

    def run(self):
        """Orquestração principal com telemetria final."""
        summary = {'status': 'FAILED', 'execution_date': self.current_timestamp}
        try:
            df_stats = self.fetch_team_stats()
            if df_stats.empty:
                raise ValueError("Fluxo interrompido: Matriz de dados vazia.")
            
            self.save_to_supabase(df_stats, 'databallr_team_stats')
            summary.update({
                'status': 'SUCCESS',
                'teams_processed': len(df_stats),
                'avg_net_rating': float(df_stats['net_rating'].mean())
            })
            logger.info("[SYS-OP] Pipeline concluído com integridade.")
        except Exception as e:
            logger.critical(f"[FATAL] {str(e)}")
            summary['error'] = str(e)
            raise
        finally:
            with open("execution_summary.json", "w") as f:
                json.dump(summary, f, indent=2)

if __name__ == "__main__":
    DataballrScraper().run()
    
