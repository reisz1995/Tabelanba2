#!/usr/bin/env python3
"""
Módulo Extrator NBA [Databallr -> Supabase]
Versão: 5.1 (Direct API Link + JSON Auto-Discovery)
Estética: Replicante / Architect-Engineer
"""

import os
import json
import logging
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from supabase import create_client, Client

# Configuração de Telemetria HUD
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class DataballrScraper:
    def __init__(self, period: str = "last14", season: str = "2025-26"):
        self.period = os.getenv("DATABALLR_PERIOD", period)
        
        db_period_map = {
            "last14": "last_14_days",
            "last30": "last_30_days",
            "full_season": "full_season",
        }
        self.db_period_label = db_period_map.get(self.period, "last_14_days")
        
        api_window_map = {
            "last14": "last_14_days", 
            "last30": "last_30_days",
            "full_season": "this_year"
        }
        self.api_date_window = api_window_map.get(self.period, "last_14_days")
        
        season_env = os.getenv("DATABALLR_SEASON", season)
        self.api_season = "20" + season_env.split('-')[1] if '-' in season_env else "2026"
        
        self.api_url = f"https://api.databallr.com/api/supabase/team_stats?season={self.api_season}&leverage=all&date_window={self.api_date_window}"
        
        now_utc = datetime.now(timezone.utc)
        self.current_date = now_utc.date().isoformat()
        self.current_timestamp = now_utc.isoformat()
        
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[403, 429, 500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Referer': 'https://databallr.com/',
            'Origin': 'https://databallr.com'
        })
        
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            raise EnvironmentError("[SYS-ERR] Credenciais Supabase não detectadas.")
        self.supabase: Client = create_client(url, key)

    def fetch_data(self) -> pd.DataFrame:
        """Motor Matemático: Extração via API + Heurística O(N)."""
        logger.info(f"[NET-FETCH] Link Direto Estabelecido: {self.api_url}")
        
        try:
            response = self.session.get(self.api_url, timeout=20)
            response.raise_for_status()
            payload = response.json()
            
            # --- ALGORITMO DE AUTO-DESCOBERTA (HEURÍSTICA) ---
            teams = []
            target_key = "UNKNOWN"
            for key, val in payload.items():
                if isinstance(val, list) and len(val) >= 28:
                    if isinstance(val[0], dict) and ('TeamId' in val[0] or 'TeamAbbreviation' in val[0]):
                        teams = val
                        target_key = key
                        break
            
            if not teams:
                logger.error(f"[VAL-ERR] Matriz nula. Chaves detectadas no payload: {list(payload.keys())}")
                return pd.DataFrame()

            logger.info(f"[SYS-OP] Matriz localizada sob a chave '{target_key}' com {len(teams)} vetores.")

            # Cálculo Base da Liga para Offense/Defense Rating
            league_avg = payload.get('opponent', {}).get('league_avg', {})
            league_pts = league_avg.get('Points', 0)
            league_poss = league_avg.get('OffPoss', 1)
            league_ortg = (league_pts / league_poss) * 100 if league_poss else 115.0
            
            teams_data = []
            for t in teams:
                off_poss = t.get('OffPoss', 1)
                def_poss = t.get('DefPoss', 1)
                pts = t.get('Points', 0)
                opp_pts = t.get('OpponentPoints', 0)
                
                ortg = (pts / off_poss) * 100 if off_poss else 0.0
                drtg = (opp_pts / def_poss) * 100 if def_poss else 0.0
                net_rating = ortg - drtg
                
                o_ts = t.get('TsPct', 0.0)
                o_tov = (t.get('Turnovers', 0) / off_poss) * 100 if off_poss else 0.0
                orb = t.get('OffFGReboundPct', 0.0)
                drb = t.get('DefFGReboundPct', 0.0)
                
                teams_data.append({
                    'team_id': int(t.get('TeamId', 0)),
                    'team_name': t.get('Name', 'Unknown'),
                    'team_abbreviation': t.get('TeamAbbreviation', 'NBA'),
                    'ortg': round(ortg, 1),
                    'drtg': round(drtg, 1),
                    'net_rating': round(net_rating, 1),
                    'offense_rating': round(ortg - league_ortg, 1),
                    'defense_rating': round(league_ortg - drtg, 1), 
                    'o_ts': round(o_ts * 100, 1),
                    'o_tov': round(o_tov, 1),
                    'orb': round(orb * 100, 1),
                    'drb': round(drb * 100, 1),
                    'record_date': self.current_date,
                    'period': self.db_period_label,
                    'created_at': self.current_timestamp
                })
            
            df = pd.DataFrame(teams_data)
            logger.info(f"[VAL-OK] {len(df)} vetores convertidos. Net Rating Médio: {df['net_rating'].mean():.2f}")
            return df

        except Exception as e:
            logger.error(f"[FATAL] Falha de processamento: {str(e)}")
            return pd.DataFrame()

    def run(self):
        """Pipeline Atômico de Persistência."""
        summary = {'status': 'FAILED', 'execution_date': self.current_timestamp}
        try:
            df = self.fetch_data()
            if df.empty:
                raise ValueError("Vetor de dados nulo. Heurística falhou em encontrar a matriz.")
            
            records = df.to_dict('records')
            self.supabase.table('databallr_team_stats').upsert(
                records, on_conflict='team_id,record_date,period'
            ).execute()
            
            summary.update({
                'status': 'SUCCESS', 
                'teams_processed': len(df),
                'api_endpoint': self.api_url,
                'avg_net_rating': round(float(df['net_rating'].mean()), 2)
            })
            logger.info(f"[SYS-OP] Sincronia concluída. Banco Supabase atualizado.")
            
        except Exception as e:
            summary['error'] = str(e)
            logger.error(f"[SYS-ERR] Interrupção crítica: {str(e)}")
            raise
        finally:
            with open("execution_summary.json", "w") as f:
                json.dump(summary, f, indent=2)

if __name__ == "__main__":
    DataballrScraper().run()
    
