#!/usr/bin/env python3
"""
Módulo Extrator [Databallr -> Supabase]
Especificação: Alta Performance, Tolerância a Falhas, Parsing Vetorizado.
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
        
        if period not in self.PERIOD_MAP:
            raise ValueError(f"[SYS-ERR] Período inválido: {period}.")
            
        self.period = period
        self.period_label = self.PERIOD_MAP[period]
        self.season = season
        
        # Ancoragem Temporal Determinística O(1)
        now_utc = datetime.now(timezone.utc)
        self.current_date = now_utc.date().isoformat()
        self.current_timestamp = now_utc.isoformat()
        
        # Configuração de Rede: Tolerância a Falhas (Retry Protocol)
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Referer': 'https://databallr.com/stats'
        })
        
        # Inicialização do Banco de Dados
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            raise EnvironmentError("[SYS-ERR] Credenciais Supabase ausentes no ambiente.")
            
        self.supabase: Client = create_client(supabase_url, supabase_key)

    def fetch_team_stats(self) -> pd.DataFrame:
        """Extração vetorial das estatísticas principais."""
        logger.info(f"[NET-FETCH] Iniciando varredura (Stats) - Período: {self.period_label}")
        url = f"{self.base_url}/api/team-stats"
        params = {'season': self.season, 'period': self.period, 'type': 'per100'}
        
        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            teams = response.json().get('teams', [])
            
            # List Comprehension para eficiência de CPU
            teams_data = [{
                'team_id': t.get('teamId'),
                'team_name': t.get('teamName'),
                'team_abbreviation': t.get('teamAbbr'),
                'ortg': t.get('oRtg'),
                'drtg': t.get('dRtg'),
                'net_rating': t.get('netRtg'),
                'offense_rating': t.get('offense'),
                'defense_rating': t.get('defense'),
                'o_ts': t.get('oTS'),
                'o_tov': t.get('oTOV'),
                'orb': t.get('oORB'),
                'd_ts': t.get('dTS'),
                'd_tov': t.get('dTOV'),
                'drb': t.get('dDRB'),
                'net_eff': t.get('netEff'),
                'net_poss': t.get('netPoss'),
                'record_date': self.current_date,
                'period': self.period_label,
                'created_at': self.current_timestamp
            } for t in teams]
            
            df = pd.DataFrame(teams_data)
            logger.info(f"[SYS-OP] Parse concluído: {len(df)} vetores processados.")
            return df
            
        except requests.RequestException as e:
            logger.error(f"[NET-ERR] Falha de conexão na malha de dados: {e}")
            return pd.DataFrame()

    def fetch_advanced_metrics(self) -> pd.DataFrame:
        """Extração vetorial das métricas avançadas (Shot Profile)."""
        logger.info("[NET-FETCH] Iniciando varredura (Advanced Metrics)")
        url = f"{self.base_url}/api/team-advanced"
        params = {'season': self.season, 'period': self.period}
        
        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            teams = response.json().get('teams', [])
            
            advanced_data = [{
                'team_id': t.get('teamId'),
                'team_abbreviation': t.get('teamAbbr'),
                'rim_freq': t.get('rimFreq'),
                'rim_fg_pct': t.get('rimFgPct'),
                'mid_freq': t.get('midFreq'),
                'mid_fg_pct': t.get('midFgPct'),
                'three_freq': t.get('threeFreq'),
                'three_pct': t.get('threePct'),
                'def_rim_freq': t.get('defRimFreq'),
                'def_rim_fg_pct': t.get('defRimFgPct'),
                'team_ts_pct': t.get('teamTsPct'),
                'opp_ts_pct': t.get('oppTsPct'),
                'pace': t.get('pace'),
                'record_date': self.current_date,
                'period': self.period_label,
                'created_at': self.current_timestamp
            } for t in teams]
            
            return pd.DataFrame(advanced_data)
            
        except requests.RequestException as e:
            logger.error(f"[NET-ERR] Falha na extração de métricas avançadas: {e}")
            return pd.DataFrame()

    def save_to_supabase(self, df: pd.DataFrame, table_name: str):
        """Injeção de dados no Supabase via Upsert."""
        if df.empty:
            logger.warning(f"[SYS-WARN] Matriz vazia. Ignorando injeção em {table_name}.")
            return
            
        logger.info(f"[DB-SYNC] Sincronizando {len(df)} registros -> {table_name}")
        records = df.to_dict('records')
        
        try:
            response = self.supabase.table(table_name).upsert(
                records,
                on_conflict='team_id,record_date,period'
            ).execute()
            logger.info(f"[DB-SYNC] Transação confirmada. {len(response.data)} linhas afetadas.")
        except Exception as e:
            logger.error(f"[DB-ERR] Falha na camada de persistência: {e}")
            raise

    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validação lógica matemática e integridade estrutural."""
        if df.empty:
            logger.error("[VAL-ERR] Matriz de dados vazia. Abortando pipeline.")
            return False
            
        required_columns = {'team_id', 'team_name', 'ortg', 'drtg', 'net_rating'}
        missing_cols = required_columns - set(df.columns)
        
        if missing_cols:
            logger.error(f"[VAL-ERR] Integridade estrutural corrompida. Faltam colunas: {missing_cols}")
            return False
            
        if df[['team_id', 'ortg', 'drtg']].isnull().any().any():
            logger.warning("[VAL-WARN] Detectada anomalia de nulidade em colunas críticas.")
            
        if not df['ortg'].between(70, 150).all():
            logger.warning("[VAL-WARN] Dispersão atípica: ORTG fora do limite estatístico (70-150).")
            
        logger.info("[VAL-OK] Validação estrutural aprovada.")
        return True

    def run(self):
        """Sequência principal de execução do pipeline."""
        logger.info("=" * 60)
        logger.info(f" INIT PIPELINE DATABALLR | ALVO: {self.period_label} | {self.current_timestamp}")
        logger.info("=" * 60)
        
        df_stats = self.fetch_team_stats()
        if not self.validate_data(df_stats):
            raise ValueError("[SYS-ERR] Condição de parada atingida na validação primária.")
            
        self.save_to_supabase(df_stats, 'databallr_team_stats')
        
        df_advanced = self.fetch_advanced_metrics()
        self.save_to_supabase(df_advanced, 'databallr_advanced_metrics')
        
        summary = {
            'execution_date': self.current_timestamp,
            'teams_processed': len(df_stats),
            'period': self.period_label,
            'avg_ortg': float(df_stats['ortg'].mean()) if not df_stats.empty else None,
            'avg_drtg': float(df_stats['drtg'].mean()) if not df_stats.empty else None,
            'top_offense': df_stats.loc[df_stats['ortg'].idxmax(), 'team_name'] if not df_stats.empty else None,
            'top_defense': df_stats.loc[df_stats['drtg'].idxmin(), 'team_name'] if not df_stats.empty else None,
            'status': 'SUCCESS'
        }
        
        self.supabase.table('databallr_execution_logs').insert(summary).execute()
        
        with open("execution_summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
            
        logger.info("[SYS-OP] Arquivo de telemetria 'execution_summary.json' exportado.")
        logger.info("=" * 60)
        logger.info(" PIPELINE ENCERRADO COM SUCESSO ")
        logger.info("=" * 60)
        return summary

def main():
    period = os.getenv("DATABALLR_PERIOD", "last14")
    season = os.getenv("DATABALLR_SEASON", "2025-26")
    scraper = DataballrScraper(period=period, season=season)
    scraper.run()

if __name__ == "__main__":
    main()
            
