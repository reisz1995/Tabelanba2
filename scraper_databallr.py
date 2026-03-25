#!/usr/bin/env python3
"""
Scraper para databallr.com - Estatísticas dos últimos 14 dias
Integração com Supabase para pipeline de dados NBA
"""

import os
import json
import requests
import pandas as pd
from datetime import datetime
from supabase import create_client, Client
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
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
            raise ValueError(f"Período inválido: {period}. Use um de: {', '.join(self.PERIOD_MAP.keys())}")
        self.period = period
        self.period_label = self.PERIOD_MAP[period]
        self.season = season
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://databallr.com/stats'
        })
        
        # Inicializar Supabase
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL e SUPABASE_KEY devem estar definidos nas variáveis de ambiente")
        
        self.supabase: Client = create_client(supabase_url, supabase_key)
        
    def fetch_team_stats_last_14_days(self) -> pd.DataFrame:
        """
        Busca estatísticas dos times dos últimos 14 dias do databallr.com
        """
        logger.info("Buscando estatísticas dos últimos 14 dias...")
        
        # Endpoint para stats dos últimos 14 dias (Last 2 weeks)
        url = f"{self.base_url}/api/team-stats"
        
        params = {
            'season': self.season,
            'period': self.period,
            'type': 'per100'     # Per 100 possessions
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Processar dados
            teams_data = []
            for team in data.get('teams', []):
                team_record = {
                    'team_id': team.get('teamId'),
                    'team_name': team.get('teamName'),
                    'team_abbreviation': team.get('teamAbbr'),
                    'ortg': team.get('oRtg'),
                    'drtg': team.get('dRtg'),
                    'net_rating': team.get('netRtg'),
                    'offense_rating': team.get('offense'),
                    'defense_rating': team.get('defense'),
                    'o_ts': team.get('oTS'),      # Offense from shooting
                    'o_tov': team.get('oTOV'),    # Offense from turnovers
                    'orb': team.get('oORB'),      # Offensive rebounding
                    'd_ts': team.get('dTS'),      # Defense from shooting
                    'd_tov': team.get('dTOV'),    # Defense from turnovers
                    'drb': team.get('dDRB'),      # Defensive rebounding
                    'net_eff': team.get('netEff'),
                    'net_poss': team.get('netPoss'),
                    'record_date': datetime.now().date().isoformat(),
                    'period': self.period_label,
                    'created_at': datetime.now().isoformat()
                }
                teams_data.append(team_record)
            
            df = pd.DataFrame(teams_data)
            logger.info(f"Dados obtidos para {len(df)} times")
            return df
            
        except requests.RequestException as e:
            logger.error(f"Erro ao buscar dados: {e}")
            # Retornar DataFrame vazio em caso de erro para não quebrar o pipeline
            return pd.DataFrame()
    
    def fetch_advanced_metrics(self) -> pd.DataFrame:
        """
        Busca métricas avançadas adicionais (Shot Profile, etc.)
        """
        logger.info("Buscando métricas avançadas...")
        
        url = f"{self.base_url}/api/team-advanced"
        
        params = {
            'season': self.season,
            'period': self.period
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            advanced_data = []
            for team in data.get('teams', []):
                record = {
                    'team_id': team.get('teamId'),
                    'team_abbreviation': team.get('teamAbbr'),
                    'rim_freq': team.get('rimFreq'),
                    'rim_fg_pct': team.get('rimFgPct'),
                    'mid_freq': team.get('midFreq'),
                    'mid_fg_pct': team.get('midFgPct'),
                    'three_freq': team.get('threeFreq'),
                    'three_pct': team.get('threePct'),
                    'def_rim_freq': team.get('defRimFreq'),
                    'def_rim_fg_pct': team.get('defRimFgPct'),
                    'team_ts_pct': team.get('teamTsPct'),
                    'opp_ts_pct': team.get('oppTsPct'),
                    'pace': team.get('pace'),
                    'record_date': datetime.now().date().isoformat(),
                    'period': self.period_label,
                    'created_at': datetime.now().isoformat()
                }
                advanced_data.append(record)
            
            return pd.DataFrame(advanced_data)
            
        except requests.RequestException as e:
            logger.error(f"Erro ao buscar métricas avançadas: {e}")
            return pd.DataFrame()
    
    def save_to_supabase(self, df: pd.DataFrame, table_name: str):
        """
        Salva DataFrame no Supabase
        """
        if df.empty:
            logger.warning(f"DataFrame vazio, nada para salvar em {table_name}")
            return
        
        logger.info(f"Salvando {len(df)} registros na tabela {table_name}...")
        
        # Converter DataFrame para lista de dicionários
        records = df.to_dict('records')
        
        # Upsert (inserir ou atualizar) baseado no team_id e record_date
        try:
            response = self.supabase.table(table_name).upsert(
                records,
                on_conflict='team_id,record_date,period'
            ).execute()
            
            logger.info(f"Dados salvos com sucesso: {len(response.data)} registros")
            
        except Exception as e:
            logger.error(f"Erro ao salvar no Supabase: {e}")
            raise
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validação de qualidade dos dados
        """
        if df.empty:
            logger.error("DataFrame está vazio!")
            return False
        
        required_columns = ['team_id', 'team_name', 'ortg', 'drtg', 'net_rating']
        missing_cols = [col for col in required_columns if col not in df.columns]
        
        if missing_cols:
            logger.error(f"Colunas obrigatórias faltando: {missing_cols}")
            return False
        
        # Verificar valores nulos críticos
        null_counts = df[['team_id', 'ortg', 'drtg']].isnull().sum()
        if null_counts.any():
            logger.warning(f"Valores nulos encontrados: {null_counts.to_dict()}")
        
        # Verificar range de ratings (geralmente entre 90-130)
        if not df['ortg'].between(80, 140).all():
            logger.warning("Valores de ORTG fora do range esperado")
        
        logger.info("Validação concluída com sucesso")
        return True
    
    def run(self):
        """
        Executa o pipeline completo
        """
        logger.info("=" * 50)
        logger.info(f"Iniciando scraper do Databallr - Período: {self.period_label}")
        logger.info("=" * 50)
        
        # 1. Buscar estatísticas básicas
        df_stats = self.fetch_team_stats_last_14_days()
        
        if not self.validate_data(df_stats):
            raise ValueError("Falha na validação dos dados")
        
        # 2. Salvar estatísticas básicas
        self.save_to_supabase(df_stats, 'databallr_team_stats')
        
        # 3. Buscar e salvar métricas avançadas
        df_advanced = self.fetch_advanced_metrics()
        if not df_advanced.empty:
            self.save_to_supabase(df_advanced, 'databallr_advanced_metrics')
        
        # 4. Criar resumo
        summary = {
            'execution_date': datetime.now().isoformat(),
            'teams_processed': len(df_stats),
            'period': self.period_label,
            'avg_ortg': df_stats['ortg'].mean() if not df_stats.empty else None,
            'avg_drtg': df_stats['drtg'].mean() if not df_stats.empty else None,
            'top_offense': df_stats.loc[df_stats['ortg'].idxmax(), 'team_name'] if not df_stats.empty else None,
            'top_defense': df_stats.loc[df_stats['drtg'].idxmin(), 'team_name'] if not df_stats.empty else None
        }
        
        # Salvar log de execução
        self.supabase.table('databallr_execution_logs').insert(summary).execute()
        
        logger.info("=" * 50)
        logger.info("Pipeline concluído com sucesso!")
        logger.info(f"Resumo: {json.dumps(summary, indent=2, default=str)}")
        logger.info("=" * 50)

        with open("execution_summary.json", "w", encoding="utf-8") as summary_file:
            json.dump(summary, summary_file, indent=2, default=str, ensure_ascii=False)
        logger.info("Arquivo execution_summary.json gerado com sucesso")
        
        return summary


def main():
    period = os.getenv("DATABALLR_PERIOD", "last14")
    season = os.getenv("DATABALLR_SEASON", "2025-26")
    scraper = DataballrScraper(period=period, season=season)
    scraper.run()


if __name__ == "__main__":
    main()
