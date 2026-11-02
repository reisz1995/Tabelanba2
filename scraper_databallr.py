#!/usr/bin/env python3
"""
Módulo Extrator NBA [Databallr -> Supabase -> Google AI Studio]
Versão: 5.6 (Multi-Period: last_14_days + this_year, Deep-Search, Key Normalization, Extended Tensors & Full Team Names)
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
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# ---------------------------------------------------------------------------
# Configuração de Telemetria HUD
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lookup: Abreviação -> Nome Completo Oficial (todos os 30 times da NBA)
# ---------------------------------------------------------------------------
TEAM_NAME_MAP: dict[str, str] = {
    'ATL': 'Atlanta Hawks',
    'BOS': 'Boston Celtics',
    'BKN': 'Brooklyn Nets',
    'CHA': 'Charlotte Hornets',
    'CHI': 'Chicago Bulls',
    'CLE': 'Cleveland Cavaliers',
    'DAL': 'Dallas Mavericks',
    'DEN': 'Denver Nuggets',
    'DET': 'Detroit Pistons',
    'GSW': 'Golden State Warriors',
    'HOU': 'Houston Rockets',
    'IND': 'Indiana Pacers',
    'LAC': 'Los Angeles Clippers',
    'LAL': 'Los Angeles Lakers',
    'MEM': 'Memphis Grizzlies',
    'MIA': 'Miami Heat',
    'MIL': 'Milwaukee Bucks',
    'MIN': 'Minnesota Timberwolves',
    'NOP': 'New Orleans Pelicans',
    'NYK': 'New York Knicks',
    'OKC': 'Oklahoma City Thunder',
    'ORL': 'Orlando Magic',
    'PHI': 'Philadelphia 76ers',
    'PHX': 'Phoenix Suns',
    'POR': 'Portland Trail Blazers',
    'SAC': 'Sacramento Kings',
    'SAS': 'San Antonio Spurs',
    'TOR': 'Toronto Raptors',
    'UTA': 'Utah Jazz',
    'WAS': 'Washington Wizards',
}


class DataballrScraper:
    def __init__(self, season: str = "2025-26", periods: list[str] = None):
        season_env = os.getenv("DATABALLR_SEASON", season)
        self.api_season = "20" + season_env.split('-')[1] if '-' in season_env else "2026"

        # [MULTI-PERÍODO] Garante suporte a last_14_days (usado no predict_games e testes)
        # e this_year (visão acumulada da temporada).
        env_period = os.getenv("DATABALLR_PERIOD")
        if periods:
            self.periods = periods
        elif env_period:
            self.periods = [p.strip() for p in env_period.split(",") if p.strip()]
        else:
            self.periods = ["last_14_days", "this_year"]

        now_utc = datetime.now(timezone.utc)
        self.current_date = now_utc.date().isoformat()
        self.current_timestamp = now_utc.isoformat()

        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[403, 429, 500, 502, 503, 504]
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/122.0.0.0 Safari/537.36'
            ),
            'Accept': 'application/json',
            'Referer': 'https://databallr.com/',
            'Origin': 'https://databallr.com',
        })

        url = os.environ.get("SUPABASE_URL")
        key = (
            os.environ.get("SUPABASE_SERVICE_KEY")
            or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
            or os.environ.get("SUPABASE_KEY")
        )

        if not url or not key:
            raise EnvironmentError("[SYS-ERR] Credenciais Supabase não detectadas.")
        self.supabase: Client = create_client(url, key)

    # -----------------------------------------------------------------------
    # Utilitário público: resolve nome completo a partir da abreviação
    # -----------------------------------------------------------------------
    @staticmethod
    def resolve_team_name(abbr: str, fallback: str = "Unknown") -> str:
        """Retorna o nome completo oficial do time pela abreviação (ex.: 'GSW' -> 'Golden State Warriors').
        Se a abreviação não estiver no mapa, retorna o fallback fornecido.
        """
        return TEAM_NAME_MAP.get(abbr.upper(), fallback)

    # -----------------------------------------------------------------------
    # Motor de extração por período
    # -----------------------------------------------------------------------
    def fetch_data_for_period(self, period_label: str) -> pd.DataFrame:
        """Motor Matemático: Extração via API + Heurística de Profundidade + Normalização."""
        api_url = (
            f"https://api.databallr.com/api/supabase/team_stats"
            f"?season={self.api_season}&leverage=all&date_window={period_label}"
        )
        logger.info(f"[NET-FETCH] Link Direto ({period_label}): {api_url}")

        try:
            response = self.session.get(api_url, timeout=20)
            response.raise_for_status()
            payload = response.json()

            # --- ALGORITMO DE AUTO-DESCOBERTA AVANÇADO ---
            teams_raw = []
            target_key = "UNKNOWN"

            # Varredura O(N) em Profundidade 0 e 1
            for k, v in payload.items():
                if isinstance(v, list) and len(v) >= 28:
                    teams_raw = v
                    target_key = k
                    break
                elif isinstance(v, dict):
                    for sub_k, sub_v in v.items():
                        if isinstance(sub_v, list) and len(sub_v) >= 28:
                            teams_raw = sub_v
                            target_key = f"{k}->{sub_k}"
                            break

            if not teams_raw:
                debug_info = {k: type(v).__name__ for k, v in payload.items()}
                logger.error(f"[VAL-ERR] Matriz nula para {period_label}. Estrutura: {debug_info}")
                return pd.DataFrame()

            logger.info(
                f"[SYS-OP] Matriz '{period_label}' localizada no nó '{target_key}' com {len(teams_raw)} vetores."
            )

            # Função de normalização de tensores (Ignora Case e Underscores)
            def normalize(d: dict) -> dict:
                return {str(k).lower().replace('_', ''): v for k, v in d.items()}

            league_avg_raw = (
                payload.get('opponent', {}).get('league_avg', {})
                or payload.get('league_avg', {})
            )
            league_avg = normalize(league_avg_raw)
            league_pts = league_avg.get('points', 0)
            league_poss = league_avg.get('offposs', 1)
            league_ortg = (
                (league_pts / league_poss) * 100
                if league_pts > 0 and league_poss > 0
                else 115.0
            )

            teams_data = []
            for t_raw in teams_raw:
                t = normalize(t_raw)

                # Extração Flexível
                off_poss = t.get('offposs', 1)
                def_poss = t.get('defposs', 1)
                pts      = t.get('points', 0)
                opp_pts  = t.get('opponentpoints') or t.get('opppoints', 0)

                # [DERIVAÇÃO DE TENSORES]
                net_poss = off_poss - def_poss

                ortg       = (pts     / off_poss) * 100 if off_poss else 0.0
                drtg       = (opp_pts / def_poss) * 100 if def_poss else 0.0
                net_rating = ortg - drtg
                net_eff    = ortg - drtg

                o_ts  = t.get('tspct', 0.0)
                o_tov = (t.get('turnovers', 0) / off_poss) * 100 if off_poss else 0.0
                orb   = t.get('offfgreboundpct') or t.get('orbpct', 0.0)
                drb   = t.get('deffgreboundpct') or t.get('drbpct', 0.0)

                team_id   = t.get('teamid') or t.get('id', 0)
                team_abbr = (
                    t.get('teamabbreviation')
                    or t.get('abbr')
                    or t.get('shortname', 'NBA')
                )

                api_name  = t.get('name') or t.get('teamname', 'Unknown')
                team_name = TEAM_NAME_MAP.get(str(team_abbr).upper(), api_name)

                teams_data.append({
                    'team_id':           int(team_id),
                    'team_name':         team_name,
                    'team_abbreviation': team_abbr,
                    'ortg':              round(ortg, 1),
                    'drtg':              round(drtg, 1),
                    'net_rating':        round(net_rating, 1),
                    'net_poss':          round(net_poss, 1),
                    'net_eff':           round(net_eff, 1),
                    'offense_rating':    round(ortg - league_ortg, 1),
                    'defense_rating':    round(league_ortg - drtg, 1),
                    'o_ts':              round(o_ts * 100, 1),
                    'o_tov':             round(o_tov, 1),
                    'orb':               round(orb * 100, 1),
                    'drb':               round(drb * 100, 1),
                    'record_date':       self.current_date,
                    'period':            period_label,
                    'created_at':        self.current_timestamp,
                })

            df = pd.DataFrame(teams_data)
            logger.info(
                f"[VAL-OK] {period_label}: {len(df)} vetores processados. "
                f"Net Rating Médio: {df['net_rating'].mean():.2f}"
            )
            return df

        except Exception as e:
            logger.error(f"[FATAL] Falha de processamento para {period_label}: {str(e)}")
            return pd.DataFrame()

    # -----------------------------------------------------------------------
    # Pipeline de persistência
    # -----------------------------------------------------------------------
    def run(self):
        """Pipeline Atômico de Persistência com suporte multi-período."""
        summary = {
            'status': 'FAILED',
            'execution_date': self.current_timestamp,
            'periods_processed': {}
        }
        total_records = 0

        try:
            for period in self.periods:
                logger.info(f"🚀 Iniciando ciclo para período: '{period}'...")
                df = self.fetch_data_for_period(period)

                if df.empty:
                    logger.warning(f"⚠️ Vetor nulo para período '{period}'. Pulando...")
                    continue

                records = df.to_dict('records')

                # [OTIMIZAÇÃO] Resolução de conflitos baseada em (team_id, period)
                self.supabase.table('databallr_team_stats').upsert(
                    records, on_conflict='team_id,period'
                ).execute()

                summary['periods_processed'][period] = {
                    'teams_processed': len(df),
                    'avg_net_rating': round(float(df['net_rating'].mean()), 2)
                }
                total_records += len(df)
                logger.info(f"[SYS-OP] Período '{period}' sincronizado ({len(df)} equipes).")

            if total_records == 0:
                raise ValueError("Nenhum registro extraído em nenhum dos períodos.")

            summary['status'] = 'SUCCESS'
            summary['total_records'] = total_records
            logger.info(f"[SYS-OP] Sincronia concluída. {total_records} registros totais atualizados.")

        except Exception as e:
            summary['error'] = str(e)
            logger.error(f"[SYS-ERR] Interrupção crítica: {str(e)}")
            raise
        finally:
            with open("execution_summary.json", "w") as f:
                json.dump(summary, f, indent=2)


if __name__ == "__main__":
    DataballrScraper().run()
