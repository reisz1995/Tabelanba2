#!/usr/bin/env python3
"""
Módulo Extrator NBA [Databallr -> Supabase -> Google AI Studio]
Versão: 6.1 (Season-to-Date, Temporada Auto-Derivada, Bloco 'team' Explícito, Six Factors)
Estética: Replicante / Architect-Engineer

[CORREÇÕES 6.0]
1. Temporada: derivada automaticamente da data (ago-dez -> temporada iniciada no ano corrente;
   jan-jul -> temporada iniciada no ano anterior). Ex.: nov/2026 -> 2026-27 -> api_season=2027.
   O parâmetro antigo 'season=2026' retornava a temporada 2025-26 COMPLETA (encerrada),
   o que fazia o banco guardar dados da temporada anterior rotulados como atuais.
2. Bloco de dados: a API retorna {team: {..., team_data}, opponent: {..., team_data}}.
   A auto-descoberta antiga selecionava 'opponent->team_data' (stat_type='opponent',
   estatísticas DOS ADVERSÁRIOS), invertendo ORtg/DRTg. Agora o bloco 'team' é explícito.
3. date_window: comprovadamente IGNORADO pela API (testados last_14_days, last_14_games,
   last_30_days, this_year, etc. — todos retornam o mesmo agregado). A parametrização
   multi-período foi removida: os dados são o agregado season-to-date da temporada atual,
   armazenados sob period='this_year'.
4. Endpoint: /v1/nba/teams/stats, o mesmo usado pelo frontend do site.

[ADIÇÕES 6.1 — SIX FACTORS]
5. Os Six Factors do site (oTS, oTOV, oORB, dTS, dTOV, dDRB, NetEff, NetPoss) são
   calculados no CLIENTE, a partir do mesmo payload da API. O scraper agora replica
   fielmente o algoritmo do chunk 'teamStatsCalculations' (funções m()/Gc()),
   validado contra os 30 times do site com erro máximo de 0,05 (arredondamento).
   Colunas novas: o_ts_impact, o_tov_impact, o_orb_impact, d_ts_impact,
   d_tov_impact, d_drb_impact (migração: migrations/20261111_databallr_six_factors.sql).
6. Semântica de net_eff/net_poss alinhada ao site:
     net_eff  = oTS + dTS  (impacto líquido da eficiência de arremessos)
     net_poss = oTOV + oORB + dTOV + dDRB (impacto líquido de posses/rebotes)
   (antes: net_eff = ORTG-DRTG e net_poss = OffPoss-DefPoss; o rating líquido
   continua disponível na coluna net_rating.)
   As colunas cruas (o_ts = TS% x100, o_tov = TOV/100 posses, orb, drb) são mantidas.
7. Fallback de upsert: se as colunas de impacto ainda não existirem no Supabase,
   os registros base são gravados sem os impactos, com warning no log.
"""

import os
import json
import logging
from datetime import datetime, timedelta, timezone
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
# ---------------------------------------------------------------------------
# Six Factors: colunas de impacto e réplica do algoritmo do Databallr
# ---------------------------------------------------------------------------
SIX_FACTOR_COLUMNS = (
    'o_ts_impact', 'o_tov_impact', 'o_orb_impact',
    'd_ts_impact', 'd_tov_impact', 'd_drb_impact',
)


def six_factor_decompose(
    fc_team: float, ts_team: float, sc_team: float, tov_nc_team: float,
    fc_lg: float, ts_lg: float, sc_lg: float, tov_nc_lg: float,
    rtg_diff: float,
) -> tuple[float, float, float]:
    """Réplica fiel da função 'm()' do chunk 'teamStatsCalculations' do Databallr.

    Decompõe a diferença de ORTG (rtg_diff, em pontos por 100 posses) em três
    componentes expressos em pontos por 100 posses:

      - TS  : impacto da eficiência de arremessos (pontos de primeira chance
              ganhos/perdidos vs. a liga, via True Shooting);
      - TOV : impacto do controle de turnovers (pontos de primeira chance);
      - ORB : impacto dos rebotes ofensivos (diferença de pontos de segunda
              chance + resíduo do ORTG não explicado pelos dois primeiros).

    Parâmetros (todos derivados de um bloco de dados + seu league_avg):
      fc_team/ts_team/sc_team/tov_nc_team : FirstChancePoints/OffPoss*100,
        TsPct (fração), SecondChancePoints/OffPoss*100 e
        (Turnovers-SecondChanceTurnovers)/OffPoss do time;
      fc_lg/ts_lg/sc_lg/tov_nc_lg : os mesmos para o league_avg;
      rtg_diff : rating do time - rating da liga (pontos por 100 posses).

    Retorna (ts_component, tov_component, orb_component).
    """
    ts_diff = ts_team - ts_lg
    tov_diff = tov_nc_team - tov_nc_lg
    fc_diff = fc_team - fc_lg
    sc_diff = sc_team - sc_lg

    # Alocações brutas (F e R no original); guardas d>0 e y<1 replicadas.
    f_raw = (ts_diff * fc_lg / ts_lg) if ts_lg > 0 else 0.0
    e_raw = (-tov_diff * fc_lg / (1 - tov_nc_lg)) if tov_nc_lg < 1 else 0.0

    if (f_raw > 0 and e_raw < 0) or (f_raw < 0 and e_raw > 0):
        # Sinais opostos: TOV integral e TS absorve o restante da 1a chance.
        e = e_raw
        f = fc_diff - e
    else:
        q = f_raw + e_raw
        if abs(q) > 1e-3:
            h = fc_diff / q
            f = f_raw * h
            e = e_raw * h
        else:
            f = fc_diff
            e = 0.0

    t = sc_diff
    residual = rtg_diff - (f + e + t)
    if abs(residual) > 0.01 and abs(f) + abs(e) + abs(t) > 0:
        return f, e, t + residual
    return f, e, t


# ---------------------------------------------------------------------------
# Resolução de temporada (auto-derivada da data)
# ---------------------------------------------------------------------------
def resolve_current_season() -> str:
    """Retorna o identificador da temporada em curso no formato 'AAAA-BB'.

    Agosto a dezembro usa o ano corrente como início da temporada; janeiro a
    julho usa o ano anterior. Ex.: nov/2026 -> '2026-27'.
    """
    now = datetime.now(timezone.utc)
    start_year = now.year if now.month >= 8 else now.year - 1
    return f"{start_year}-{str((start_year + 1) % 100).zfill(2)}"


def season_label_to_api_season(season_env: str) -> str:
    """Converte '2026-27' (ou '2027') em o ano de encerramento usado pela API ('2027')."""
    season_env = str(season_env).strip()
    if season_env.isdigit():
        return season_env
    if '-' in season_env:
        return "20" + season_env.split('-')[1]
    raise ValueError(f"[SYS-ERR] Temporada inválida: '{season_env}'")


class DataballrScraper:
    def __init__(self, season: str = None):
        # [TEMPORADA] Derivação automática; DATABALLR_SEASON sobrepõe se definido.
        env_season = os.getenv("DATABALLR_SEASON")
        self.season = season or env_season or resolve_current_season()
        self.api_season = season_label_to_api_season(self.season)

        # [PERÍODO ÚNICO] A API só entrega o agregado season-to-date.
        # Não aceitar rótulos de janelas curtas: a API ignora date_window e
        # retornaria o agregado completo da temporada sob um rótulo incorreto.
        self.period = "this_year"

        now_utc = datetime.now(timezone.utc)
        self.current_date = now_utc.date().isoformat()
        self.current_timestamp = now_utc.isoformat()
        self.source_updated_at = None

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
    # Seleção do bloco de dados de time (não dos adversários)
    # -----------------------------------------------------------------------
    def _select_team_block(self, payload: dict):
        """Isola a matriz de estatísticas do próprio time.

        Prioridade explícita:
          1. Nós com stat_type == 'team' (ex.: payload['team']['team_data']).
        Fallback de varredura (profundidade 0 e 1) caso a estrutura mude,
        sempre preferindo uma chave cujo nome NÃO seja 'opponent'.
        """
        def is_matrix(v) -> bool:
            return isinstance(v, list) and len(v) >= 28

        # 1) Caminho explícito /v1: {team: {stat_type, team_data}, opponent: {...}}
        for node_key in ('team', 'teams', 'data'):
            node = payload.get(node_key)
            if isinstance(node, dict) and is_matrix(node.get('team_data')):
                if str(node.get('stat_type', 'team')).lower() == 'team':
                    return node['team_data'], f"{node_key}->team_data"

        # 2) Formato antigo de lista no nível raiz
        for k, v in payload.items():
            if is_matrix(v):
                return v, k

        # 3) Fallback varredura O(N): nunca aceitar stat_type='opponent' primeiro
        candidates = []
        for k, v in payload.items():
            if isinstance(v, dict):
                for sub_k, sub_v in v.items():
                    if is_matrix(sub_v):
                        stat_type = str(v.get('stat_type', sub_k)).lower()
                        if stat_type == 'opponent' or 'opponent' in k.lower():
                            continue
                        candidates.append((
                            0 if stat_type == 'team' else 1,
                            0 if 'opponent' not in k.lower() else 1,
                            k, sub_k, sub_v
                        ))
        if candidates:
            candidates.sort(key=lambda c: (c[0], c[1]))
            _, _, k, sub_k, sub_v = candidates[0]
            return sub_v, f"{k}->{sub_k}"

        return None, None

    # -----------------------------------------------------------------------
    # Motor de extração (season-to-date)
    # -----------------------------------------------------------------------
    def fetch_data(self) -> pd.DataFrame:
        """Motor Matemático: Extração via API + Normalização."""
        period_label = self.period
        api_url = (
            f"https://api.databallr.com/v1/nba/teams/stats"
            f"?season={self.api_season}&leverage=all"
        )
        logger.info(f"[NET-FETCH] Link Direto ({period_label} | season={self.season}): {api_url}")

        try:
            response = self.session.get(api_url, timeout=20)
            response.raise_for_status()
            payload = response.json()
            team_block = payload.get('team')
            if isinstance(team_block, dict):
                self.source_updated_at = team_block.get('updated_at')

            teams_raw, target_key = self._select_team_block(payload)

            if not teams_raw:
                debug_info = {k: type(v).__name__ for k, v in payload.items()}
                logger.error(f"[VAL-ERR] Matriz nula. Estrutura: {debug_info}")
                return pd.DataFrame()

            logger.info(
                f"[SYS-OP] Matriz season-to-date localizada no nó '{target_key}' "
                f"com {len(teams_raw)} vetores."
            )

            # Função de normalização de tensores (Ignora Case e Underscores)
            def normalize(d: dict) -> dict:
                return {str(k).lower().replace('_', ''): v for k, v in d.items()}

            # Bloco opponent: necessário para os fatores defensivos (Six Factors).
            opp_block = payload.get('opponent')
            opps_raw = (opp_block or {}).get('team_data') or []
            opp_league_avg = normalize(
                (opp_block or {}).get('league_avg') or {}
            )
            opp_map = {}
            for o_raw in opps_raw:
                o = normalize(o_raw)
                o_id = o.get('teamid') or o.get('id')
                if o_id is not None:
                    opp_map[int(o_id)] = o

            # Baselines da liga (bloco 'team' para o lado ofensivo e bloco
            # 'opponent' para o lado defensivo — como no site).
            block = (
                payload.get('team')
                or payload.get('teams')
                or payload
            )
            league_avg = normalize(
                block.get('league_avg') if isinstance(block, dict) else {}
            )
            lg_off_poss = league_avg.get('offposs', 0)
            lg_ortg = (
                (league_avg.get('points', 0) / lg_off_poss) * 100
                if lg_off_poss > 0
                else 115.0
            )
            lg_ts = league_avg.get('tspct', 0.0)
            lg_fc100 = (league_avg.get('firstchancepoints', 0) / lg_off_poss) * 100 if lg_off_poss else 0.0
            lg_sc100 = (league_avg.get('secondchancepoints', 0) / lg_off_poss) * 100 if lg_off_poss else 0.0
            lg_tov_nc = (
                (league_avg.get('turnovers', 0) - league_avg.get('secondchanceturnovers', 0)) / lg_off_poss
                if lg_off_poss else 0.0
            )

            lgo_off_poss = opp_league_avg.get('offposs', 0)
            lgo_ts = opp_league_avg.get('tspct', 0.0)
            lgo_fc100 = (opp_league_avg.get('firstchancepoints', 0) / lgo_off_poss) * 100 if lgo_off_poss else 0.0
            lgo_sc100 = (opp_league_avg.get('secondchancepoints', 0) / lgo_off_poss) * 100 if lgo_off_poss else 0.0
            lgo_tov_nc = (
                (opp_league_avg.get('turnovers', 0) - opp_league_avg.get('secondchanceturnovers', 0)) / lgo_off_poss
                if lgo_off_poss else 0.0
            )
            six_factors_available = bool(opp_map) and lg_ts > 0 and lgo_ts > 0
            if not six_factors_available:
                logger.warning(
                    "[SIX-WARN] Bloco 'opponent' ou baselines da liga ausentes; "
                    "impactos Six Factors serão gravados como 0."
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
                ortg       = (pts     / off_poss) * 100 if off_poss else 0.0
                drtg       = (opp_pts / def_poss) * 100 if def_poss else 0.0
                net_rating = ortg - drtg

                o_ts  = t.get('tspct', 0.0)
                o_tov = (t.get('turnovers', 0) / off_poss) * 100 if off_poss else 0.0
                orb   = t.get('offfgreboundpct') or t.get('orbpct', 0.0)
                drb   = t.get('deffgreboundpct') or t.get('drbpct', 0.0)

                # [SIX FACTORS] Réplica do cálculo client-side do Databallr
                # (Gc(): ofensa usa baselines do bloco 'team'; defesa usa a
                # entrada do MESMO TeamId no bloco 'opponent' com baselines do
                # league_avg desse bloco).
                o_ts_impact = o_tov_impact = o_orb_impact = 0.0
                d_ts_impact = d_tov_impact = d_drb_impact = 0.0
                # Fallback (sem bloco 'opponent'): net_eff vira o rating líquido
                # e net_poss fica em 0 — sem decomposição não há o que gravar.
                net_eff = ortg - drtg
                net_poss = 0.0

                team_id_val = t.get('teamid') or t.get('id', 0)
                opp_t = opp_map.get(int(team_id_val)) if six_factors_available else None

                if opp_t is not None:
                    opp_off_poss = opp_t.get('offposs', 1)
                    # DRTG pela fórmula do site: pontos do adversário / posses dele
                    drtg_site = (opp_t.get('points', 0) / opp_off_poss) * 100 if opp_off_poss else 0.0

                    # --- Ofensa (bloco 'team') ---
                    c_ts, c_tov, c_orb = six_factor_decompose(
                        fc_team=(t.get('firstchancepoints', 0) / off_poss) * 100,
                        ts_team=o_ts,
                        sc_team=(t.get('secondchancepoints', 0) / off_poss) * 100,
                        tov_nc_team=(t.get('turnovers', 0) - t.get('secondchanceturnovers', 0)) / off_poss,
                        fc_lg=lg_fc100, ts_lg=lg_ts, sc_lg=lg_sc100, tov_nc_lg=lg_tov_nc,
                        rtg_diff=ortg - lg_ortg,
                    )

                    # --- Defesa (entrada do mesmo TeamId no bloco 'opponent') ---
                    d_ts, d_tov, d_orb = six_factor_decompose(
                        fc_team=(opp_t.get('firstchancepoints', 0) / opp_off_poss) * 100,
                        ts_team=opp_t.get('tspct', 0.0),
                        sc_team=(opp_t.get('secondchancepoints', 0) / opp_off_poss) * 100,
                        tov_nc_team=(opp_t.get('turnovers', 0) - opp_t.get('secondchanceturnovers', 0)) / opp_off_poss,
                        fc_lg=lgo_fc100, ts_lg=lgo_ts, sc_lg=lgo_sc100, tov_nc_lg=lgo_tov_nc,
                        rtg_diff=drtg_site - lg_ortg,
                    )

                    # Display do site: defensivos com sinal invertido
                    # (positivo = boa defesa); netEff = C.ts - D.ts;
                    # netPoss = C.tov + C.orb - D.tov - D.orb (componentes crus).
                    o_ts_impact, o_tov_impact, o_orb_impact = c_ts, c_tov, c_orb
                    d_ts_impact, d_tov_impact, d_drb_impact = -d_ts, -d_tov, -d_orb
                    net_eff = c_ts - d_ts
                    net_poss = c_tov + c_orb - d_tov - d_orb

                team_id   = team_id_val
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
                    'offense_rating':    round(ortg - lg_ortg, 1),
                    'defense_rating':    round(lg_ortg - drtg, 1),
                    'o_ts':              round(o_ts * 100, 1),
                    'o_tov':             round(o_tov, 1),
                    'orb':               round(orb * 100, 1),
                    'drb':               round(drb * 100, 1),
                    'o_ts_impact':       round(o_ts_impact, 1),
                    'o_tov_impact':      round(o_tov_impact, 1),
                    'o_orb_impact':      round(o_orb_impact, 1),
                    'd_ts_impact':       round(d_ts_impact, 1),
                    'd_tov_impact':      round(d_tov_impact, 1),
                    'd_drb_impact':      round(d_drb_impact, 1),
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
    # Persistência com fallback (colunas Six Factors podem não existir ainda)
    # -----------------------------------------------------------------------
    def _upsert_records(self, records: list) -> str:
        """Upsert em (team_id, period); se as colunas de impacto não existirem,
        grava os registros base e avisa para rodar a migração SQL."""
        try:
            self.supabase.table('databallr_team_stats').upsert(
                records, on_conflict='team_id,period'
            ).execute()
            return 'full'
        except Exception as exc:
            msg = str(exc)
            if 'PGRST204' in msg or 'o_ts_impact' in msg or 'column' in msg.lower():
                trimmed = [
                    {k: v for k, v in r.items() if k not in SIX_FACTOR_COLUMNS}
                    for r in records
                ]
                self.supabase.table('databallr_team_stats').upsert(
                    trimmed, on_conflict='team_id,period'
                ).execute()
                logger.warning(
                    "[DB-WARN] Colunas Six Factors ausentes no Supabase; "
                    "registros gravados sem os impactos. Execute "
                    "migrations/20261111_databallr_six_factors.sql."
                )
                return 'partial'
            raise

    # -----------------------------------------------------------------------
    # Pipeline de persistência
    # -----------------------------------------------------------------------
    def run(self):
        """Pipeline Atômico de Persistência (season-to-date único)."""
        summary = {
            'status': 'FAILED',
            'execution_date': self.current_timestamp,
            'season': self.season,
            'api_season': self.api_season,
            'source_updated_at': None,
            'periods_processed': {}
        }
        total_records = 0

        try:
            logger.info(f"🚀 Iniciando ciclo '{self.period}' (temporada {self.season})...")
            df = self.fetch_data()

            if df.empty:
                raise ValueError("Nenhum registro extraído da API Databallr.")

            if not self.source_updated_at:
                raise ValueError("A API Databallr não informou updated_at; coleta cancelada por segurança.")

            source_updated = datetime.fromisoformat(
                self.source_updated_at.replace('Z', '+00:00')
            )
            if source_updated.tzinfo is None:
                source_updated = source_updated.replace(tzinfo=timezone.utc)
            source_age = datetime.now(timezone.utc) - source_updated
            summary['source_updated_at'] = self.source_updated_at
            summary['source_age_hours'] = round(source_age.total_seconds() / 3600, 2)
            logger.info(
                f"[SRC-AGE] Databallr atualizado em {self.source_updated_at}; "
                f"idade do snapshot: {summary['source_age_hours']}h."
            )
            if source_age > timedelta(hours=12):
                raise ValueError(
                    f"Snapshot Databallr está atrasado ({summary['source_age_hours']}h; "
                    "limite 12h). Dados não foram gravados no Supabase."
                )

            records = df.to_dict('records')

            # [OTIMIZAÇÃO] Resolução de conflitos baseada em (team_id, period)
            upsert_mode = self._upsert_records(records)

            summary['periods_processed'][self.period] = {
                'teams_processed': len(df),
                'avg_net_rating': round(float(df['net_rating'].mean()), 2),
                'upsert_mode': upsert_mode
            }
            total_records = len(df)
            logger.info(f"[SYS-OP] Período '{self.period}' sincronizado ({len(df)} equipes).")

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
