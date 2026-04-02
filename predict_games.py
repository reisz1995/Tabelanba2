import os
import sys
import json
import time
import requests
from datetime import datetime, timedelta
import pytz
from supabase import create_client
from groq import Groq

# ==========================================
# 1. INICIALIZAÇÃO DE INFRAESTRUTURA
# ==========================================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

if not all([SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, GROQ_API_KEY]):
    print("❌ COLAPSO_DE_SISTEMA: Faltam variáveis de ambiente.")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
groq_client = Groq(api_key=GROQ_API_KEY)

# ==========================================
# 2. MOTORES DE EXTRAÇÃO E LIMPEZA
# ==========================================

class InjuryMonitor:
    def __init__(self, filepath):
        self.injuries = []
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                self.injuries = json.load(f)

    def get_elite_injuries(self, team_name, min_rating=7.0):
        """
        FIX: Condição unificada em um único `if` para evitar
        duplicatas caso um jogador satisfaça múltiplos critérios.
        """
        elite_injuries = []
        for injury in self.injuries:
            if team_name not in injury.get('team_name', ''):
                continue
            player_rating = injury.get('player_rating', 0) or injury.get('rating', 0)
            is_numeric_elite = isinstance(player_rating, (int, float)) and player_rating >= min_rating
            is_flag_elite = (
                injury.get('is_star') or
                injury.get('all_star') or
                injury.get('impact') == 'high'
            )
            if is_numeric_elite or is_flag_elite:
                elite_injuries.append(injury)
        return elite_injuries


def extract_pure_json(raw_response: str) -> str:
    clean_text = raw_response.strip()
    if clean_text.startswith("```json"):
        clean_text = clean_text[7:]
    elif clean_text.startswith("```"):
        clean_text = clean_text[3:]
    if clean_text.endswith("```"):
        clean_text = clean_text[:-3]
    return clean_text.strip()


def with_retry(func, retries=3, base_delay=1.5):
    """
    FIX: Adicionado backoff exponencial e preservação da última
    exceção real para diagnóstico correto em caso de falha total.
    """
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return func()
        except Exception as e:
            last_exc = e
            if attempt < retries:
                wait = base_delay * (2 ** attempt)
                time.sleep(wait)
    raise last_exc


# ==========================================
# 3. INTERFACES DE DADOS (ESPN & SUPABASE)
# ==========================================

def get_espn_games(date_obj):
    base_date = date_obj.strftime('%Y%m%d')
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={base_date}"

    try:
        res = requests.get(url, timeout=10).json()
        events = res.get('events', [])

        if not events:
            next_day = (date_obj + timedelta(days=1)).strftime('%Y%m%d')
            print(f"⚠️ Vetor nulo detectado para {base_date}. Redirecionando radar para {next_day}...")
            url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={next_day}"
            res = requests.get(url, timeout=10).json()
            events = res.get('events', [])

        games = []
        for event in events:
            # FIX: Acesso defensivo a 'competitions' e 'competitors'
            competitions = event.get('competitions', [])
            if not competitions:
                continue
            comps = competitions[0].get('competitors', [])
            if not comps:
                continue

            home_team = next((c['team'] for c in comps if c.get('homeAway') == 'home'), None)
            away_team = next((c['team'] for c in comps if c.get('homeAway') == 'away'), None)

            if not home_team or not away_team:
                continue

            games.append({
                'id': event.get('id'),
                'date': event.get('date'),
                'home': home_team,
                'away': away_team
            })

        print(f"📡 Radar ESPN: {len(games)} confrontos detectados no espaço-tempo.")
        return games

    except Exception as e:
        print(f"❌ Colapso na interface ESPN: {e}")
        return []


def get_databallr_matrix():
    try:
        res = supabase.table("databallr_team_stats").select("*").eq("period", "last_14_days").execute()
        return {str(row.get("team_name")).lower(): row for row in res.data}
    except Exception as e:
        print(f"⚠️ Falha de conexão com a matriz Databallr: {e}")
        return {}


def match_databallr_stats(espn_team_name: str, databallr_matrix: dict) -> dict:
    """
    FIX: Log adicionado quando o time não é encontrado,
    evitando que defaults silenciosos mascarem times não mapeados.
    """
    espn_lower = espn_team_name.lower()

    if espn_lower in databallr_matrix:
        return databallr_matrix[espn_lower]

    for db_name, stats in databallr_matrix.items():
        if db_name in espn_lower or espn_lower in db_name:
            return stats

    print(f"⚠️ Time '{espn_team_name}' não encontrado na matriz Databallr. Usando defaults.")
    return {"ortg": 115.0, "drtg": 115.0, "net_eff": 0.0, "o_ts": 55.0, "orb": 25.0, "net_poss": 0}


def get_market_odds(home_full: str, away_full: str) -> dict:
    """
    FIX: Retorno padronizado como dict em todos os caminhos,
    evitando mistura de tipos (str vs dict) no payload JSON.
    Exception explícita com log.
    """
    try:
        res = supabase.table("nba_odds_matrix").select("*").execute()
        for row in res.data:
            if home_full in row.get("matchup", "") or away_full in row.get("matchup", ""):
                return row
    except Exception as e:
        print(f"⚠️ Odds indisponíveis: {e}")

    return {"status": "indisponível", "matchup": f"{home_full} vs {away_full}"}


def get_team_stats(team_id) -> dict:
    """
    FIX: URL corrigida — removidos artefatos de hyperlink Markdown
    que quebravam a requisição HTTP silenciosamente.
    """
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}"
        res = requests.get(url, timeout=10).json()
        team_data = res.get('team', {})

        standing = team_data.get('standingSummary', '')
        record = team_data.get('record', {})
        items = record.get('items', [])

        wins = 0
        losses = 0
        win_pct = 0.5
        streak = "0"

        if items:
            overall = items[0]
            stats = overall.get('stats', [])
            if isinstance(stats, list):
                for stat in stats:
                    stat_name = stat.get('name')
                    if stat_name == 'wins':
                        wins = int(stat.get('value', 0))
                    elif stat_name == 'losses':
                        losses = int(stat.get('value', 0))
                    elif stat_name == 'streak':
                        streak = stat.get('displayValue', '0')

        if (wins + losses) > 0:
            win_pct = wins / (wins + losses)

        return {
            'win_pct': win_pct,
            'wins': wins,
            'losses': losses,
            'streak': streak,
            'is_contender': win_pct >= 0.60,
            'is_weak': win_pct <= 0.40,
            'standing_summary': standing
        }
    except Exception:
        return {
            'win_pct': 0.5, 'wins': 0, 'losses': 0, 'streak': '0',
            'is_contender': False, 'is_weak': False, 'standing_summary': ''
        }


def get_team_defense_metrics(team_id) -> dict:
    """
    FIX: URL corrigida — removidos artefatos de hyperlink Markdown.
    """
    def normalize_metric_value(raw_value):
        if raw_value is None:
            return None
        if isinstance(raw_value, (int, float)):
            return float(raw_value)
        if isinstance(raw_value, str):
            cleaned = raw_value.strip().replace(",", ".")
            if not cleaned:
                return None
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

    def iter_stats_objects(payload):
        if isinstance(payload, dict):
            for key, value in payload.items():
                if key == "stats" and isinstance(value, list):
                    yield from value
                else:
                    yield from iter_stats_objects(value)
        elif isinstance(payload, list):
            for item in payload:
                yield from iter_stats_objects(item)

    def match_stat_name(stat):
        summary = " ".join([
            str(stat.get('name', '')).lower().strip(),
            str(stat.get('displayName', '')).lower().strip(),
            str(stat.get('shortDisplayName', '')).lower().strip()
        ])
        if "defensive" in summary and "rating" in summary:
            return "defensive_rating"
        if "pace" in summary:
            return "pace"
        if "points allowed" in summary or "opp points" in summary or "opponent points" in summary:
            return "points_allowed"
        return None

    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/statistics"
        res = requests.get(url, timeout=10)
        if res.status_code != 200:
            return {'defensive_rating': None, 'pace': None, 'points_allowed': None}

        data = res.json()
        defensive_rating, pace, points_allowed = None, None, None

        for stat in iter_stats_objects(data):
            metric_name = match_stat_name(stat)
            if not metric_name:
                continue
            metric_value = normalize_metric_value(stat.get('value', stat.get('displayValue')))

            if metric_name == "defensive_rating" and defensive_rating is None:
                defensive_rating = metric_value
            elif metric_name == "pace" and pace is None:
                pace = metric_value
            elif metric_name == "points_allowed" and points_allowed is None:
                points_allowed = metric_value

        return {'defensive_rating': defensive_rating, 'pace': pace, 'points_allowed': points_allowed}

    except Exception:
        return {'defensive_rating': None, 'pace': None, 'points_allowed': None}


def _parse_espn_date(date_str: str) -> datetime:
    """
    FIX: Parse robusto de datas ESPN com suporte a dois formatos
    (com e sem segundos), evitando crash silencioso em `extract_h2h`.
    """
    for fmt in ("%Y-%m-%dT%H:%MZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Formato de data não reconhecido: {date_str}")


def extract_h2h(team_id, opponent_id) -> list:
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/schedule"

    def fetch_schedule():
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        return res.json().get('events', [])

    try:
        events = with_retry(fetch_schedule, retries=3)
        if not events:
            return []

        past_games = [
            e for e in events
            if e.get('competitions', [{}])[0].get('status', {}).get('type', {}).get('state') == 'post'
        ]
        past_games.sort(key=lambda x: x.get('date', ''), reverse=True)

        h2h_raw = [
            g for g in past_games
            if any(
                c.get('id') == str(opponent_id)
                for c in g.get('competitions', [{}])[0].get('competitors', [])
            )
        ]

        parsed = []
        for g in h2h_raw[:3]:
            comp = g['competitions'][0]['competitors']
            main = next((c for c in comp if c.get('id') == str(team_id)), None)
            opp = next((c for c in comp if c.get('id') != str(team_id)), None)

            if not main or not opp:
                continue

            # FIX: Usando helper com suporte a múltiplos formatos de data
            try:
                dt = _parse_espn_date(g['date'])
            except ValueError:
                continue

            def get_score(c):
                s = c.get('score', 0)
                if isinstance(s, dict):
                    return int(s.get('value', 0))
                return int(s) if s else 0

            main_s = get_score(main)
            opp_s = get_score(opp)

            parsed.append({
                "date": dt.strftime("%d/%m"),
                "result": 'V' if main.get('winner') else 'D',
                "score": f"{max(main_s, opp_s)}-{min(main_s, opp_s)}"
            })

        return parsed

    except Exception:
        return []


def get_last_games(team_id, limit=5) -> dict:
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/schedule"

    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        events = res.json().get('events', [])

        past_games = [
            e for e in events
            if e.get('competitions', [{}])[0].get('status', {}).get('type', {}).get('state') == 'post'
        ]
        past_games.sort(key=lambda x: x.get('date', ''), reverse=True)

        last_games = []
        wins = 0
        losses = 0

        for g in past_games[:limit]:
            comp = g.get('competitions', [{}])[0]
            team_comp = next(
                (c for c in comp.get('competitors', []) if c.get('id') == str(team_id)),
                None
            )
            if team_comp:
                is_winner = team_comp.get('winner', False)
                if is_winner:
                    wins += 1
                else:
                    losses += 1

                last_games.append({
                    'result': 'V' if is_winner else 'D',
                    'date': g.get('date', '')[:10],
                    'home_away': 'CASA' if team_comp.get('homeAway') == 'home' else 'FORA'
                })

        total_games = wins + losses
        momentum_score = (wins / total_games) if total_games > 0 else 0.5

        return {
            'last_games': last_games,
            'wins_last_5': wins,
            'losses_last_5': losses,
            # FIX: round() é mais limpo e idiomático que float(f"{x:.3f}")
            'momentum_score': round(momentum_score, 3)
        }

    except Exception:
        return {
            'last_games': [],
            'wins_last_5': 0,
            'losses_last_5': 0,
            'momentum_score': 0.5
        }


# ==========================================
# 4. MOTOR PREDITIVO (GROQ IA)
# ==========================================

# FIX: Renomeada para GROQ_SYSTEM_PROMPT para deixar explícito
# que é uma constante imutável usada como prompt de sistema.
GROQ_SYSTEM_PROMPT = """Você é o Estatístico Chefe do sistema NBA-MONITOR. Calcule o Edge.

DIRETRIZES OBRIGATÓRIAS DE ANÁLISE:

1. MATEMÁTICA DE OVER/UNDER (DATABALLR 14D - PRIORIDADE MÁXIMA):
   - Utilize as métricas avançadas dos últimos 14 dias (ORTG, DRTG, NET_EFF, True Shooting).
   - Equação Base: Projete a pontuação cruzando o ORTG (Ataque) de um time contra o DRTG (Defesa) do outro, ajustado pelo Ritmo (Pace/Net Poss).
   - Defesa em Colapso = DRTG > 116.0. Ataque de Elite = ORTG > 117.0.
   - OVER RECOMENDADO apenas se ambos os times tiverem projeção matemática > 112 pontos cada e True Shooting (o_ts) > 57%.

2. IMPACTO DE ESTRELAS (ELITE ONLY):
   - Só considere impacto de lesão se o jogador for ESTRELA DE ELITE (nota >= 7.0 ou All-Star)

3. FATORES CASA E MOMENTUM:
   - Contenders (win% >= 60%) em casa: Vantagem massiva.
   - Use o NET_EFF (Eficiência Líquida) recente para validar se o momentum de V/D é real ou sorte.

4. HANDICAPS (REGRAS OBRIGATÓRIAS):
   - EVITE linhas exatas de +5.5. Prefira extremidades (+10 underdog claro, -5 favorito sólido).

SAÍDA OBRIGATÓRIA (JSON Estrito):
{
  "palpite_principal": "string (ex: OVER 225.5, Boston -5, Philadelphia +10)",
  "confianca": 0.0,
  "linha_seguranca_over": "string",
  "linha_seguranca_under": "string",
  "handicap_recomendado": "string",
  "alerta_lesao": "string",
  "keyFactor": "string (ex: ORTG vs DRTG cruzado indica Over, Edge de Net_Eff)",
  "detailedAnalysis": "string (máximo 200 chars, foco no embate matemático dos últimos 14d)"
}"""


def build_analysis_payload(
    game: dict,
    inj_monitor: InjuryMonitor,
    h2h: list,
    home_stats: dict,
    away_stats: dict,
    home_momentum: dict,
    away_momentum: dict,
    home_defense: dict,
    away_defense: dict,
    home_db: dict,
    away_db: dict
) -> dict:
    """
    FIX: Responsabilidade de montagem do payload extraída de analyze_game,
    facilitando testes unitários e reduzindo o tamanho da função principal.
    """
    home = game['home']['displayName']
    away = game['away']['displayName']

    home_def_rating = home_defense.get('defensive_rating')
    away_def_rating = away_defense.get('defensive_rating')

    safe_home_drtg = home_def_rating if home_def_rating is not None else 115.0
    safe_away_drtg = away_def_rating if away_def_rating is not None else 115.0

    home_bad_defense = safe_home_drtg > 116.0
    away_bad_defense = safe_away_drtg > 116.0

    home_elite_inj = inj_monitor.get_elite_injuries(home)
    away_elite_inj = inj_monitor.get_elite_injuries(away)

    home_momentum_score = home_momentum.get('momentum_score', 0.5)
    away_momentum_score = away_momentum.get('momentum_score', 0.5)

    home_advantage_factor = "ALTO" if home_stats.get('is_contender') else "NORMAL"

    return {
        "Confronto": f"{home} vs {away}",
        "Metricas_Avancadas_14_Dias_Databallr": {
            "Home_Adv": {
                "ortg_ataque": home_db.get('ortg'),
                "drtg_defesa": home_db.get('drtg'),
                "eficiencia_liquida_net_eff": home_db.get('net_eff'),
                "true_shooting_pct": home_db.get('o_ts'),
                "rebote_ofensivo_pct": home_db.get('orb')
            },
            "Away_Adv": {
                "ortg_ataque": away_db.get('ortg'),
                "drtg_defesa": away_db.get('drtg'),
                "eficiencia_liquida_net_eff": away_db.get('net_eff'),
                "true_shooting_pct": away_db.get('o_ts'),
                "rebote_ofensivo_pct": away_db.get('orb')
            },
            "Instrucao_Cruzamento": (
                "Projete (Home ORTG vs Away DRTG) e (Away ORTG vs Home DRTG) "
                "para extrair a linha ideal de Over/Under."
            )
        },
        "Contexto_Casa": {
            "home_win_pct": home_stats.get('win_pct', 0),
            "is_contender": home_stats.get('is_contender', False),
            "is_weak": home_stats.get('is_weak', False),
            "home_advantage_factor": home_advantage_factor,
            "streak": home_stats.get('streak', '0')
        },
        "Contexto_Fora": {
            "away_win_pct": away_stats.get('win_pct', 0),
            "is_contender": away_stats.get('is_contender', False),
            "is_weak": away_stats.get('is_weak', False),
            "streak": away_stats.get('streak', '0')
        },
        "Momentum_Recalibrado": {
            "home_last_5": home_momentum.get('wins_last_5', 0),
            "home_losses_last_5": home_momentum.get('losses_last_5', 0),
            "home_momentum_score": home_momentum_score,
            "away_last_5": away_momentum.get('wins_last_5', 0),
            "away_losses_last_5": away_momentum.get('losses_last_5', 0),
            "away_momentum_score": away_momentum_score,
            "peso_momento": "ALTO (prioridade sobre season average)"
        },
        "Defesa_e_Pontuacao": {
            "home_def_rating": home_def_rating if home_def_rating is not None else "N/A",
            "away_def_rating": away_def_rating if away_def_rating is not None else "N/A",
            "home_bad_defense": home_bad_defense,
            "away_bad_defense": away_bad_defense,
            "tendencia_over": (
                "FORTE" if (home_bad_defense and away_bad_defense)
                else "MODERADA" if (home_bad_defense or away_bad_defense)
                else "NEUTRA"
            )
        },
        "Lesoes_Elite_Only": {
            "home_elite_injuries": home_elite_inj if home_elite_inj else "Nenhuma",
            "away_elite_injuries": away_elite_inj if away_elite_inj else "Nenhuma",
            "criterio": "Apenas jogadores nota >= 7.0 ou All-Star"
        },
        "H2H_Recente": h2h,
        "Market_Odds": get_market_odds(home, away),
        "Regras_Handicap": {
            "evitar": "+5.5 (armadilha estatística)",
            "preferir": "+10 (underdog claro) ou -5 (favorito sólido)"
        }
    }


def call_groq_with_retry(payload: dict) -> dict:
    """
    FIX: Chamada à API Groq extraída de analyze_game para
    responsabilidade única e reutilização mais fácil.
    """
    def _call():
        res = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": GROQ_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        raw_text = res.choices[0].message.content
        clean_text = extract_pure_json(raw_text)
        return json.loads(clean_text)

    return with_retry(_call)


def analyze_game(
    game: dict,
    inj_monitor: InjuryMonitor,
    h2h: list,
    home_stats: dict,
    away_stats: dict,
    home_momentum: dict,
    away_momentum: dict,
    home_defense: dict,
    away_defense: dict,
    home_db: dict,
    away_db: dict
):
    """
    FIX: Função refatorada — delega montagem de payload e chamada à IA
    para funções dedicadas (build_analysis_payload / call_groq_with_retry).
    """
    home = game['home']['displayName']
    away = game['away']['displayName']

    payload = build_analysis_payload(
        game, inj_monitor, h2h,
        home_stats, away_stats,
        home_momentum, away_momentum,
        home_defense, away_defense,
        home_db, away_db
    )

    try:
        return call_groq_with_retry(payload)
    except Exception as e:
        print(f"❌ Erro IA ({home} vs {away}): {e}")
        return None


# ==========================================
# 5. EXECUÇÃO PRINCIPAL (MAIN)
# ==========================================

if __name__ == "__main__":
    date_obj = datetime.now(pytz.timezone('America/Sao_Paulo'))
    date_iso = date_obj.strftime("%Y-%m-%d")
    print(f"🕒 INICIANDO MOTOR PREDITIVO PARA A DATA: {date_iso}")

    inj_monitor = InjuryMonitor("nba_injuries.json")
    games = get_espn_games(date_obj)

    if not games:
        print("✅ STATUS VERDE: Ausência confirmada de jogos na NBA para esta janela de 48h.")
        print("Finalizando operação pacificamente para preservar recursos computacionais.")
        sys.exit(0)

    print("🧠 Carregando tensores de eficiência Databallr (14 Dias)...")
    databallr_matrix = get_databallr_matrix()

    predictions = []

    for game in games:
        home_full = game['home']['displayName']
        away_full = game['away']['displayName']
        home_id = game['home']['id']
        away_id = game['away']['id']

        # FIX: game_id com f-string fechada corretamente e espaços sanitizados
        game_id = f"{date_iso}_{home_full}_vs_{away_full}".replace(" ", "_")

        print(f"\n🔎 Processando: {home_full} vs {away_full} (ID: {game_id})")

        # Coleta paralela de dados por time
        home_stats    = get_team_stats(home_id)
        away_stats    = get_team_stats(away_id)
        home_defense  = get_team_defense_metrics(home_id)
        away_defense  = get_team_defense_metrics(away_id)
        home_momentum = get_last_games(home_id)
        away_momentum = get_last_games(away_id)
        h2h           = extract_h2h(home_id, away_id)
        home_db       = match_databallr_stats(home_full, databallr_matrix)
        away_db       = match_databallr_stats(away_full, databallr_matrix)

        result = analyze_game(
            game, inj_monitor, h2h,
            home_stats, away_stats,
            home_momentum, away_momentum,
            home_defense, away_defense,
            home_db, away_db
        )

        if not result:
            print(f"⚠️ Análise ignorada para {home_full} vs {away_full}.")
            continue

        record = {
            "id": game_id,
            "date": date_iso,
            "home_team": home_full,
            "away_team": away_full,
            "prediction": result,
            "main_pick": result.get("palpite_principal"),
            "confidence": result.get("confianca"),
            "over_line": result.get("linha_seguranca_over"),
            "under_line": result.get("linha_seguranca_under"),
            "handicap_line": result.get("handicap_recomendado"),
            "injury_alert": result.get("alerta_lesao", "Não"),
            "key_factor": result.get("keyFactor"),
            "momentum_data": {
                "home": home_momentum,
                "away": away_momentum
            },
            "defense_data": h2h
        }

        try:
            supabase.table("game_predictions").upsert(record).execute()
            print(f"✅ Gravado: {home_full} vs {away_full} → {record['main_pick']} (conf: {record['confidence']})")
        except Exception as e:
            print(f"❌ Falha ao gravar no Supabase ({game_id}): {e}")

        predictions.append(record)

    print(f"\n🏁 Operação concluída. {len(predictions)} predições processadas para {date_iso}.")
