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
        elite_injuries = []
        for injury in self.injuries:
            if team_name in injury.get('team_name', ''):
                player_rating = injury.get('player_rating', 0) or injury.get('rating', 0)
                if isinstance(player_rating, (int, float)) and player_rating >= min_rating:
                    elite_injuries.append(injury)
                elif injury.get('is_star') or injury.get('all_star') or injury.get('impact') == 'high':
                    elite_injuries.append(injury)
        return elite_injuries

def extract_pure_json(raw_response):
    clean_text = raw_response.strip()
    if clean_text.startswith("```json"):
        clean_text = clean_text[7:]
    elif clean_text.startswith("```"):
        clean_text = clean_text[3:]
    if clean_text.endswith("```"):
        clean_text = clean_text[:-3]
    return clean_text.strip()

def with_retry(func, retries=3):
    for attempt in range(retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == retries:
                raise e
            time.sleep(1.5)

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
            comps = event['competitions'][0]['competitors']
            games.append({
                'id': event['id'], 
                'date': event['date'],
                'home': next(c['team'] for c in comps if c['homeAway'] == 'home'),
                'away': next(c['team'] for c in comps if c['homeAway'] == 'away')
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

def match_databallr_stats(espn_team_name, databallr_matrix):
    espn_lower = espn_team_name.lower()
    if espn_lower in databallr_matrix:
        return databallr_matrix[espn_lower]
    for db_name, stats in databallr_matrix.items():
        if db_name in espn_lower or espn_lower in db_name:
            return stats
    return {"ortg": 115.0, "drtg": 115.0, "net_eff": 0.0, "o_ts": 55.0, "orb": 25.0, "net_poss": 0}

def get_market_odds(home_full, away_full):
    try:
        res = supabase.table("nba_odds_matrix").select("*").execute()
        for row in res.data:
            if home_full in row.get("matchup", "") or away_full in row.get("matchup", ""):
                return row
    except:
        pass
    return "Mercado Indisponível"

def get_team_stats(team_id):
    try:
        url = f"[https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/](https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/){team_id}"
        res = requests.get(url, timeout=10).json()
        team_data = res.get('team', {})
        
        standing = team_data.get('standingSummary', '')
        record = team_data.get('record', {})
        items = record.get('items', [])
        
        wins = 0
        losses = 0
        win_pct = 0.5
        streak = "0"
        
        if items and len(items) > 0:
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
    except Exception as e:
        return {'win_pct': 0.5, 'wins': 0, 'losses': 0, 'streak': '0', 'is_contender': False, 'is_weak': False, 'standing_summary': ''}

def get_team_defense_metrics(team_id):
    def normalize_metric_value(raw_value):
        if raw_value is None: return None
        if isinstance(raw_value, (int, float)): return float(raw_value)
        if isinstance(raw_value, str):
            cleaned = raw_value.strip().replace(",", ".")
            if not cleaned: return None
            try: return float(cleaned)
            except ValueError: return None
        return None

    def iter_stats_objects(payload):
        if isinstance(payload, dict):
            for key, value in payload.items():
                if key == "stats" and isinstance(value, list): yield from value
                else: yield from iter_stats_objects(value)
        elif isinstance(payload, list):
            for item in payload: yield from iter_stats_objects(item)

    def match_stat_name(stat):
        summary = " ".join([
            str(stat.get('name', '')).lower().strip(),
            str(stat.get('displayName', '')).lower().strip(),
            str(stat.get('shortDisplayName', '')).lower().strip()
        ])
        if "defensive" in summary and "rating" in summary: return "defensive_rating"
        if "pace" in summary: return "pace"
        if "points allowed" in summary or "opp points" in summary or "opponent points" in summary: return "points_allowed"
        return None

    try:
        url = f"[https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/](https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/){team_id}/statistics"
        res = requests.get(url, timeout=10)
        if res.status_code != 200: return {'defensive_rating': None, 'pace': None, 'points_allowed': None}
        
        data = res.json()
        defensive_rating, pace, points_allowed = None, None, None

        for stat in iter_stats_objects(data):
            metric_name = match_stat_name(stat)
            if not metric_name: continue
            metric_value = normalize_metric_value(stat.get('value', stat.get('displayValue')))

            if metric_name == "defensive_rating" and defensive_rating is None: defensive_rating = metric_value
            elif metric_name == "pace" and pace is None: pace = metric_value
            elif metric_name == "points_allowed" and points_allowed is None: points_allowed = metric_value
        
        return {'defensive_rating': defensive_rating, 'pace': pace, 'points_allowed': points_allowed}
    except Exception as e:
        return {'defensive_rating': None, 'pace': None, 'points_allowed': None}

def extract_h2h(team_id, opponent_id):
    # CORREÇÃO: URL higienizada e livre de artefatos Markdown
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/schedule"
    
    def fetch_schedule():
        res = requests.get(url, timeout=10)
        res.raise_for_status() 
        return res.json().get('events', [])

    try:
        # Dependência: A função with_retry deve estar ativa no escopo global
        events = with_retry(fetch_schedule, retries=3)
        if not events: 
            return []
        
        past_games = [e for e in events if e.get('competitions', [{}])[0].get('status', {}).get('type', {}).get('state') == 'post']
        past_games.sort(key=lambda x: x.get('date', ''), reverse=True)
        
        h2h_raw = [g for g in past_games if any(c.get('id') == str(opponent_id) for c in g.get('competitions', [{}])[0].get('competitors', []))]
        
        parsed = []
        for g in h2h_raw[:2]:
            comp = g['competitions'][0]['competitors']
            main = next((c for c in comp if c.get('id') == str(team_id)), None)
            opp = next((c for c in comp if c.get('id') != str(team_id)), None)
            
            if not main or not opp:
                continue
                
            dt = datetime.strptime(g['date'], "%Y-%m-%dT%H:%MZ")
            
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
    except Exception as e:
        # Silenciamento estratégico no log para evitar saturação em I/O
        return []

def get_last_games(team_id, limit=5):
    # CORREÇÃO: URL higienizada
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/schedule"
    
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        events = res.json().get('events', [])
        
        past_games = [e for e in events if e.get('competitions', [{}])[0].get('status', {}).get('type', {}).get('state') == 'post']
        past_games.sort(key=lambda x: x.get('date', ''), reverse=True)
        
        last_games = []
        wins = 0
        losses = 0
        
        for g in past_games[:limit]:
            comp = g.get('competitions', [{}])[0]
            team_comp = next((c for c in comp.get('competitors', []) if c.get('id') == str(team_id)), None)
            
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
            'momentum_score': float(f"{momentum_score:.3f}") # Precisão flutuante limpa
        }
    except Exception as e:
        return {
            'last_games': [], 
            'wins_last_5': 0, 
            'losses_last_5': 0, 
            'momentum_score': 0.5
        }
        

# ==========================================
# 4. MOTOR PREDITIVO (GROQ IA)
# ==========================================
SYSTEM_INSTRUCTION = """Você é o Estatístico Chefe do sistema NBA-MONITOR. Calcule o Edge.

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

def analyze_game(game, inj_monitor, h2h, home_stats, away_stats, home_momentum, away_momentum, home_defense, away_defense, home_db, away_db):
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

    payload = {
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
            "Instrucao_Cruzamento": "Projete (Home ORTG vs Away DRTG) e (Away ORTG vs Home DRTG) para extrair a linha ideal de Over/Under."
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
            "home_def_rating": home_def_rating if home_def_rating else "N/A",
            "away_def_rating": away_def_rating if away_def_rating else "N/A",
            "home_bad_defense": home_bad_defense,
            "away_bad_defense": away_bad_defense,
            "tendencia_over": "FORTE" if (home_bad_defense and away_bad_defense) else ("MODERADA" if (home_bad_defense or away_bad_defense) else "NEUTRA")
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
    
    def call_groq():
        res = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SYSTEM_INSTRUCTION}, 
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}
            ],
            temperature=0.1, 
            response_format={"type": "json_object"}
        )
        raw_text = res.choices[0].message.content
        clean_text = extract_pure_json(raw_text)
        return json.loads(clean_text)
    
    try: 
        return with_retry(call_groq)
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
        game_id = f"{date_iso}_{home_full}_{away_full}".replace(" ", "_")
        
        print(f"⚡ Processando colisão: {home_full} vs {away_full}")
        
        home_db_stats = match_databallr_stats(home_full, databallr_matrix)
        away_db_stats = match_databallr_stats(away_full, databallr_matrix)
        
        h2h_data = {"home_vs_away": extract_h2h(game['home']['id'], game['away']['id'])}
        home_stats = get_team_stats(game['home']['id'])
        away_stats = get_team_stats(game['away']['id'])
        home_momentum = get_last_games(game['home']['id'], limit=5)
        away_momentum = get_last_games(game['away']['id'], limit=5)
        home_defense = get_team_defense_metrics(game['home']['id'])
        away_defense = get_team_defense_metrics(game['away']['id'])
        
        ai_result = analyze_game(
            game, inj_monitor, h2h_data, home_stats, away_stats,
            home_momentum, away_momentum, home_defense, away_defense,
            home_db_stats, away_db_stats
        )
        
        if ai_result:
            predictions.append({
                "id": game_id, 
                "date": date_iso, 
                "home_team": home_full, 
                "away_team": away_full,
                "prediction": json.dumps(ai_result, ensure_ascii=False),
                "main_pick": ai_result.get("palpite_principal", "N/A"),
                "confidence": float(ai_result.get("confianca", 0.0)),
                "over_line": ai_result.get("linha_seguranca_over", ""),
                "under_line": ai_result.get("linha_seguranca_under", ""),
                "handicap_line": ai_result.get("handicap_recomendado", ""),
                "injury_alert": ai_result.get("alerta_lesao", "Não"),
                "key_factor": ai_result.get("keyFactor", ""),
                "momentum_data": h2h_data, # <--- RESTAURAÇÃO: Apenas os confrontos diretos
                "defense_data": {
                    "home_def_rating": home_defense.get('defensive_rating'),
                    "away_def_rating": away_defense.get('defensive_rating')
                }
            })
        time.sleep(1.5)

    if games and not predictions:
        print("⚠️ ALERTA CRÍTICO: Jogos detetados, mas a IA gerou zero matrizes preditivas.")
        print("Possível colapso no parse JSON do Groq ou Rate Limit excedido.")
        sys.exit(1)

    print(f"📦 Empacotando {len(predictions)} matrizes preditivas para injeção no Supabase...")
    
    try:
        supabase.table("game_predictions").upsert(predictions).execute()
        print("✅ SUCESSO ABSOLUTO: Matrizes termodinâmicas injetadas na tabela 'game_predictions'.")
    except Exception as e:
        print(f"❌ FALHA NO UPSERT: {e}")
        sys.exit(1)
