
import os
import json
import time
import requests
from datetime import datetime
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
    exit(1)

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
        """Retorna apenas lesões de jogadores elite (nota >= 7)."""
        elite_injuries = []
        for injury in self.injuries:
            if team_name in injury.get('team_name', ''):
                # Verifica se há rating do jogador
                player_rating = injury.get('player_rating', 0) or injury.get('rating', 0)
                if isinstance(player_rating, (int, float)) and player_rating >= min_rating:
                    elite_injuries.append(injury)
                # Se não tiver rating explícito, verifica se é "star" ou "all-star"
                elif injury.get('is_star') or injury.get('all_star') or injury.get('impact') == 'high':
                    elite_injuries.append(injury)
        return elite_injuries

def extract_pure_json(raw_response):
    """Remove a escória visual (Markdown) que a IA injeta no texto."""
    clean_text = raw_response.strip()
    if clean_text.startswith("```json"):
        clean_text = clean_text[7:]
    elif clean_text.startswith("```"):
        clean_text = clean_text[3:]
        
    if clean_text.endswith("```"):
        clean_text = clean_text[:-3]
        
    return clean_text.strip()

def with_retry(func, retries=3):
    """Motor de resiliência contra latência de rede."""
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
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_obj.strftime('%Y%m%d')}"
    res = requests.get(url).json()
    games = []
    for event in res.get('events', []):
        comps = event['competitions'][0]['competitors']
        games.append({
            'id': event['id'], 
            'date': event['date'],
            'home': next(c['team'] for c in comps if c['homeAway'] == 'home'),
            'away': next(c['team'] for c in comps if c['homeAway'] == 'away')
        })
    return games

def get_market_odds(home_full, away_full):
    res = supabase.table("nba_odds_matrix").select("*").execute()
    for row in res.data:
        if home_full in row.get("matchup", "") or away_full in row.get("matchup", ""):
            return row
    return "Mercado Indisponível"

def get_team_stats(team_id):
    """Busca estatísticas avançadas do time (contender status, defesa, pace)."""
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}"
        res = requests.get(url, timeout=10).json()
        team_data = res.get('team', {})
        
        # Dados de standings para calcular win%
        standing = team_data.get('standingSummary', '')
        record = team_data.get('record', {})
        
        # Calcular win rate e streak
        overall = record.get('items', [{}])[0] if record.get('items') else {}
        stats = overall.get('stats', [])
        
        wins = 0
        losses = 0
        win_pct = 0.5
        streak = "0"
        
        for stat in stats:
            if stat.get('name') == 'wins':
                wins = int(stat.get('value', 0))
            elif stat.get('name') == 'losses':
                losses = int(stat.get('value', 0))
            elif stat.get('name') == 'streak':
                streak = stat.get('displayValue', '0')
        
        if wins + losses > 0:
            win_pct = wins / (wins + losses)
        
        # Classificar como contender (>60% wins) ou fraco (<40% wins)
        is_contender = win_pct >= 0.60
        is_weak = win_pct <= 0.40
        
        return {
            'win_pct': win_pct,
            'wins': wins,
            'losses': losses,
            'streak': streak,
            'is_contender': is_contender,
            'is_weak': is_weak,
            'standing_summary': standing
        }
    except Exception as e:
        print(f"⚠️ Erro ao buscar stats do time {team_id}: {e}")
        return {
            'win_pct': 0.5,
            'wins': 0,
            'losses': 0,
            'streak': '0',
            'is_contender': False,
            'is_weak': False,
            'standing_summary': ''
        }

def get_team_defense_metrics(team_id):
    """Busca métricas defensivas e de pace do time."""
    try:
        # ESPN API para estatísticas do time
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/statistics"
        res = requests.get(url, timeout=10)
        
        if res.status_code != 200:
            return {'defensive_rating': None, 'pace': None, 'points_allowed': None}
        
        data = res.json()
        stats = data.get('results', {}).get('stats', [])
        
        defensive_rating = None
        pace = None
        points_allowed = None
        
        for stat in stats:
            name = stat.get('name', '').lower()
            if 'defensive' in name and 'rating' in name:
                defensive_rating = stat.get('value')
            elif 'pace' in name:
                pace = stat.get('value')
            elif 'points allowed' in name or 'opp points' in name:
                points_allowed = stat.get('value')
        
        return {
            'defensive_rating': defensive_rating,
            'pace': pace,
            'points_allowed': points_allowed
        }
    except Exception as e:
        print(f"⚠️ Erro ao buscar métricas defensivas: {e}")
        return {'defensive_rating': None, 'pace': None, 'points_allowed': None}

def extract_h2h(team_id, opponent_id):
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/schedule"
    
    def fetch_schedule():
        res = requests.get(url, timeout=10)
        res.raise_for_status() 
        return res.json().get('events', [])

    try:
        events = with_retry(fetch_schedule, retries=3)
        if not events: return []
        
        past_games = [e for e in events if e['competitions'][0]['status']['type']['state'] == 'post']
        past_games.sort(key=lambda x: x['date'], reverse=True)
        h2h_raw = [g for g in past_games if any(c['id'] == str(opponent_id) for c in g['competitions'][0]['competitors'])]
        
        parsed = []
        for g in h2h_raw[:2]:
            comp = g['competitions'][0]['competitors']
            main = next(c for c in comp if c['id'] == str(team_id))
            opp = next(c for c in comp if c['id'] != str(team_id))
            dt = datetime.strptime(g['date'], "%Y-%m-%dT%H:%MZ")
            
            def get_score(c):
                s = c.get('score', 0)
                if isinstance(s, dict): return int(s.get('value', 0))
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
        print(f"⚠️ Colapso na extração H2H: {e}")
        return []

def get_last_games(team_id, limit=5):
    """Busca os últimos jogos do time para análise de momentum."""
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/schedule"
    
    try:
        res = requests.get(url, timeout=10)
        events = res.json().get('events', [])
        
        past_games = [e for e in events if e['competitions'][0]['status']['type']['state'] == 'post']
        past_games.sort(key=lambda x: x['date'], reverse=True)
        
        last_games = []
        wins = 0
        losses = 0
        
        for g in past_games[:limit]:
            comp = g['competitions'][0]
            team_comp = next((c for c in comp['competitors'] if c['id'] == str(team_id)), None)
            
            if team_comp:
                is_winner = team_comp.get('winner', False)
                if is_winner:
                    wins += 1
                else:
                    losses += 1
                
                last_games.append({
                    'result': 'V' if is_winner else 'D',
                    'date': g['date'][:10],
                    'home_away': 'CASA' if team_comp.get('homeAway') == 'home' else 'FORA'
                })
        
        return {
            'last_games': last_games,
            'wins_last_5': wins,
            'losses_last_5': losses,
            'momentum_score': wins / (wins + losses) if (wins + losses) > 0 else 0.5
        }
    except Exception as e:
        print(f"⚠️ Erro ao buscar últimos jogos: {e}")
        return {'last_games': [], 'wins_last_5': 0, 'losses_last_5': 0, 'momentum_score': 0.5}

# ==========================================
# 4. MOTOR PREDITIVO (GROQ IA)
# ==========================================
SYSTEM_INSTRUCTION = """Você é o Estatístico Chefe do sistema NBA-MONITOR. Calcule o Edge.

DIRETRIZES OBRIGATÓRIAS DE ANÁLISE:

1. IMPACTO DE ESTRELAS (ELITE ONLY):
   - Só considere impacto de lesão se o jogador for ESTRELA DE ELITE (nota >= 7.0 ou All-Star)
   - Jogadores role players não alteram significativamente o resultado

2. FATORES CONTEXTUAIS:
   - FATORES CASA: Considere distância de viagem do time visitante (jet lag, costas-a-costas)
   - Times CONTENDERS (win% >= 60%) têm 78% de vitórias em casa
   - Times FRACOS (win% <= 40%) têm apenas 36% de vitórias em casa
   - Recalibração: Dê MAIOR PESO ao momento atual (últimos 5 jogos) vs season average

3. ANÁLISE DEFENSIVA E PONTUAÇÃO:
   - Defesa Ruim = Tendência FORTE de OVER
   - Para apostar em OVER, verifique: "Os dois times têm estrelas para fazer +110 pontos cada?"
   - Se não tiverem capacidade ofensiva confirmada, EVITE o OVER
   - PACE e EFICIÊNCIA: Cruze eficiência de ataque vs defesa projetada

4. HANDICAPS (REGRAS OBRIGATÓRIAS):
   - Handicap +5.5 NÃO PRESTA (EVITE ESSA LINHA EXATA)
   - PREFERÊNCIA: Busque linhas próximas a +10 (underdog claro) ou -5 (favorito sólido)
   - Linhas entre +4 e +6 são armadilhas estatísticas

SAÍDA OBRIGATÓRIA (JSON Estrito):
{
  "palpite_principal": "string (ex: OVER 225.5, Boston -5, Philadelphia +10)",
  "confianca": 0.0,
  "linha_seguranca_over": "string",
  "linha_seguranca_under": "string", 
  "handicap_recomendado": "string (evite +5.5, prefira +10 ou -5)",
  "alerta_lesao": "string (só se estrela elite >=7)",
  "keyFactor": "string (momento, defesa, casa, etc)",
  "detailedAnalysis": "string (máximo 200 chars, foco em edge identificado)"
}"""

def analyze_game(game, inj_monitor, h2h, home_stats, away_stats, home_momentum, away_momentum, home_defense, away_defense):
    home = game['home']['displayName']
    away = game['away']['displayName']
    
    # Só considera lesões de ELITE (nota >= 7)
    home_elite_inj = inj_monitor.get_elite_injuries(home, min_rating=7.0)
    away_elite_inj = inj_monitor.get_elite_injuries(away, min_rating=7.0)
    
    # Análise de contender vs weak
    home_advantage_factor = 0.78 if home_stats.get('is_contender') else (0.36 if home_stats.get('is_weak') else 0.60)
    away_disadvantage = 0.36 if away_stats.get('is_weak') else 0.50
    
    # Momentum recalibrado (maior peso)
    home_momentum_score = home_momentum.get('momentum_score', 0.5)
    away_momentum_score = away_momentum.get('momentum_score', 0.5)
    
    # Análise defensiva para OVER/UNDER
    home_def_rating = home_defense.get('defensive_rating', 0) or 0
    away_def_rating = away_defense.get('defensive_rating', 0) or 0
    bad_defense_threshold = 115  # Defesa ruim = rating alto
    
    home_bad_defense = home_def_rating > bad_defense_threshold if home_def_rating else False
    away_bad_defense = away_def_rating > bad_defense_threshold if away_def_rating else False
    
    payload = {
        "Confronto": f"{home} vs {away}",
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
    # GARANTIA TEMPORAL ABSOLUTA
    date_obj = datetime.now(pytz.timezone('America/Sao_Paulo'))
    date_iso = date_obj.strftime("%Y-%m-%d")
    print(f"🕒 INICIANDO MOTOR PREDITIVO PARA A DATA: {date_iso}")

    inj_monitor = InjuryMonitor("nba_injuries.json")
    games = get_espn_games(date_obj)
    predictions = []

    for game in games:
        home_full = game['home']['displayName']
        away_full = game['away']['displayName']
        game_id = f"{date_iso}_{home_full}_{away_full}".replace(" ", "_")
        
        print(f"⚡ Processando colisão: {home_full} vs {away_full}")
        
        # Coleta de dados enriquecida
        h2h_data = {"home_vs_away": extract_h2h(game['home']['id'], game['away']['id'])}
        home_stats = get_team_stats(game['home']['id'])
        away_stats = get_team_stats(game['away']['id'])
        home_momentum = get_last_games(game['home']['id'], limit=5)
        away_momentum = get_last_games(game['away']['id'], limit=5)
        home_defense = get_team_defense_metrics(game['home']['id'])
        away_defense = get_team_defense_metrics(game['away']['id'])
        
        ai_result = analyze_game(
            game, 
            inj_monitor, 
            h2h_data, 
            home_stats, 
            away_stats,
            home_momentum,
            away_momentum,
            home_defense,
            away_defense
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
                "momentum_data": {
                    "home": home_momentum,
                    "away": away_momentum
                },
                "defense_data": {
                    "home_def_rating": home_defense.get('defensive_rating'),
                    "away_def_rating": away_defense.get('defensive_rating')
                }
            })
        time.sleep(1.5) # Respeita o Rate Limit da IA

    # MOTOR DE PÂNICO E INJEÇÃO
    if not predictions:
        print("⚠️ ALERTA CRÍTICO: Zero predições geradas. Possível falha na API da ESPN (Sem jogos) ou na API do Groq (Rate Limit/Parse Error).")
        exit(1) # OBRIGA o GitHub Actions a acionar luz vermelha.

    print(f"📦 Empacotando {len(predictions)} matrizes preditivas para injeção no Supabase...")
    
    try:
        supabase.table("game_predictions").upsert(predictions).execute()
        print("✅ SUCESSO ABSOLUTO: Matrizes termodinâmicas injetadas na tabela 'game_predictions'.")
    except Exception as e:
        print(f"❌ COLAPSO NO BANCO DE DADOS: A estrutura enviada foi rejeitada pelo Supabase.")
        print(f"MOTIVO: {e}")
        exit(1) # OBRIGADO o GitHub Actions a acionar luz vermelha.
