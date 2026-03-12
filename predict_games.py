import os
import json
import time
import random
import requests
from datetime import datetime, timedelta
import pytz
from supabase import create_client
import google.generativeai as genai

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if not all([SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, GEMINI_API_KEY]):
    print("❌ COLAPSO_DE_SISTEMA: Faltam variáveis de ambiente críticas.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
genai.configure(api_key=GEMINI_API_KEY)

class InjuryMonitor:
    def __init__(self, filepath):
        self.injuries = []
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                self.injuries = json.load(f)

def get_espn_games(date_obj):
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_obj.strftime('%Y%m%d')}"
    res = requests.get(url).json()
    games = []
    for event in res.get('events', []):
        comps = event['competitions'][0]['competitors']
        games.append({
            'id': event['id'], 'date': event['date'],
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

def with_retry(func, retries=3, base_delay=2.0):
    for attempt in range(retries + 1):
        try:
            return func()
        except requests.exceptions.RequestException as e:
            if attempt == retries:
                print(f"❌ [FALHA_CRÍTICA] Esgotamento de retentativas: {e}")
                raise e
            delay = (base_delay * (2 ** attempt)) + random.uniform(0, 1.0)
            print(f"⚠️ [ANOMALIA_DE_REDE] Tentativa {attempt + 1}/{retries} falhou. Backoff ativo: aguardando {delay:.2f}s...")
            time.sleep(delay)

def extract_h2h(team_id, opponent_id):
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/schedule"
    
    def fetch_schedule():
        res = requests.get(url, timeout=7)
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
        print(f"⚠️ [COLAPSO_H2H] Falha irrecuperável na extração H2H: {e}")
        return []

SYSTEM_INSTRUCTION = """Você é o Estatístico Chefe do sistema NBA-MONITOR. Calcule o Edge.
DIRETRIZES: Avalie ritmo, degradação térmica (lesões) e assimetria de mercado (Market_Odds). 
SAÍDA OBRIGATÓRIA (JSON Estrito):
{"palpite_principal": "string", "confianca": 0.0, "linha_seguranca_over": "string", "linha_seguranca_under": "string", "alerta_lesao": "string", "keyFactor": "string", "detailedAnalysis": "string"}"""

def analyze_game(game, inj, h2h):
    home = game['home']['displayName']
    away = game['away']['displayName']
    game_inj = [i for i in inj if home in i.get('player_name','') or away in i.get('team_name','')]
    
    payload = {
        "Confronto": f"{home} vs {away}",
        "H2H_Recente": h2h,
        "Desfalques": game_inj or "Nenhum",
        "Market_Odds": get_market_odds(home, away)
    }
    
    def call_ai_studio():
        model = genai.GenerativeModel(
            model_name="gemini-3-flash_preview",
            system_instruction=SYSTEM_INSTRUCTION,
            generation_config={
                "response_mime_type": "application/json",
                "temperature": 0.1
            }
        )
        res = model.generate_content(json.dumps(payload))
        return json.loads(res.text)
    
    try: 
        def retry_ai():
            for attempt in range(3):
                try: return call_ai_studio()
                except Exception as e:
                    if attempt == 2: raise e
                    time.sleep(2)
        return retry_ai()
    except Exception as e: 
        print(f"❌ [FALHA_ESTATÍSTICO_CHEFE] Erro na geração do Edge: {e}")
        return None

if __name__ == "__main__":
    now = datetime.now(pytz.timezone('America/Sao_Paulo'))
    date_obj = now - timedelta(days=1) if now.hour < 6 else now
    date_iso = date_obj.strftime("%Y-%m-%d")
    
    inj_monitor = InjuryMonitor("nba_injuries.json")
    games = get_espn_games(date_obj)
    predictions = []

    for game in games:
        home_full = game['home']['displayName']
        away_full = game['away']['displayName']
        game_id = f"{date_iso}_{home_full}_{away_full}".replace(" ", "_")
        
        print(f"⚡ Processando colisão: {home_full} vs {away_full}")
        h2h_data = {"home_vs_away": extract_h2h(game['home']['id'], game['away']['id'])}
        
        ai_result = analyze_game(game, inj_monitor.injuries, h2h_data)
        if ai_result:
            predictions.append({
                "id": game_id, "date": date_iso, "home_team": home_full, "away_team": away_full,
                "prediction": json.dumps(ai_result, ensure_ascii=False),
                "main_pick": ai_result.get("palpite_principal", "N/A"),
                "confidence": float(ai_result.get("confianca", 0.0)),
                "over_line": ai_result.get("linha_seguranca_over", ""),
                "under_line": ai_result.get("linha_seguranca_under", ""),
                "injury_alert": ai_result.get("alerta_lesao", "Não"),
                "key_factor": ai_result.get("keyFactor", ""),
                "momentum_data": h2h_data
            })
        time.sleep(1.5)

    if predictions:
        supabase.table("game_predictions").upsert(predictions).execute()
        print("✅ Matriz de predições selada no banco de dados.")
