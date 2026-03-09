import os
import json
import time
import random
import re
import requests
from datetime import datetime, timedelta
import pytz
from supabase import create_client
from groq import Groq

# --- CONFIGURAÇÃO DE AMBIENTE ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
INJURIES_FILE = "nba_injuries.json" 

if not all([SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY]):
    print("❌ COLAPSO_DE_SISTEMA: Faltam variáveis de ambiente críticas.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
groq_client = Groq(api_key=GROQ_API_KEY)
MODEL_ID = "llama-3.3-70b-versatile"

# --- CLASSE DE GESTÃO DE LESÕES ---
class InjuryMonitor:
    def __init__(self, filepath):
        self.injuries = []
        self.filepath = filepath
        self.load_injuries()

    def load_injuries(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    self.injuries = json.load(f)
                print(f"🚑 Matriz de degradação: {len(self.injuries)} lesões carregadas.")
            except Exception as e:
                print(f"⚠️ Erro ao ler matriz de lesões: {e}")

# --- FUNÇÃO DE EXTRAÇÃO DA ESPN (EVOLUÍDA PARA DISP_NAME) ---
def get_espn_games(date_obj):
    date_str = date_obj.strftime("%Y%m%d")
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        games = []
        for event in data.get('events', []):
            competition = event['competitions'][0]
            competitors = competition['competitors']
            # Captura o objeto completo da equipa para evitar confusão de nomes
            home_team = next(c['team'] for c in competitors if c['homeAway'] == 'home')
            away_team = next(c['team'] for c in competitors if c['homeAway'] == 'away')
            games.append({
                'id': event['id'],
                'date': event['date'],
                'home': home_team,
                'away': away_team
            })
        return games
    except Exception as e:
        print(f"❌ Falha ao extrair payload da ESPN: {e}")
        return []

# --- EXTRATOR DE MERCADO (BLINDAGEM CONTRA NETS/HORNETS) ---
def get_market_odds(home_full_name, away_full_name):
    try:
        res = supabase.table("nba_odds_matrix").select("*").execute()
        for row in res.data:
            matchup = row.get("matchup", "")
            
            # Protocolo de Correspondência Estrita:
            # Verifica se o nome completo da equipa aparece na string do confronto
            if home_full_name in matchup or away_full_name in matchup:
                return row
                
        return "Mercado Indisponível (Calcular projeção isolada)"
    except Exception as e:
        print(f"⚠️ Anomalia na extração de Odds: {e}")
        return None

# --- MOTOR DE COLISÃO VETORIAL (GROQ JSON MODE) ---
SYSTEM_INSTRUCTION = """Você é o Estatístico Chefe do sistema NBA-MONITOR.
Processe a matriz de colisão vetorial e determine o 'Edge' (Vantagem Matemática).
Você não é um comentarista. Aplique raciocínio determinístico e jargão termodinâmico.

DIRETRIZES:
1. PACE E EFICIÊNCIA: Cruze a eficiência de ataque/defesa projetada.
2. DEGRADAÇÃO: Aplique penalidades pesadas baseadas no payload 'Desfalques'.
3. ASSIMETRIA DE MERCADO: Compare sua projeção com 'Market_Odds'.

SAÍDA OBRIGATÓRIA (JSON Estrito):
{
  "palpite_principal": "string",
  "confianca": "float (0-100)",
  "linha_seguranca_over": "string",
  "linha_seguranca_under": "string",
  "alerta_lesao": "string",
  "keyFactor": "string",
  "detailedAnalysis": "string"
}"""

def analyze_game(game_data, injuries_data):
    # Usar displayName ("Brooklyn Nets") em vez de name ("Nets") para evitar colisões
    home_name = game_data['home']['displayName']
    away_name = game_data['away']['displayName']
    
    market_data = get_market_odds(home_name, away_name)
    game_injuries = [inj for inj in injuries_data if home_name in inj.get('player_name', '') or away_name in inj.get('team_name', '')]
    
    payload = {
        "Confronto": f"{home_name} vs {away_name}",
        "Desfalques_Confronto": game_injuries if game_injuries else "Nenhum desfalque crítico",
        "Market_Odds": market_data,
    }

    prompt = f"Execute a colisão vetorial rigorosa:\n{json.dumps(payload, indent=2, ensure_ascii=False)}"

    def call_groq():
        response = groq_client.chat.completions.create(
            model=MODEL_ID,
            messages=[
                {"role": "system", "content": SYSTEM_INSTRUCTION},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        return json.loads(response.choices[0].message.content)

    try:
        # Algoritmo with_retry deve estar definido no seu escopo
        return call_groq() 
    except Exception as e:
        print(f"❌ Erro na IA ({home_name} vs {away_name}): {e}")
        return None

# --- CICLO DE EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    tz = pytz.timezone('America/Sao_Paulo')
    now = datetime.now(tz)
    
    if now.hour < 10:
        date_obj = now - timedelta(days=1)
    else:
        date_obj = now
        
    date_iso = date_obj.strftime("%Y-%m-%d")
    print(f"🏀 Iniciando Varredura. Data: {date_iso}")
    
    inj_monitor = InjuryMonitor(INJURIES_FILE)
    games = get_espn_games(date_obj)

    if not games:
        print("💤 Matriz de jogos vazia.")
        exit(0)

    predictions = []
    for game in games:
        # Mapeamento estrito para identificação no Supabase
        home_full = game['home']['displayName']
        away_full = game['away']['displayName']
        game_id = f"{date_iso}_{home_full}_{away_full}".replace(" ", "_")

        print(f"⚡ Processando colisão: {home_full} vs {away_full}...")
        ai_result = analyze_game(game, inj_monitor.injuries)
        
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
                "injury_alert": ai_result.get("alerta_lesao", "Não"),
                "key_factor": ai_result.get("keyFactor", "")
            })
        time.sleep(1.5)

    if predictions:
        print(f"💾 Sincronizando {len(predictions)} predições...")
        supabase.table("game_predictions").upsert(predictions).execute()
        print("✅ Operação Concluída.")
