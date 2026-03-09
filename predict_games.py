import os
import json
import time
import random
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

# --- CLASSE DE GESTÃO DE LESÕES (MANTIDA DO SETUP ORIGINAL) ---
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

# --- FUNÇÃO DE EXTRAÇÃO DA ESPN (MANTIDA DO SETUP ORIGINAL) ---
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

# --- EXTRATOR DE MERCADO (ATUALIZAÇÃO V3.0) ---
def get_market_odds(home_team, away_team):
    try:
        res = supabase.table("nba_odds_matrix").select("*").execute()
        for row in res.data:
            matchup = row.get("matchup", "")
            if home_team in matchup or away_team in matchup:
                return row
        return "Mercado Indisponível (Calcular projeção isolada)"
    except Exception as e:
        print(f"⚠️ Anomalia na extração de Odds: {e}")
        return None

# --- ALGORITMO DE RESILIÊNCIA (FULL JITTER V3.0) ---
def with_retry(func, retries=3, initial_delay=1.0, max_delay=10.0):
    last_error = None
    for attempt in range(retries + 1):
        try:
            return func()
        except Exception as e:
            last_error = e
            if attempt == retries:
                raise last_error
            backoff_limit = min(max_delay, initial_delay * (2 ** attempt))
            jitter_delay = random.uniform(0, backoff_limit)
            print(f"[REDE] Latência no nó de inferência (Tentativa {attempt + 1}). Reiniciando em {jitter_delay:.2f}s...")
            time.sleep(jitter_delay)
    return None

# --- MOTOR DE COLISÃO VETORIAL (GROQ JSON MODE) ---
SYSTEM_INSTRUCTION = """Você é o Estatístico Chefe do sistema NBA-MONITOR.
Processe a matriz de colisão vetorial e determine o 'Edge' (Vantagem Matemática).
Você não é um comentarista. Aplique raciocínio determinístico e jargão termodinâmico.

DIRETRIZES:
1. PACE E EFICIÊNCIA: Cruze a eficiência de ataque/defesa projetada.
2. DEGRADAÇÃO: Aplique penalidades pesadas baseadas no payload 'Desfalques'.
3. ASSIMETRIA DE MERCADO: Compare sua projeção com 'Market_Odds'.

SAÍDA OBRIGATÓRIA (JSON ESTrito):
{
  "palpite_principal": "string (ex: Lakers -4.5 ou Over 220.5)",
  "confianca": "float (0 a 100)",
  "linha_seguranca_over": "string",
  "linha_seguranca_under": "string",
  "alerta_lesao": "string (Sim/Não - Descreva impacto)",
  "keyFactor": "string",
  "detailedAnalysis": "string (Equação lógica de forma brutalista)"
}"""

def analyze_game(game_data, injuries_data):
    home = game_data['home']['name']
    away = game_data['away']['name']
    
    market_data = get_market_odds(home, away)
    game_injuries = [inj for inj in injuries_data if home in inj.get('team', '') or away in inj.get('team', '')]
    
    payload = {
        "Confronto": f"{home} vs {away}",
        "Desfalques_Confronto": game_injuries if game_injuries else "Nenhum desfalque crítico reportado",
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
            temperature=0.1, # Frieza matemática ativada
            response_format={"type": "json_object"} # Força saída JSON
        )
        return json.loads(response.choices[0].message.content)

    try:
        return with_retry(call_groq)
    except Exception as e:
        print(f"❌ Colapso de processamento ({home} vs {away}): {e}")
        return None

# --- CICLO DE EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    tz = pytz.timezone('America/Sao_Paulo')
    now = datetime.now(tz)
    
    # Se passar das 10h da manhã, foca nos jogos do dia atual, senão pega os de ontem
    if now.hour < 10:
        date_obj = now - timedelta(days=1)
    else:
        date_obj = now
        
    date_iso = date_obj.strftime("%Y-%m-%d")
    print(f"🏀 Iniciando Varredura. Data: {date_iso}")
    
    inj_monitor = InjuryMonitor(INJURIES_FILE)
    games = get_espn_games(date_obj)

    if not games:
        print("💤 Matriz de jogos vazia. Encerrando operação.")
        exit(0)

    predictions = []
    for game in games:
        home = game['home']['name']
        away = game['away']['name']
        game_id = f"{date_iso}_{home}_{away}".replace(" ", "")

        print(f"⚡ Processando colisão: {home} vs {away}...")
        ai_result = analyze_game(game, inj_monitor.injuries)
        
        if ai_result:
            prediction_json_str = json.dumps(ai_result, ensure_ascii=False)

            predictions.append({
                "id": game_id,
                "date": date_iso,
                "home_team": home,
                "away_team": away,
                "prediction": prediction_json_str,
                "main_pick": ai_result.get("palpite_principal", "N/A"),
                "confidence": float(ai_result.get("confianca", 0.0)),
                "over_line": ai_result.get("linha_seguranca_over", ""),
                "under_line": ai_result.get("linha_seguranca_under", ""),
                "injury_alert": ai_result.get("alerta_lesao", "Não"),
                "key_factor": ai_result.get("keyFactor", "")
            })
        
        # Pausa termodinâmica para preservar limite de requisições
        time.sleep(1.5)

    if predictions:
        print(f"💾 Sincronizando {len(predictions)} predições com Supabase...")
        try:
            supabase.table("game_predictions").upsert(predictions).execute()
            print("✅ Sincronização concluída com sucesso.")
        except Exception as e:
            print(f"❌ Falha crítica ao persistir dados: {e}")
