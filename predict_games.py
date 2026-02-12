import os
import json
import time
import requests
from datetime import datetime
import pytz
from supabase import create_client
from groq import Groq

# --- CONFIGURAÇÃO ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

# Tenta carregar o arquivo gerado pelo script de lesões
INJURIES_FILE = "nba_injuries.json" 

if not all([SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY]):
    print("❌ Erro: Faltam variáveis de ambiente.")
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
                print(f"🚑 Carregadas {len(self.injuries)} lesões do arquivo local.")
            except Exception as e:
                print(f"⚠️ Erro ao ler arquivo de lesões: {e}")
        else:
            print("⚠️ Arquivo de lesões não encontrado. Análise será feita sem contexto médico.")

    def get_team_injuries_text(self, team_name):
        """Retorna texto formatado com as lesões principais do time"""
        if not self.injuries:
            return "Sem dados de lesão."

        # Filtra lesões do time (busca aproximada)
        team_injuries = [
            i for i in self.injuries 
            if team_name.lower() in i.get('team_name', '').lower() 
            or i.get('team_abbreviation') == team_name.split()[-1] # Tenta pegar pelo último nome (ex: Lakers)
        ]

        if not team_injuries:
            return "Nenhuma lesão reportada."

        report = []
        for inj in team_injuries:
            # Focamos apenas em jogadores que impactam (exclui G-League/Two-Way se quiser filtrar mais)
            status = inj.get('injury_status', 'Unknown')
            player = inj.get('player_name', 'Unknown')
            
            # Emoji based on status
            icon = "❌" if status == "Out" else "⚠️" if status in ["Day-To-Day", "Questionable"] else "ℹ️"
            
            report.append(f"{icon} {player} ({status})")
        
        return ", ".join(report) if report else "Time saudável."

# Instancia o monitor globalmente
injury_monitor = InjuryMonitor(INJURIES_FILE)

def get_nba_date():
    utc_now = datetime.now(pytz.utc)
    et_now = utc_now.astimezone(pytz.timezone('US/Eastern'))
    return et_now

def get_espn_games(date_obj):
    date_str = date_obj.strftime('%Y%m%d')
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}"
    
    print(f"📡 Consultando ESPN: {url}")
    try:
        res = requests.get(url, timeout=10)
        data = res.json()
        
        games_list = []
        for event in data.get('events', []):
            competition = event['competitions'][0]
            status = event.get('status', {}).get('type', {}).get('state', 'pre')

            if status == 'post':
                continue

            competitors = competition['competitors']
            home_team = next(t for t in competitors if t['homeAway'] == 'home')
            away_team = next(t for t in competitors if t['homeAway'] == 'away')
            
            games_list.append({
                'home': {
                    'name': home_team['team']['displayName'],
                    'record': home_team.get('records', [{'summary': '0-0'}])[0]['summary']
                },
                'away': {
                    'name': away_team['team']['displayName'],
                    'record': away_team.get('records', [{'summary': '0-0'}])[0]['summary']
                }
            })
        return games_list
    except Exception as e:
        print(f"❌ Erro na ESPN: {e}")
        return []

def get_team_stats(team_name):
    try:
        search_term = team_name.split(' ')[-1]
        res = supabase.table("classificacao_nba").select("*").ilike("time", f"%{search_term}%").execute()
        if res.data: return res.data[0]
        
        search_term_first = team_name.split(' ')[0]
        res_retry = supabase.table("classificacao_nba").select("*").ilike("time", f"%{search_term_first}%").execute()
        if res_retry.data: return res_retry.data[0]
            
    except Exception as e:
        print(f"⚠️ Erro ao buscar stats para {team_name}: {e}")
    return None

def analyze_game(game_data):
    home = game_data['home']
    away = game_data['away']
    print(f"🤖 Analisando {home['name']} vs {away['name']}...")
    
    home_stats = get_team_stats(home['name'])
    away_stats = get_team_stats(away['name'])

    # Busca Lesões
    home_injuries = injury_monitor.get_team_injuries_text(home['name'])
    away_injuries = injury_monitor.get_team_injuries_text(away['name'])

    prompt = f"""
    Aja como um analista 'Sharp' profissional de NBA (Vegas Style).
    Jogo: {home['name']} (Casa) vs {away['name']} (Fora).

    DADOS TÉCNICOS:
    - {home['name']}: Recorde {home['record']}, Streak: {home_stats.get('strk', 'N/A') if home_stats else 'N/A'}.
    - {away['name']}: Recorde {away['record']}, Streak: {away_stats.get('strk', 'N/A') if away_stats else 'N/A'}.

    🚨 RELATÓRIO DE LESÕES (CRÍTICO):
    - {home['name']}: {home_injuries}
    - {away['name']}: {away_injuries}

    SETUP DE ANÁLISE:
    1. IMPACTO DAS LESÕES: Se uma estrela (ex: LeBron, Curry, Giannis) estiver "Out", ignore o recorde do time e considere-o muito mais fraco. Se for "Day-To-Day", considere risco alto.
    2. FADIGA: Considere cansaço se houver indicação de B2B (Back to Back).
    3. MATCHUP: Defesa fraca contra ataque forte = Over. Jogo truncado = Under.
    4. HANDICAP: Busque valor no Underdog se o Favorito estiver desfalcado.

    Responda APENAS um JSON válido neste formato:
    {{
        "palpite_principal": "Ex: Lakers -5.0 ou Bulls +8.0",
        "confianca": "Alta/Média/Baixa",
        "fator_decisivo": "Sua explicação focada nas LESÕES e no matchup",
        "analise_curta": "Resumo de 1 frase",
        "linha_seguranca_over": "Ex: Over 215.5",
        "linha_seguranca_under": "Ex: Under 238.5",
        "alerta_lesao": "Sim/Não (Se houver estrela fora)"
    }}
    """
    try:
        chat = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model=MODEL_ID, temperature=0.2, response_format={"type": "json_object"}
        )
        return json.loads(chat.choices[0].message.content)
    except Exception as e:
        print(f"⚠️ Erro Groq: {e}")
        return None

def main():
    date_obj = get_nba_date()
    date_iso = date_obj.strftime('%Y-%m-%d')
    print(f"📅 Data NBA: {date_iso}")
    
    games = get_espn_games(date_obj)

    if not games:
        print("💤 Nenhum jogo futuro encontrado para hoje.")
        return

    predictions = []
    for game in games:
        home = game['home']['name']
        away = game['away']['name']
        game_id = f"{date_iso}_{home}_{away}".replace(" ", "")

        ai_result = analyze_game(game)
        if ai_result:
            prediction_json_str = json.dumps(ai_result, ensure_ascii=False)

            predictions.append({
                "id": game_id,
                "date": date_iso,
                "home_team": home,
                "away_team": away,
                "prediction": prediction_json_str,
                "main_pick": ai_result.get("palpite_principal"),
                "confidence": ai_result.get("confianca"),
                "over_line": ai_result.get("linha_seguranca_over"),
                "under_line": ai_result.get("linha_seguranca_under"),
                "injury_alert": ai_result.get("alerta_lesao", "Não") # Nova coluna
            })
        time.sleep(1.5) # Pausa leve para não estourar rate limit da Groq

    if predictions:
        print(f"💾 Salvando {len(predictions)} previsões...")
        try:
            # Upsert atualizado
            supabase.table("game_predictions").upsert(predictions).execute()
            print("✅ Sucesso!")
        except Exception as db_err:
            print(f"❌ Erro ao salvar no banco: {db_err}")

if __name__ == "__main__":
    main()
