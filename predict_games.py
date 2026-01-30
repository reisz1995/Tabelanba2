import os
import json
import time
import requests # Vamos usar requests direto para a ESPN
from datetime import datetime
import pytz
from supabase import create_client
from groq import Groq

# --- CONFIGURAÇÃO ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY]):
    print("❌ Erro: Faltam variáveis de ambiente.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
groq_client = Groq(api_key=GROQ_API_KEY)

MODEL_ID = "llama-3.3-70b-versatile"

def get_nba_date():
    """Retorna a data atual no horário de Nova York (NBA Time)"""
    utc_now = datetime.now(pytz.utc)
    et_now = utc_now.astimezone(pytz.timezone('US/Eastern'))
    return et_now

def get_espn_games(date_obj):
    """Busca jogos na API da ESPN (Muito mais estável que a da NBA)"""
    # Formato ESPN: YYYYMMDD
    date_str = date_obj.strftime('%Y%m%d')
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}"
    
    print(f"📡 Consultando ESPN: {url}")
    try:
        res = requests.get(url, timeout=10)
        data = res.json()
        
        games_list = []
        for event in data.get('events', []):
            competition = event['competitions'][0]
            competitors = competition['competitors']
            
            # ESPN coloca Home/Away em ordem variada, precisamos checar o atributo 'homeAway'
            home_team = next(t for t in competitors if t['homeAway'] == 'home')
            away_team = next(t for t in competitors if t['homeAway'] == 'away')
            
            games_list.append({
                'home': {
                    'name': home_team['team']['displayName'], # Ex: "Los Angeles Lakers"
                    'record': home_team.get('records', [{'summary': '0-0'}])[0]['summary']
                },
                'away': {
                    'name': away_team['team']['displayName'],
                    'record': away_team.get('records', [{'summary': '0-0'}])[0]['summary']
                }
            })
        return games_list
    except Exception as e:
        print(f"❌ Erro ao buscar na ESPN: {e}")
        return []

def get_team_stats(team_name):
    try:
        # Busca estatísticas do seu banco para dar contexto à IA
        # O ilike ajuda a casar "Lakers" com "Los Angeles Lakers"
        search_term = team_name.split(' ')[-1] # Pega só o último nome (Ex: Lakers)
        res = supabase.table("classificacao_nba").select("*").ilike("time", f"%{search_term}%").execute()
        if res.data and len(res.data) > 0:
            return res.data[0]
    except:
        pass
    return None

def analyze_game(game_data):
    home = game_data['home']
    away = game_data['away']
    
    print(f"🤖 Analisando {home['name']} vs {away['name']}...")

    home_stats = get_team_stats(home['name'])
    away_stats = get_team_stats(away['name'])

    prompt = f"""
    Aja como um analista 'Sharp' profissional da NBA.
    Jogo: {home['name']} (Casa) vs {away['name']} (Fora).
    
    Estatísticas {home['name']}: Recorde {home['record']}, Streak Atual: {home_stats.get('strk', 'N/A') if home_stats else 'N/A'}.
    Estatísticas {away['name']}: Recorde {away['record']}, Streak Atual: {away_stats.get('strk', 'N/A') if away_stats else 'N/A'}.

    Responda APENAS um JSON válido com esta estrutura exata:
    {{
        "palpite_principal": "Ex: Lakers -5.5",
        "confianca": "Alta/Média/Baixa",
        "fator_decisivo": "Frase curta sobre o motivo (ex: Lesão do Embiid)",
        "analise_curta": "Resumo de 2 linhas",
        "linha_seguranca_over": "Ex: Over 210.5",
        "linha_seguranca_under": "Ex: Under 240.5"
    }}
    """

    try:
        chat_completion = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model=MODEL_ID,
            temperature=0.3,
            response_format={"type": "json_object"}
        )
        return json.loads(chat_completion.choices[0].message.content)
    except Exception as e:
        print(f"⚠️ Erro na Groq: {e}")
        return None

def main():
    date_obj = get_nba_date()
    date_iso = date_obj.strftime('%Y-%m-%d')
    
    print(f"📅 Data NBA (US/Eastern): {date_iso}")
    
    # Busca jogos na ESPN (Sem bloqueios)
    games = get_espn_games(date_obj)

    if not games:
        print("💤 Nenhum jogo encontrado na ESPN para hoje.")
        return

    predictions = []

    for game in games:
        home_name = game['home']['name']
        away_name = game['away']['name']
        
        # ID único: YYYY-MM-DD_Home_Away
        game_id = f"{date_iso}_{home_name}_{away_name}".replace(" ", "")

        ai_result = analyze_game(game)

        if ai_result:
            prediction_json_str = json.dumps(ai_result)

            predictions.append({
                "id": game_id,
                "date": date_iso,
                "home_team": home_name,
                "away_team": away_name,
                "prediction": prediction_json_str 
            })
            
        time.sleep(1) # Pausa respeitosa para a Groq

    if predictions:
        print(f"💾 Salvando {len(predictions)} previsões...")
        try:
            # Upsert para atualizar se já existir
            data = supabase.table("game_predictions").upsert(predictions).execute()
            print("✅ Sucesso total!")
        except Exception as e:
            print(f"❌ Erro ao salvar no Supabase: {e}")

if __name__ == "__main__":
    main()

