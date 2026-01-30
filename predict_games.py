import os
import json
from datetime import datetime
import pytz # Biblioteca de fuso horário
from supabase import create_client
from groq import Groq
from nba_api.live.nba.endpoints import scoreboard

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
    # Converte UTC para US/Eastern (Horário da NBA)
    et_now = utc_now.astimezone(pytz.timezone('US/Eastern'))
    return et_now.strftime('%Y-%m-%d')

def get_team_stats(team_name):
    try:
        res = supabase.table("classificacao_nba").select("*").ilike("time", f"%{team_name}%").execute()
        if res.data and len(res.data) > 0:
            return res.data[0]
    except:
        pass
    return None

def analyze_game(game):
    home_team = game['homeTeam']['teamName']
    away_team = game['awayTeam']['teamName']
    
    print(f"🤖 Analisando {home_team} vs {away_team}...")

    home_stats = get_team_stats(home_team)
    away_stats = get_team_stats(away_team)

    prompt = f"""
    Aja como um analista 'Sharp' da NBA. Analise: {home_team} (Casa) vs {away_team} (Fora).
    
    Dados {home_team}: {game['homeTeam']['wins']}-{game['homeTeam']['losses']}, Streak: {home_stats.get('strk', 'N/A') if home_stats else 'N/A'}.
    Dados {away_team}: {game['awayTeam']['wins']}-{game['awayTeam']['losses']}, Streak: {away_stats.get('strk', 'N/A') if away_stats else 'N/A'}.

    Gere um JSON estrito com estas chaves:
    {{
        "palpite_principal": "Ex: Lakers -5.5 ou Celtics ML",
        "confianca": "Alta/Média/Baixa",
        "fator_decisivo": "Uma frase curta explicando o motivo chave",
        "analise_curta": "Resumo de 2 linhas do jogo",
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
        print(f"⚠️ Erro na Groq ({home_team} vs {away_team}): {e}")
        return None

def main():
    # Pega a data correta da NBA (NY Time)
    nba_date = get_nba_date()
    print(f"📅 Data NBA (US/Eastern): {nba_date}")
    
    print("🏀 Buscando jogos...")
    try:
        # Scoreboard da Live API geralmente traz os jogos do dia corrente
        board = scoreboard.ScoreBoard()
        games = board.games.get_dict()
    except Exception as e:
        print(f"❌ Erro na API da NBA: {e}")
        return

    if not games:
        print("💤 Nenhum jogo encontrado na API Live.")
        return

    predictions = []

    for game in games:
        home = game['homeTeam']['teamName']
        away = game['awayTeam']['teamName']
        
        # Cria um ID único baseado na DATA CORRETA + Times
        game_id = f"{nba_date}_{home}_{away}".replace(" ", "")

        ai_result = analyze_game(game)

        if ai_result:
            prediction_json_str = json.dumps(ai_result)

            predictions.append({
                "id": game_id,
                "date": nba_date, # Salva com a data corrigida
                "home_team": home,
                "away_team": away,
                "prediction": prediction_json_str 
            })

    if predictions:
        print(f"💾 Salvando {len(predictions)} previsões para o dia {nba_date}...")
        try:
            data = supabase.table("game_predictions").upsert(predictions).execute()
            print("✅ Sucesso total!")
        except Exception as e:
            print(f"❌ Erro ao salvar no Supabase: {e}")
    else:
        print("⚠️ Nenhuma previsão gerada.")

if __name__ == "__main__":
    main()
