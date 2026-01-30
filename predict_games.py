import os
import json
import time
from datetime import datetime
import pytz
from supabase import create_client
from groq import Groq
# MUDANÇA: Usamos a API de stats (que aceita datas) em vez da Live
from nba_api.stats.endpoints import scoreboardv2

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
    return et_now.strftime('%Y-%m-%d')

def get_team_stats(team_name):
    try:
        # Tenta buscar pelo nome. A API nova retorna 'Lakers', 'Celtics', etc.
        res = supabase.table("classificacao_nba").select("*").ilike("time", f"%{team_name}%").execute()
        if res.data and len(res.data) > 0:
            return res.data[0]
    except:
        pass
    return None

def analyze_game(home_data, away_data):
    home_team = home_data['name']
    away_team = away_data['name']
    
    print(f"🤖 Analisando {home_team} vs {away_team}...")

    home_stats = get_team_stats(home_team)
    away_stats = get_team_stats(away_team)

    prompt = f"""
    Aja como um analista 'Sharp' da NBA. Analise: {home_team} (Casa) vs {away_team} (Fora).
    
    Dados {home_team}: Recorde {home_data['record']}, Streak: {home_stats.get('strk', 'N/A') if home_stats else 'N/A'}.
    Dados {away_team}: Recorde {away_data['record']}, Streak: {away_stats.get('strk', 'N/A') if away_stats else 'N/A'}.

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
    nba_date = get_nba_date()
    print(f"📅 Buscando jogos para a data NBA (US/Eastern): {nba_date}")
    
    try:
        # MUDANÇA: Usamos ScoreboardV2 passando a data explícita
        board = scoreboardv2.ScoreboardV2(game_date=nba_date)
        
        # A API retorna Datasets. Precisamos do 'LineScore' para pegar nomes e records
        line_score = board.line_score.get_dict()
        headers = line_score['headers']
        rows = line_score['data']
        
        # Mapeando índices das colunas para facilitar
        idx_game_id = headers.index('GAME_ID')
        idx_team_name = headers.index('TEAM_NAME')
        idx_wins_losses = headers.index('TEAM_WINS_LOSSES')
        
        # Organizar jogos por ID
        games_map = {}
        
        for row in rows:
            game_id = row[idx_game_id]
            team_data = {
                'name': row[idx_team_name],
                'record': row[idx_wins_losses]
            }
            
            if game_id not in games_map:
                games_map[game_id] = []
            games_map[game_id].append(team_data)
            
    except Exception as e:
        print(f"❌ Erro na API da NBA: {e}")
        return

    if not games_map:
        print("💤 Nenhum jogo agendado para esta data.")
        return

    predictions = []

    for game_id, teams in games_map.items():
        if len(teams) != 2:
            continue # Ignora dados incompletos

        # Na API Stats, o time da casa geralmente é o segundo na lista, mas não é garantido.
        # Vamos assumir a ordem padrão ou que o script apenas compara Time A vs Time B.
        # Para ser preciso, a API ScoreboardV2 tem o GameHeader que diz quem é HOME_TEAM_ID,
        # mas para simplificar, vamos tratar team[1] como casa (padrão NBA API) ou apenas comparar.
        # No LineScore, geralmente o visitante vem primeiro.
        away_data = teams[0]
        home_data = teams[1]

        home_name = home_data['name']
        away_name = away_data['name']

        # ID único para o Supabase
        db_id = f"{nba_date}_{home_name}_{away_name}".replace(" ", "")

        ai_result = analyze_game(home_data, away_data)

        if ai_result:
            prediction_json_str = json.dumps(ai_result)

            predictions.append({
                "id": db_id,
                "date": nba_date,
                "home_team": home_name,
                "away_team": away_name,
                "prediction": prediction_json_str 
            })
            
        # Pequena pausa para não estourar rate limit da Groq se houver muitos jogos
        time.sleep(1) 

    if predictions:
        print(f"💾 Salvando {len(predictions)} previsões...")
        try:
            data = supabase.table("game_predictions").upsert(predictions).execute()
            print("✅ Sucesso total!")
        except Exception as e:
            print(f"❌ Erro ao salvar no Supabase: {e}")

if __name__ == "__main__":
    main()
