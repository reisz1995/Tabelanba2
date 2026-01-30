import os
import json
from datetime import datetime
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

    # Prompt Otimizado com as novas diretrizes
    prompt = f"""
    Aja como um analista 'Sharp' da NBA. Analise: {home_team} (Casa) vs {away_team} (Fora).
    
    Dados {home_team}: {game['homeTeam']['wins']}-{game['homeTeam']['losses']}, Streak: {home_stats.get('strk', 'N/A') if home_stats else 'N/A'}.
    Dados {away_team}: {game['awayTeam']['wins']}-{game['awayTeam']['losses']}, Streak: {away_stats.get('strk', 'N/A') if away_stats else 'N/A'}.

    Gere um JSON estrito com estas chaves:
    {{
        "palpite_principal": "Ex: Lakers -5.5 ou Celtics ML",
        "confianca": "Alta/Média/Baixa",
        "fator_decisivo": "Uma frase curta explicando o motivo chave (ex: Lesão do Embiid, Matchup no garrafão)",
        "analise_curta": "Resumo de 2 linhas do jogo",
        "linha_seguranca_over": "Uma linha alternativa segura para Over (ex: Over 210.5)",
        "linha_seguranca_under": "Uma linha alternativa segura para Under (ex: Under 240.5)"
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
    print("🏀 Buscando jogos de hoje...")
    try:
        board = scoreboard.ScoreBoard()
        games = board.games.get_dict()
    except Exception as e:
        print(f"❌ Erro na API da NBA: {e}")
        return

    if not games:
        print("💤 Nenhum jogo hoje.")
        return

    predictions = []
    today_str = datetime.now().strftime('%Y-%m-%d')

    for game in games:
        home = game['homeTeam']['teamName']
        away = game['awayTeam']['teamName']
        game_id = f"{today_str}_{home}_{away}".replace(" ", "")

        ai_result = analyze_game(game)

        if ai_result:
            # Salvamos o JSON inteiro como string para o frontend processar
            prediction_json_str = json.dumps(ai_result)

            predictions.append({
                "id": game_id,
                "date": today_str,
                "home_team": home,
                "away_team": away,
                "prediction": prediction_json_str 
            })

    if predictions:
        print(f"💾 Salvando {len(predictions)} previsões...")
        try:
            data = supabase.table("game_predictions").upsert(predictions).execute()
            print("✅ Sucesso total!")
        except Exception as e:
            print(f"❌ Erro ao salvar no Supabase: {e}")

if __name__ == "__main__":
    main()
