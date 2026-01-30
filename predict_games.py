import os
import json
from datetime import datetime
from supabase import create_client
from groq import Groq
from nba_api.live.nba.endpoints import scoreboard

# --- CONFIGURAÇÃO ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY") # Use a Service Role para escrita!
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY]):
    print("❌ Erro: Faltam variáveis de ambiente (SUPABASE ou GROQ).")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
groq_client = Groq(api_key=GROQ_API_KEY)

def get_team_stats(team_name):
    """Busca stats do time na sua tabela classificacao_nba (já populada pelo scrape.js)"""
    try:
        # Tenta buscar pelo nome ou parte dele
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

    # Buscar dados do seu banco para dar contexto à IA
    home_stats = get_team_stats(home_team)
    away_stats = get_team_stats(away_team)

    # Montar o Prompt para a Groq
    prompt = f"""
    Aja como um analista profissional de apostas da NBA (Sharp).
    Analise o jogo de hoje: {home_team} (Casa) vs {away_team} (Visitante).
    
    Dados do {home_team}:
    - Recorde: {game['homeTeam']['wins']}-{game['homeTeam']['losses']}
    - Streak Atual: {home_stats.get('strk', 'N/A') if home_stats else 'N/A'}
    - Últimos 10: {home_stats.get('u10', 'N/A') if home_stats else 'N/A'}
    
    Dados do {away_team}:
    - Recorde: {game['awayTeam']['wins']}-{game['awayTeam']['losses']}
    - Streak Atual: {away_stats.get('strk', 'N/A') if away_stats else 'N/A'}
    - Últimos 10: {away_stats.get('u10', 'N/A') if away_stats else 'N/A'}

    Responda em PT-BR, curto e direto (máximo 5 linhas):
    1. alertar quando o jogo tende a ser under e qunado pode ser over.
    2. Quem é o favorito e por quê (fator chave)?
    3. Uma aposta sugerida (ex: Lakers -5.5 ou Over 220).
    4. Handicap (Vantagem/Desvantagem): É o mercado mais popular.
    Como há grandes discrepâncias técnicas, a casa dá uma vantagem de pontos ao azarão (Ex: +7.5) ou uma desvantagem ao favorito (Ex: -7.5).
    
    """

    try:
        chat_completion = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama3-70b-8192", # Modelo rápido e inteligente
            temperature=0.5,
        )
        return chat_completion.choices[0].message.content
    except Exception as e:
        print(f"Erro na Groq: {e}")
        return "Análise indisponível no momento."

def main():
    print("🏀 Buscando jogos de hoje...")
    board = scoreboard.ScoreBoard()
    games = board.games.get_dict()

    if not games:
        print("💤 Nenhum jogo agendado para hoje.")
        return

    predictions = []
    today_str = datetime.now().strftime('%Y-%m-%d')

    for game in games:
        home = game['homeTeam']['teamName']
        away = game['awayTeam']['teamName']
        game_id = f"{today_str}_{home}_{away}".replace(" ", "")

        analysis = analyze_game(game)

        predictions.append({
            "id": game_id,
            "date": today_str,
            "home_team": home,
            "away_team": away,
            "prediction_text": analysis
        })

    # Salvar no Supabase
    if predictions:
        print(f"💾 Salvando {len(predictions)} previsões...")
        data, count = supabase.table("game_predictions").upsert(predictions).execute()
        print("✅ Sucesso!")

if __name__ == "__main__":
    main()
