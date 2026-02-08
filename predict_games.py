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

if not all([SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY]):
    print("❌ Erro: Faltam variáveis de ambiente.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
groq_client = Groq(api_key=GROQ_API_KEY)
MODEL_ID = "llama-3.3-70b-versatile"

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
        # Tentativa 1: Busca pelo último nome (ex: "Lakers")
        search_term = team_name.split(' ')[-1]
        res = supabase.table("classificacao_nba").select("*").ilike("time", f"%{search_term}%").execute()
        
        if res.data: 
            return res.data[0]
        
        # Tentativa 2: Busca pelo primeiro nome (ex: "Portland" para Trail Blazers)
        search_term_first = team_name.split(' ')[0]
        res_retry = supabase.table("classificacao_nba").select("*").ilike("time", f"%{search_term_first}%").execute()
        
        if res_retry.data:
            return res_retry.data[0]
            
    except Exception as e:
        print(f"⚠️ Erro ao buscar stats para {team_name}: {e}")
    
    return None


def analyze_game(game_data):
    home = game_data['home']
    away = game_data['away']
    print(f"🤖 Analisando {home['name']} vs {away['name']}...")
    
    home_stats = get_team_stats(home['name'])
    away_stats = get_team_stats(away['name'])

    # --- INSTRUÇÕES ESTRATÉGICAS ADICIONADAS AQUI ---
    prompt = f"""
    Aja como um analista 'Sharp' profissional de NBA. 
    Jogo: {home['name']} (Casa) vs {away['name']} (Fora).

    DADOS DOS TIMES:
    - {home['name']}: Recorde {home['record']}, Streak Atual: {home_stats.get('strk', 'N/A') if home_stats else 'N/A'}.
    - {away['name']}: Recorde {away['record']}, Streak Atual: {away_stats.get('strk', 'N/A') if away_stats else 'N/A'}.

    SIGA RIGOROSAMENTE AS SEGUINTES REGRAS DE ANÁLISE (SETUP):
    1. DEFESA E PONTUAÇÃO:
       - Defesa Ruim = Tendência forte de OVER.
       - Para apostar em OVER, pergunte-se: "Os dois times têm estrelas para fazer +110 pontos cada?" Se não, cuidado.
    
    2. FATOR ESTRELA E CANSAÇO:
       - Se o melhor jogador do time não joga (considere conhecimento geral sobre lesões recentes), o jogo fica complicado/imprevisível.
       - Cansaço pode quebrar o favorito (atenção a Back-to-backs).
       - Cuidado com time que vem de derrota (podem vir mordidos para ganhar ou estar em crise).

    3. HANDICAPS (Obrigatório):
       - Times de forças iguais (jogo parelho) = PREFIRA Handicap Positivo (+).
       - Jogo difícil ou intermediário = SEMPRE Handicap Positivo (+).
       - REGRA DE OURO: Handicap +5.5 NÃO PRESTA (evite essa linha exata).
       - PREFERÊNCIA: Busque linhas próximas a +10 (underdog claro) ou -5 (favorito sólido).

    Responda APENAS um JSON válido com o seguinte formato:
    {{
        "palpite_principal": "Ex: Lakers -5.0 ou Heat +4.0",
        "confianca": "Alta/Média/Baixa",
        "fator_decisivo": "Explique usando as regras acima (ex: defesa ruim, cansaço, etc)",
        "analise_curta": "Resumo de 2 linhas focado no matchup",
        "linha_seguranca_over": "Ex: Over 210.5",
        "linha_seguranca_under": "Ex: Under 240.5"
    }}
    """
    try:
        chat = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model=MODEL_ID, temperature=0.3, response_format={"type": "json_object"}
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
            # 1. Correção dos acentos (ensure_ascii=False)
            prediction_json_str = json.dumps(ai_result, ensure_ascii=False)

            predictions.append({
                "id": game_id,
                "date": date_iso,
                "home_team": home,
                "away_team": away,
                "prediction": prediction_json_str,
                # 2. Preenchendo as novas colunas separadas
                "main_pick": ai_result.get("palpite_principal"),
                "confidence": ai_result.get("confianca"),
                "over_line": ai_result.get("linha_seguranca_over"),
                "under_line": ai_result.get("linha_seguranca_under")
            })
        time.sleep(1)

    if predictions:
        print(f"💾 Salvando {len(predictions)} previsões...")
        # Upsert vai atualizar as linhas existentes com as novas colunas
        supabase.table("game_predictions").upsert(predictions).execute()
        print("✅ Sucesso!")

if __name__ == "__main__":
    main()
