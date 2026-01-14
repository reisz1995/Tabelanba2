import pandas as pd
from supabase import create_client
import os

# Configurações de acesso (Use variáveis de ambiente por segurança)
url_supabase = os.environ.get("SUPABASE_URL")
chave_supabase = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, chave_supabase)

def atualizar_dados_nba():
    # 1. Extração dos dados
    url_espn = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
    
    # O Pandas lê as tabelas. Geralmente a ESPN divide Nome do Time e Estatísticas
    tabelas = pd.read_html(url_espn)
    
    # Unindo as tabelas (Time + Dados)
    df_nomes = tabelas[0]
    df_stats = tabelas[1]
    df_final = pd.concat([df_nomes, df_stats], axis=1)

    # 2. Mapeamento das colunas (conforme o CSV que você enviou)
    df_final.columns = [
        'time_nome', 'vitorias', 'derrotas', 'pct_vitoria', 'jogos_atras', 
        'casa', 'visitante', 'divisao', 'conferencia', 'pontos_pro', 
        'pontos_contra', 'diferenca_pontos', 'sequencia', 'ultimos_10'
    ]

    # Limpeza rápida: Remover nomes de conferência extras se houver
    df_final['time_nome'] = df_final['time_nome'].str.replace(r'^[a-z]-', '', regex=True)

    # 3. Transformar em lista de dicionários para o Supabase
    dados_para_upsert = df_final.to_dict(orient='records')

    # 4. Enviar para o Supabase (Upsert baseado no time_nome)
    for dado in dados_para_upsert:
        supabase.table("classificacao_nba").upsert(dado, on_conflict="time_nome").execute()
    
    print("Banco de dados NBA atualizado com sucesso!")

if __name__ == "__main__":
    atualizar_dados_nba()
