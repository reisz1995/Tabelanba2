import pandas as pd
import numpy as np
from supabase import create_client
import os
import re

# Configurações de acesso
url_supabase = os.environ.get("SUPABASE_URL")
chave_supabase = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, chave_supabase)

def clean_numeric(x):
    """Remove caracteres especiais e converte para float, retorna None se falhar"""
    if pd.isna(x) or x == '-':
        return None
    try:
        # Remove '+' e converte para float
        return float(str(x).replace('+', ''))
    except:
        return None

def atualizar_dados_nba():
    url_espn = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
    
    # 1. Extração
    tabelas = pd.read_html(url_espn)
    df_nomes = tabelas[0]
    df_stats = tabelas[1]
    df_final = pd.concat([df_nomes, df_stats], axis=1)

    # 2. Renomear Colunas
    df_final.columns = [
        'time_nome', 'vitorias', 'derrotas', 'pct_vitoria', 'jogos_atras', 
        'casa', 'visitante', 'divisao', 'conferencia', 'pontos_pro', 
        'pontos_contra', 'diferenca_pontos', 'sequencia', 'ultimos_10'
    ]

    # 3. Limpeza de Dados (O segredo para evitar o erro de JSON)
    
    # Limpar nomes dos times (remove prefixos de classificação como '1-')
    df_final['time_nome'] = df_final['time_nome'].apply(lambda x: re.sub(r'^\d+-', '', str(x)))

    # Tratar colunas numéricas para garantir que não existam NaNs incompatíveis
    cols_numericas = ['vitorias', 'derrotas', 'pct_vitoria', 'pontos_pro', 'pontos_contra', 'diferenca_pontos']
    for col in cols_numericas:
        df_final[col] = df_final[col].apply(clean_numeric)

    # Converter todo o resto que for NaN para None (null no SQL)
    df_final = df_final.replace({np.nan: None})

    # 4. Enviar para o Supabase
    dados_para_upsert = df_final.to_dict(orient='records')

    for dado in dados_para_upsert:
        try:
            supabase.table("classificacao_nba").upsert(dado, on_conflict="time_nome").execute()
        except Exception as e:
            print(f"Erro ao inserir time {dado.get('time_nome')}: {e}")
    
    print("Processo concluído!")

if __name__ == "__main__":
    atualizar_dados_nba()
    
