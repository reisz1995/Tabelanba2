import pandas as pd
import numpy as np
from supabase import create_client
import os
import re

# Configuração de Ambiente
url_supabase = os.environ.get("SUPABASE_URL")
chave_supabase = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, chave_supabase)

def tratar_nome_espn(nome_bruto):
    """
    Remove siglas grudadas (OKCOklahoma City -> Oklahoma City)
    e prefixos de posição (1-Boston -> Boston)
    """
    if pd.isna(nome_bruto): return None
    
    # 1. Limpa números de ranking (ex: 1-Boston ou 10-Miami)
    nome = re.sub(r'^\d+-', '', str(nome_bruto))
    
    # 2. Se o nome começa com 3 maiúsculas seguidas de uma Maiúscula+minúscula (Ex: OKCOklahoma)
    # Removemos as 3 primeiras. Se forem 2 (Ex: NYNew York), removemos 2.
    match_3 = re.match(r'^[A-Z]{3}([A-Z][a-z])', nome)
    if match_3:
        return nome[3:]
    
    match_2 = re.match(r'^[A-Z]{2}([A-Z][a-z])', nome)
    if match_2:
        return nome[2:]
        
    return nome.strip()

def atualizar_banco():
    url = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
    
    # Extrai tabelas (A ESPN separa nomes de estatísticas)
    tabelas = pd.read_html(url)
    df_nomes = tabelas[0]
    df_dados = tabelas[1]
    
    # Junta as duas partes
    df = pd.concat([df_nomes, df_dados], axis=1)
    
    # Nomeia colunas conforme seu padrão
    df.columns = [
        'time_nome', 'vitorias', 'derrotas', 'pct_vitoria', 'jogos_atras', 
        'casa', 'visitante', 'divisao', 'conferencia', 'pontos_pro', 
        'pontos_contra', 'diferenca_pontos', 'sequencia', 'ultimos_10'
    ]

    # LIMPEZA CRÍTICA:
    # 1. Remove linhas que são apenas títulos de conferência (onde vitórias não é número)
    df = df[pd.to_numeric(df['vitorias'], errors='coerce').notnull()].copy()
    
    # 2. Aplica o tratamento de nomes (Resolve Oklahoma e outros 30 times)
    df['time_nome'] = df['time_nome'].apply(tratar_nome_espn)
    
    # 3. Converte numéricos e trata sinais de '+'
    cols_num = ['vitorias', 'derrotas', 'pct_vitoria', 'pontos_pro', 'pontos_contra', 'diferenca_pontos']
    for col in cols_num:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace('+', ''), errors='coerce')
    
    # 4. Prepara para JSON (NaN vira None)
    df = df.replace({np.nan: None})
    
    # Envio para o Supabase
    registros = df.to_dict(orient='records')
    for item in registros:
        try:
            supabase.table("classificacao_nba").upsert(item, on_conflict="time_nome").execute()
        except Exception as e:
            print(f"Erro no time {item['time_nome']}: {e}")

    print(f"Processamento finalizado. {len(registros)} linhas processadas.")

if __name__ == "__main__":
    atualizar_banco()
    
