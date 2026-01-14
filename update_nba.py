import pandas as pd
import numpy as np
from supabase import create_client
import os
import re

url_supabase = os.environ.get("SUPABASE_URL")
chave_supabase = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, chave_supabase)

def limpar_nome_nba(nome_bruto):
    if pd.isna(nome_bruto): return "Desconhecido"
    nome = str(nome_bruto)
    
    # 1. Remove números de ranking (ex: 1-Boston -> Boston)
    nome = re.sub(r'^\d+-', '', nome)
    
    # 2. Lógica para OKC e outros:
    # Se o nome tem 3 maiúsculas seguidas de uma letra que inicia o nome real
    # Ex: OKCOklahoma -> Mantém Oklahoma
    # Usamos fatiamento: se as 3 primeiras são maiúsculas, testamos se o resto é o nome
    if len(nome) > 3 and nome[:3].isupper() and nome[3].isupper():
        return nome[3:]
    if len(nome) > 2 and nome[:2].isupper() and nome[2].isupper():
        return nome[2:]
        
    return nome.strip()

def atualizar_banco():
    url = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
    tabelas = pd.read_html(url)
    
    # Une as tabelas da ESPN
    df = pd.concat([tabelas[0], tabelas[1]], axis=1)
    
    # Nomeia as colunas
    df.columns = ['time_nome', 'vitorias', 'derrotas', 'pct_vitoria', 'jogos_atras', 
                  'casa', 'visitante', 'divisao', 'conferencia', 'pontos_pro', 
                  'pontos_contra', 'diferenca_pontos', 'sequencia', 'ultimos_10']

    # LIMPEZA PARA GARANTIR OS 31 REGISTROS
    # Em vez de deletar linhas estranhas, vamos apenas limpar os nomes
    df['time_nome'] = df['time_nome'].apply(limpar_nome_nba)
    
    # Converte tudo que for número, o que não for vira None (mas mantém a linha)
    cols_num = ['vitorias', 'derrotas', 'pct_vitoria', 'pontos_pro', 'pontos_contra', 'diferenca_pontos']
    for col in cols_num:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace('+', ''), errors='coerce')

    df = df.replace({np.nan: None})
    
    # Pega exatamente as primeiras 31 linhas encontradas no site
    df_31 = df.head(31)

    registros = df_31.to_dict(orient='records')
    for item in registros:
        try:
            supabase.table("classificacao_nba").upsert(item, on_conflict="time_nome").execute()
        except Exception as e:
            print(f"Erro no time {item['time_nome']}: {e}")

    print(f"Sucesso! {len(registros)} linhas enviadas para o Supabase.")

if __name__ == "__main__":
    atualizar_banco()
    
