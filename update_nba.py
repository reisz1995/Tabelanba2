import pandas as pd
import numpy as np
from supabase import create_client
import os
import re

url_supabase = os.environ.get("SUPABASE_URL")
chave_supabase = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, chave_supabase)

def limpar_nome_final(nome):
    if pd.isna(nome): return None
    # Remove rankings (1-, 2-)
    n = re.sub(r'^\d+-', '', str(nome))
    # Remove siglas grudadas de 2 ou 3 letras (OKCOklahoma -> Oklahoma)
    # Procuramos o padrão onde termina a sigla e começa o nome (Letra maiúscula seguida de minúscula)
    match = re.search(r'[A-Z]{2,3}([A-Z][a-z].*)', n)
    if match:
        return match.group(1).strip()
    return n.strip()

def atualizar_banco():
    url = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
    tabelas = pd.read_html(url)
    
    # Une Nomes (Tabela 0) e Dados (Tabela 1)
    df = pd.concat([tabelas[0], tabelas[1]], axis=1)
    df.columns = ['time_nome', 'vitorias', 'derrotas', 'pct_vitoria', 'jogos_atras', 
                  'casa', 'visitante', 'divisao', 'conferencia', 'pontos_pro', 
                  'pontos_contra', 'diferenca_pontos', 'sequencia', 'ultimos_10']

    # 1. Aplica limpeza de nome
    df['time_nome'] = df['time_nome'].apply(limpar_nome_final)

    # 2. FILTRO CRUCIAL: Mantém apenas linhas onde 'vitorias' é um número real
    # Isso elimina cabeçalhos de conferência e rodapés estranhos
    df = df[pd.to_numeric(df['vitorias'], errors='coerce').notnull()].copy()

    # 3. Conversão de tipos
    cols_num = ['vitorias', 'derrotas', 'pct_vitoria', 'pontos_pro', 'pontos_contra', 'diferenca_pontos']
    for col in cols_num:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace('+', ''), errors='coerce')

    # 4. Garante que temos apenas os 30 times da NBA
    df = df.head(30)

    # 5. Envio para o Supabase
    registros = df.replace({np.nan: None}).to_dict(orient='records')
    
    sucesso = 0
    for reg in registros:
        try:
            supabase.table("classificacao_nba").upsert(reg, on_conflict="time_nome").execute()
            sucesso += 1
        except Exception as e:
            print(f"Erro no time {reg['time_nome']}: {e}")

    print(f"Sucesso! {sucesso} times da NBA atualizados.")

if __name__ == "__main__":
    atualizar_banco()
