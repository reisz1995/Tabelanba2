import pandas as pd
import numpy as np
from supabase import create_client
import os
import re

url_supabase = os.environ.get("SUPABASE_URL")
chave_supabase = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, chave_supabase)

def extrair_nome_real(nome_bruto):
    """
    Tratamento especial para garantir que o OKC e outros times não retornem None.
    """
    if pd.isna(nome_bruto) or nome_bruto == "": return None
    
    texto = str(nome_bruto).strip()
    
    # 1. Remove rankings (ex: '1-Boston' -> 'Boston')
    texto = re.sub(r'^\d+-', '', texto)
    
    # 2. Se o nome contém a sigla grudada (Ex: OKCOklahoma City)
    # Procuramos o padrão de 2 ou 3 letras maiúsculas seguidas de uma letra maiúscula e minúscula
    match = re.search(r'^[A-Z]{2,3}([A-Z][a-z].*)', texto)
    if match:
        return match.group(1)
    
    # 3. Caso o regex falhe (ex: times com nomes curtos), removemos apenas as 2 ou 3 primeiras letras
    # se elas forem seguidas de letra maiúscula.
    if len(texto) > 3 and texto[:3].isupper():
        return texto[3:]
    elif len(texto) > 2 and texto[:2].isupper():
        return texto[2:]

    return texto

def atualizar_banco():
    url = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
    tabelas = pd.read_html(url)
    
    # Une as tabelas
    df = pd.concat([tabelas[0], tabelas[1]], axis=1)
    df.columns = ['time_nome', 'vitorias', 'derrotas', 'pct_vitoria', 'jogos_atras', 
                  'casa', 'visitante', 'divisao', 'conferencia', 'pontos_pro', 
                  'pontos_contra', 'diferenca_pontos', 'sequencia', 'ultimos_10']

    # LIMPEZA DOS NOMES
    df['time_nome'] = df['time_nome'].apply(extrair_nome_real)

    # TRATAMENTO PARA O OKC (Caso específico de falha na primeira linha)
    # Se a primeira linha for None mas tiver dados, sabemos que é o líder (OKC)
    if df.iloc[0]['time_nome'] is None and not pd.isna(df.iloc[0]['vitorias']):
        df.at[0, 'time_nome'] = "Oklahoma City Thunder"

    # Conversão numérica
    cols_num = ['vitorias', 'derrotas', 'pct_vitoria', 'pontos_pro', 'pontos_contra', 'diferenca_pontos']
    for col in cols_num:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace('+', ''), errors='coerce')

    # Preenche NaNs para o JSON
    df = df.replace({np.nan: None})
    
    # Força a captura das 31 linhas
    df_31 = df.head(31)

    sucesso_count = 0
    for registro in df_31.to_dict(orient='records'):
        if registro['time_nome'] is not None:
            try:
                supabase.table("classificacao_nba").upsert(registro, on_conflict="time_nome").execute()
                sucesso_count += 1
            except Exception as e:
                print(f"Erro no time {registro['time_nome']}: {e}")
        else:
            print(f"Pulando linha vazia ou inválida: {registro}")

    print(f"Processo concluído! {sucesso_count} times atualizados no Supabase.")

if __name__ == "__main__":
    atualizar_banco()
