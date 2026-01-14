import pandas as pd
import numpy as np
from supabase import create_client
import os
import re

url_supabase = os.environ.get("SUPABASE_URL")
chave_supabase = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, chave_supabase)

def limpar_nome_time(nome_sujo):
    if pd.isna(nome_sujo): return None
    # Remove rankings tipo "1-", "2-"
    nome = re.sub(r'^\d+-', '', str(nome_sujo))
    
    # Lógica de extração: O nome do time na ESPN vem após a sigla de 2 ou 3 letras.
    # Exemplos: OKCOklahoma City Thunder, LALos Angeles Lakers, NYNew York Knicks.
    # Se encontrarmos 2 ou 3 letras maiúsculas seguidas de outra Maiúscula+minúscula, cortamos a sigla.
    res = re.sub(r'^[A-Z]{2,3}([A-Z][a-z])', r'\1', nome)
    return res.strip()

def atualizar_dados():
    url = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
    
    # Forçamos o pandas a ler todas as tabelas
    tabelas = pd.read_html(url)
    
    # A ESPN separa os nomes (tabela 0) das estatísticas (tabela 1)
    df_nomes = tabelas[0]
    df_stats = tabelas[1]
    
    # Unimos horizontalmente
    df = pd.concat([df_nomes, df_stats], axis=1)
    
    # Mapeamos as colunas exatamente como no seu banco
    df.columns = [
        'time_nome', 'vitorias', 'derrotas', 'pct_vitoria', 'jogos_atras', 
        'casa', 'visitante', 'divisao', 'conferencia', 'pontos_pro', 
        'pontos_contra', 'diferenca_pontos', 'sequencia', 'ultimos_10'
    ]

    # TRATAMENTO PARA NÃO PULAR O OKC:
    # Em vez de filtrar linhas agora, vamos primeiro limpar os nomes.
    df['time_nome'] = df['time_nome'].apply(limpar_nome_time)
    
    # Removemos apenas linhas onde o nome do time é claramente um cabeçalho de conferência
    # (Ex: "Conferência Leste", "ESTE", "OESTE")
    palavras_filtro = ['Conferência', 'ESTE', 'OESTE', 'CONF']
    df = df[~df['time_nome'].str.contains('|'.join(palavras_filtro), na=False)]

    # Converte colunas numéricas (trata o sinal de + e transforma string em número)
    cols_num = ['vitorias', 'derrotas', 'pct_vitoria', 'pontos_pro', 'pontos_contra', 'diferenca_pontos']
    for col in cols_num:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace('+', ''), errors='coerce')

    # Transforma NaNs em None (para o Supabase aceitar o JSON)
    df = df.replace({np.nan: None})

    # Pegamos as 31 primeiras linhas para garantir sua meta
    df_final = df.head(31)

    # Upsert no Supabase
    lista_dados = df_final.to_dict(orient='records')
    for item in lista_dados:
        try:
            # O upsert usa o time_nome para decidir se cria um novo ou atualiza o existente
            supabase.table("classificacao_nba").upsert(item, on_conflict="time_nome").execute()
        except Exception as e:
            print(f"Erro ao processar {item['time_nome']}: {e}")

    print(f"Sucesso! {len(lista_dados)} linhas processadas.")

if __name__ == "__main__":
    atualizar_dados()
    
