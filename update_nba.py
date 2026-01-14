import pandas as pd
import numpy as np
from supabase import create_client
import os
import re

# Configurações de acesso
url_supabase = os.environ.get("SUPABASE_URL")
chave_supabase = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, chave_supabase)

def limpar_nome_time(nome_sujo):
    """
    Remove a sigla grudada no início (Ex: OKCOklahoma City Thunder -> Oklahoma City Thunder)
    A ESPN costuma colocar 2 ou 3 letras maiúsculas antes do nome real.
    """
    if pd.isna(nome_sujo): return None
    
    # 1. Remove números de posição (ex: 1-Boston -> Boston)
    nome = re.sub(r'^\d+-', '', str(nome_sujo))
    
    # 2. Lógica para nomes grudados (Ex: NYNew York ou OKCOklahoma)
    # Procuramos onde a sequência de maiúsculas termina e o nome começa.
    # Se o nome começa com a sigla (2 ou 3 letras), removemos.
    # Exceção: Se o nome for apenas a sigla (o que não ocorre na ESPN).
    
    # Remove prefixos de 2 ou 3 letras maiúsculas que precedem uma letra maiúscula seguida de minúscula
    # Ex: 'OKCOklahoma' -> 'Oklahoma', 'LALos Angeles' -> 'Los Angeles'
    limpo = re.sub(r'^[A-Z]{2,3}([A-Z][a-z])', r'\1', nome)
    
    return limpo.strip()

def atualizar_dados_nba():
    url_espn = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
    
    try:
        tabelas = pd.read_html(url_espn)
        df_nomes = tabelas[0]
        df_stats = tabelas[1]
        
        df_nomes.columns = ['time_nome']
        df_final = pd.concat([df_nomes, df_stats], axis=1)

        # Nomear conforme as colunas do seu Banco/CSV
        df_final.columns = [
            'time_nome', 'vitorias', 'derrotas', 'pct_vitoria', 'jogos_atras', 
            'casa', 'visitante', 'divisao', 'conferencia', 'pontos_pro', 
            'pontos_contra', 'diferenca_pontos', 'sequencia', 'ultimos_10'
        ]

        # Filtro para remover linhas de títulos ("Conferência Leste", etc)
        # Se 'vitorias' não for um número, a linha é descartada
        df_final = df_final[pd.to_numeric(df_final['vitorias'], errors='coerce').notnull()]

        # Limpar os nomes dos times (Garante que Oklahoma City apareça)
        df_final['time_nome'] = df_final['time_nome'].apply(limpar_nome_time)

        # Converter colunas numéricas (trata o erro de "7.0" enviando como float/numeric)
        cols_numericas = ['vitorias', 'derrotas', 'pct_vitoria', 'pontos_pro', 'pontos_contra', 'diferenca_pontos']
        for col in cols_numericas:
            df_final[col] = pd.to_numeric(df_final[col].astype(str).str.replace('+', ''), errors='coerce')

        # Substituir NaN por None (essencial para o JSON do Supabase)
        df_final = df_final.replace({np.nan: None})

        # Upsert no Supabase
        dados = df_final.to_dict(orient='records')
        for registro in dados:
            if registro['time_nome']:
                supabase.table("classificacao_nba").upsert(registro, on_conflict="time_nome").execute()
        
        print(f"Sucesso! {len(dados)} times atualizados.")

    except Exception as e:
        print(f"Erro crítico durante a execução: {e}")

if __name__ == "__main__":
    atualizar_dados_nba()
    
