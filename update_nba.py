import pandas as pd
import numpy as np
from supabase import create_client
import os
import re

# Configuração Supabase
url_supabase = os.environ.get("SUPABASE_URL")
chave_supabase = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, chave_supabase)

def extrair_nome_limpo(nome_bruto):
    """Remove siglas (OKCOklahoma -> Oklahoma) e rankings (1-Boston -> Boston)"""
    if pd.isna(nome_bruto): return None
    texto = str(nome_bruto).strip()
    
    # 1. Remove ranking tipo "1-", "10-"
    texto = re.sub(r'^\d+-', '', texto)
    
    # 2. Se houver 2 ou 3 maiúsculas antes do nome (Ex: OKCOklahoma ou NYNew York)
    # Cortamos a sigla mantendo o início do nome real (Maiúscula+minúscula)
    match = re.search(r'^[A-Z]{2,3}([A-Z][a-z].*)', texto)
    if match:
        return match.group(1).strip()
    
    return texto

def atualizar_banco():
    url = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
    
    # Lendo as tabelas separadas da ESPN
    tabelas = pd.read_html(url)
    df_nomes = tabelas[0]
    df_stats = tabelas[1]
    
    # Unindo Horizontalmente
    df = pd.concat([df_nomes, df_stats], axis=1)
    
    # Definindo colunas padrão
    df.columns = [
        'time_nome', 'vitorias', 'derrotas', 'pct_vitoria', 'jogos_atras', 
        'casa', 'visitante', 'divisao', 'conferencia', 'pontos_pro', 
        'pontos_contra', 'diferenca_pontos', 'sequencia', 'ultimos_10'
    ]

    # --- LIMPEZA E FILTRO ---
    # 1. Limpa os nomes (Garante que o OKC não seja nulo)
    df['time_nome'] = df['time_nome'].apply(extrair_nome_limpo)

    # 2. FILTRO ESSENCIAL: Mantém apenas linhas onde 'vitorias' é número
    # Isso remove as linhas de título "Conferência Oeste/Este" automaticamente
    df = df[pd.to_numeric(df['vitorias'], errors='coerce').notnull()].copy()

    # 3. Conversão Numérica (limpa sinais de + e converte para float)
    cols_num = ['vitorias', 'derrotas', 'pct_vitoria', 'pontos_pro', 'pontos_contra', 'diferenca_pontos']
    for col in cols_num:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace('+', ''), errors='coerce')

    # 4. Forçamos o limite de 30 times (Total da NBA)
    df_final = df.head(30)

    # 5. Envio para o Supabase
    registros = df_final.replace({np.nan: None}).to_dict(orient='records')
    
    print(f"Iniciando envio de {len(registros)} times...")
    
    for item in registros:
        try:
            supabase.table("classificacao_nba").upsert(item, on_conflict="time_nome").execute()
        except Exception as e:
            print(f"Erro no time {item['time_nome']}: {e}")

    print("Concluído! Apenas os 30 times da NBA foram processados.")

if __name__ == "__main__":
    atualizar_banco()
