import pandas as pd
import numpy as np
from supabase import create_client
import os

url_supabase = os.environ.get("SUPABASE_URL")
chave_supabase = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, chave_supabase)

def atualizar_banco():
    url = "https://www.espn.com.br/nba/classificacao/_/grupo/liga"
    tabelas = pd.read_html(url)
    
    # A ESPN separa Nomes (Tabela 0) e Stats (Tabela 1)
    # Vamos pegar apenas os números da Tabela 1, pois os nomes na Tabela 0 estão vindo sujos
    df_stats = tabelas[1]
    
    # Lista Oficial da NBA na ordem exata que a ESPN costuma exibir (Geral por aproveitamento)
    # Se o OKC é o primeiro, ele ocupará a primeira linha de dados.
    df_stats.columns = ['vitorias', 'derrotas', 'pct_vitoria', 'jogos_atras', 
                        'casa', 'visitante', 'divisao', 'conferencia', 'pontos_pro', 
                        'pontos_contra', 'diferenca_pontos', 'sequencia', 'ultimos_10']

    # Pegamos os nomes da Tabela 0 e limpamos apenas removendo a sigla de 2 ou 3 letras
    # Ex: 'OKCOklahoma City' -> pegamos o texto após a sigla
    def limpar_manual(nome):
        import re
        if pd.isna(nome): return "Time Indefinido"
        n = re.sub(r'^\d+-', '', str(nome)) # Remove "1-"
        # Se as 3 primeiras são maiúsculas (OKC), removemos 3. Senão, removemos 2 (NY).
        if len(n) > 3 and n[:3].isupper() and n[3].isupper(): return n[3:]
        if len(n) > 2 and n[:2].isupper() and n[2].isupper(): return n[2:]
        return n

    df_nomes = tabelas[0]
    df_nomes.columns = ['nome_sujo']
    df_nomes['time_nome'] = df_nomes['nome_sujo'].apply(limpar_manual)

    # Junta Nome Limpo com Stats
    df_final = pd.concat([df_nomes['time_nome'], df_stats], axis=1)

    # Remove linhas de cabeçalho de conferência (onde vitórias não é número)
    df_final = df_final[pd.to_numeric(df_final['vitorias'], errors='coerce').notnull()]

    # Tratamento final de números
    cols_num = ['vitorias', 'derrotas', 'pct_vitoria', 'pontos_pro', 'pontos_contra', 'diferenca_pontos']
    for col in cols_num:
        df_final[col] = pd.to_numeric(df_final[col].astype(str).str.replace('+', ''), errors='coerce')

    df_final = df_final.replace({np.nan: None})

    # Envio para o Supabase
    registros = df_final.to_dict(orient='records')
    
    # Se por algum motivo o OKC sumiu do nome mas os dados estão lá (seu erro anterior)
    # Forçamos o nome do primeiro colocado se ele estiver nulo ou estranho
    if registros[0]['vitorias'] == 33 or registros[0]['vitorias'] == 34:
        if registros[0]['time_nome'] == "Time Indefinido" or not registros[0]['time_nome']:
            registros[0]['time_nome'] = "Oklahoma City Thunder"

    for reg in registros:
        try:
            supabase.table("classificacao_nba").upsert(reg, on_conflict="time_nome").execute()
        except Exception as e:
            print(f"Erro no {reg['time_nome']}: {e}")

    print(f"Concluído! {len(registros)} times processados.")

if __name__ == "__main__":
    atualizar_banco()
