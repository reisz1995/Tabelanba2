import os
import json
from supabase import create_client, Client

def main():
    # Configurações do Supabase
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        print("Erro: SUPABASE_URL e SUPABASE_KEY devem ser configurados.")
        return

    supabase: Client = create_client(url, key)

    # Carregar dados do JSON
    try:
        with open("nba_injuries.json", "r", encoding="utf-8") as f:
            injuries = json.load(f)
    except FileNotFoundError:
        print("Erro: nba_injuries.json não encontrado.")
        return

    if not injuries:
        print("Nenhuma lesão encontrada no JSON.")
        return

    print(f"Enviando {len(injuries)} lesões para o Supabase...")

    # Preparar dados para o Supabase (garantir que os campos batem com o schema)
    # O schema gerado em nba_injuries_api.py usa os mesmos nomes de campos do JSON

    # Realizar o upsert
    # Nota: O schema define unique_player_injury UNIQUE (player_id, injury_date)
    # Usaremos player_id e injury_date como conflito para o upsert

    try:
        # Dividir em lotes para evitar problemas de limite de payload
        batch_size = 50
        for i in range(0, len(injuries), batch_size):
            batch = injuries[i:i + batch_size]
            response = supabase.table("nba_injured_players").upsert(
                batch,
                on_conflict="player_id,injury_date"
            ).execute()
            print(f"Lote {i//batch_size + 1} enviado com sucesso.")

        print("✅ Integração com Supabase concluída com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao enviar dados para o Supabase: {e}")

if __name__ == "__main__":
    main()
