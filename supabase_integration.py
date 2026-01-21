import os
import json
from supabase import create_client, Client

def main():
    print("🚀 Iniciando integração com Supabase...")

    # Configurações do Supabase - Tenta vários nomes de variáveis de ambiente
    url = os.environ.get("SUPABASE_URL")
    key = (os.environ.get("SUPABASE_SERVICE_KEY") or
           os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or
           os.environ.get("SUPABASE_KEY"))

    if not url:
        print("❌ Erro: SUPABASE_URL não configurada.")
        return
    if not key:
        print("❌ Erro: Chave do Supabase (SUPABASE_SERVICE_KEY, etc) não configurada.")
        return

    print(f"🔗 Conectando ao Supabase em: {url}")
    try:
        supabase: Client = create_client(url, key)
    except Exception as e:
        print(f"❌ Erro ao criar cliente Supabase: {e}")
        return

    # Carregar dados do JSON
    json_file = "nba_injuries.json"
    print(f"📂 Carregando dados de: {json_file}")
    try:
        if not os.path.exists(json_file):
            print(f"❌ Erro: Arquivo {json_file} não encontrado no diretório atual ({os.getcwd()}).")
            return

        with open(json_file, "r", encoding="utf-8") as f:
            injuries = json.load(f)
    except Exception as e:
        print(f"❌ Erro ao ler {json_file}: {e}")
        return

    if not injuries:
        print("⚠️ Aviso: Nenhuma lesão encontrada no JSON. O arquivo está vazio ou contém uma lista vazia.")
        return

    print(f"📊 {len(injuries)} lesões carregadas do arquivo local.")

    # Realizar o upsert
    # A tabela 'nba_injured_players' deve ter sido criada usando o script 'supabase_schema.sql'
    table_name = "nba_injured_players"

    try:
        print(f"📥 Enviando dados para a tabela '{table_name}'...")

        # Dividir em lotes para evitar problemas de limite de payload
        batch_size = 50
        total_inserted = 0

        for i in range(0, len(injuries), batch_size):
            batch = injuries[i:i + batch_size]

            # Nota: O upsert depende da constraint unique_player_injury UNIQUE (player_id, injury_date)
            # definida no schema.
            response = supabase.table(table_name).upsert(
                batch,
                on_conflict="player_id,injury_date"
            ).execute()

            batch_count = len(batch)
            total_inserted += batch_count
            print(f"✅ Lote {i//batch_size + 1} ({batch_count} registros) enviado.")

        print(f"\n✨ Sucesso! Total de {total_inserted} registros processados no Supabase.")

    except Exception as e:
        print(f"❌ Erro fatal durante a inserção no Supabase: {e}")
        if "row-level security" in str(e).lower():
            print("\n💡 Dica de RLS (Row-Level Security):")
            print("   O erro indica que o script não tem permissão para inserir dados.")
            print("   Para resolver, certifique-se de:")
            print("   1. Usar a 'service_role key' (Secret Key) do Supabase, não a 'anon key'.")
            print("   2. Ou executar o SQL das políticas de segurança (Policies) no SQL Editor do Supabase.")
        else:
            print("\nDica: Verifique se a tabela 'nba_injured_players' foi criada corretamente no seu banco de dados Supabase.")

if __name__ == "__main__":
    main()
