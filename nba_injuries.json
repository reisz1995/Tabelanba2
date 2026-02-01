#!/usr/bin/env python3
"""
NBA Injuries Sync Script
Busca lesões na ESPN e sincroniza com o Supabase (Limpa antigos -> Insere novos)
"""

import os
import requests
import json
from datetime import datetime
from typing import List, Dict, Any
from supabase import create_client, Client

# --- CONFIGURAÇÃO ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY") # Use a Service Key para poder deletar sem restrições de RLS se necessário

if not all([SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Erro: Defina as variáveis de ambiente SUPABASE_URL e SUPABASE_SERVICE_KEY")
    exit(1)

# Inicializa o cliente Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
TABLE_NAME = "nba_injured_players"

class NBAInjuriesAPI:
    """Classe para buscar dados de lesões da NBA"""
    
    BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; NBA-Injuries-Bot/1.0)'
        })
    
    def get_all_teams(self) -> List[Dict[str, Any]]:
        """Busca lista de times"""
        url = f"{self.BASE_URL}/teams"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            teams = []
            for sport in data.get('sports', []):
                for league in sport.get('leagues', []):
                    for team_data in league.get('teams', []):
                        team = team_data.get('team', {})
                        teams.append({
                            'id': team.get('id'),
                            'name': team.get('displayName'),
                            'abbreviation': team.get('abbreviation')
                        })
            return teams
        except Exception as e:
            print(f"❌ Erro ao buscar times: {e}")
            return []

    def get_team_roster(self, team_id: str) -> List[Dict[str, Any]]:
        """Busca elenco do time"""
        url = f"{self.BASE_URL}/teams/{team_id}/roster"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json().get('athletes', [])
        except Exception:
            return []

    def extract_injury_data(self, athlete: Dict[str, Any], team_info: Dict[str, Any]) -> Dict[str, Any]:
        """Formata os dados para o padrão do banco"""
        injuries = athlete.get('injuries', [])
        if not injuries:
            return None
        
        injury = injuries[0] # Pega a lesão principal
        
        return {
            'player_id': athlete.get('id'),
            'player_name': athlete.get('displayName'),
            'player_short_name': athlete.get('shortName'),
            'team_id': team_info.get('id'),
            'team_name': team_info.get('name'),
            'team_abbreviation': team_info.get('abbreviation'),
            'position': athlete.get('position', {}).get('abbreviation'),
            'jersey_number': athlete.get('jersey'),
            'headshot_url': athlete.get('headshot', {}).get('href'),
            'injury_status': injury.get('status'),
            'injury_type': injury.get('type'),
            'injury_details': injury.get('details'), # Ex: "Out"
            'injury_description': injury.get('longComment'), # Descrição completa
            'injury_date': injury.get('date'),
            'last_updated': datetime.now().isoformat(),
            'espn_player_url': f"https://www.espn.com/nba/player/_/id/{athlete.get('id')}"
        }

    def fetch_all_injuries(self) -> List[Dict[str, Any]]:
        """Orquestra a busca de dados"""
        print("🏀 Iniciando varredura da NBA...")
        teams = self.get_all_teams()
        print(f"✅ {len(teams)} times detectados.")
        
        all_injuries = []
        
        for idx, team in enumerate(teams, 1):
            print(f"\r⏳ Processando {team['name']} ({idx}/{len(teams)})...", end="", flush=True)
            roster = self.get_team_roster(team['id'])
            
            for athlete in roster:
                injury_data = self.extract_injury_data(athlete, team)
                if injury_data:
                    all_injuries.append(injury_data)
        
        print("\n✅ Coleta finalizada.")
        return all_injuries

def update_supabase(data: List[Dict[str, Any]]):
    """
    1. Limpa a tabela inteira (apaga dados antigos)
    2. Insere os novos dados
    """
    if not data:
        print("⚠️ Nenhum dado de lesão encontrado. O banco não será alterado.")
        return

    print(f"\n🚀 Iniciando sincronização com Supabase ({len(data)} registros)...")

    try:
        # 1. LIMPEZA (DELETE ALL)
        # Usamos neq("id", 0) assumindo que IDs são positivos, ou seja, apaga tudo.
        print("🧹 Limpando dados antigos...")
        supabase.table(TABLE_NAME).delete().neq("player_id", "0").execute()
        
        # 2. INSERÇÃO (BULK INSERT)
        print("💾 Inserindo dados atualizados...")
        # O Supabase aceita insert em lote (lista de dicionários)
        response = supabase.table(TABLE_NAME).insert(data).execute()
        
        print("✅ Sucesso! Tabela atualizada.")
        
    except Exception as e:
        print(f"❌ Erro ao atualizar Supabase: {e}")

def main():
    api = NBAInjuriesAPI()
    
    # 1. Busca os dados frescos da API
    injuries_list = api.fetch_all_injuries()
    
    # 2. Exibe resumo no console
    print(f"\n📊 Total de lesionados: {len(injuries_list)}")
    
    # 3. Envia para o Supabase (Limpa Velhos -> Põe Novos)
    if injuries_list:
        update_supabase(injuries_list)
    else:
        print("Nenhuma lesão encontrada hoje.")

if __name__ == "__main__":
    main()
