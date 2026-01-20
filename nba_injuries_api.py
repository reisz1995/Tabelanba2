#!/usr/bin/env python3
"""
NBA Injuries API Script
Extrai dados de lesões de jogadores da NBA usando a ESPN API
e prepara os dados para inserção no Supabase
"""

import requests
import json
from datetime import datetime
from typing import List, Dict, Any

class NBAInjuriesAPI:
    """Classe para buscar dados de lesões da NBA"""
    
    BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; NBA-Injuries-API/1.0)'
        })
    
    def get_all_teams(self) -> List[Dict[str, Any]]:
        """Busca todos os times da NBA"""
        url = f"{self.BASE_URL}/teams"
        response = self.session.get(url)
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
                        'abbreviation': team.get('abbreviation'),
                        'logo': team.get('logos', [{}])[0].get('href') if team.get('logos') else None
                    })
        
        return teams
    
    def get_team_roster(self, team_id: str) -> List[Dict[str, Any]]:
        """Busca o roster de um time específico"""
        url = f"{self.BASE_URL}/teams/{team_id}/roster"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            return data.get('athletes', [])
        except Exception as e:
            print(f"Erro ao buscar roster do time {team_id}: {e}")
            return []
    
    def extract_injury_data(self, athlete: Dict[str, Any], team_info: Dict[str, Any]) -> Dict[str, Any]:
        """Extrai dados de lesão de um atleta"""
        injuries = athlete.get('injuries', [])
        
        if not injuries:
            return None
        
        # Pega a lesão mais recente
        injury = injuries[0] if injuries else {}
        
        return {
            'player_id': athlete.get('id'),
            'player_name': athlete.get('displayName'),
            'player_short_name': athlete.get('shortName'),
            'team_id': team_info.get('id'),
            'team_name': team_info.get('name'),
            'team_abbreviation': team_info.get('abbreviation'),
            'position': athlete.get('position', {}).get('abbreviation'),
            'position_full': athlete.get('position', {}).get('displayName'),
            'jersey_number': athlete.get('jersey'),
            'headshot_url': athlete.get('headshot', {}).get('href'),
            'injury_status': injury.get('status'),
            'injury_type': injury.get('type'),
            'injury_details': injury.get('details'),
            'injury_description': injury.get('longComment'),
            'injury_date': injury.get('date'),
            'last_updated': datetime.now().isoformat(),
            'espn_player_url': f"https://www.espn.com/nba/player/_/id/{athlete.get('id')}",
        }
    
    def get_all_injuries(self) -> List[Dict[str, Any]]:
        """Busca todas as lesões da NBA"""
        print("🏀 Buscando todos os times da NBA...")
        teams = self.get_all_teams()
        print(f"✅ {len(teams)} times encontrados\n")
        
        all_injuries = []
        
        for idx, team in enumerate(teams, 1):
            print(f"[{idx}/{len(teams)}] Processando {team['name']}...")
            
            roster = self.get_team_roster(team['id'])
            
            injured_count = 0
            for athlete in roster:
                injury_data = self.extract_injury_data(athlete, team)
                if injury_data:
                    all_injuries.append(injury_data)
                    injured_count += 1
            
            if injured_count > 0:
                print(f"  ⚠️  {injured_count} jogador(es) lesionado(s)")
            else:
                print(f"  ✓ Nenhum jogador lesionado")
        
        return all_injuries
    
    def save_to_json(self, injuries: List[Dict[str, Any]], filename: str = "nba_injuries.json"):
        """Salva os dados em JSON"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(injuries, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Dados salvos em: {filename}")
    
    def generate_supabase_schema(self) -> str:
        """Gera o schema SQL para criar a tabela no Supabase"""
        return """
-- Tabela de Jogadores Lesionados da NBA
CREATE TABLE IF NOT EXISTS nba_injured_players (
  id BIGSERIAL PRIMARY KEY,
  player_id VARCHAR(50) NOT NULL,
  player_name VARCHAR(255) NOT NULL,
  player_short_name VARCHAR(100),
  team_id VARCHAR(50),
  team_name VARCHAR(255),
  team_abbreviation VARCHAR(10),
  position VARCHAR(10),
  position_full VARCHAR(50),
  jersey_number VARCHAR(10),
  headshot_url TEXT,
  injury_status VARCHAR(50),
  injury_type VARCHAR(100),
  injury_details TEXT,
  injury_description TEXT,
  injury_date TIMESTAMP,
  last_updated TIMESTAMP DEFAULT NOW(),
  espn_player_url TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  
  -- Índices para melhor performance
  CONSTRAINT unique_player_injury UNIQUE (player_id, injury_date)
);

-- Índices
CREATE INDEX idx_team_id ON nba_injured_players(team_id);
CREATE INDEX idx_injury_status ON nba_injured_players(injury_status);
CREATE INDEX idx_last_updated ON nba_injured_players(last_updated);

-- Comentários
COMMENT ON TABLE nba_injured_players IS 'Jogadores da NBA atualmente lesionados ou fora de jogo';
COMMENT ON COLUMN nba_injured_players.player_id IS 'ID do jogador na ESPN';
COMMENT ON COLUMN nba_injured_players.injury_status IS 'Status da lesão (Out, Day-To-Day, Questionable, etc)';
COMMENT ON COLUMN nba_injured_players.last_updated IS 'Data da última atualização dos dados';
"""
    
    def save_supabase_schema(self, filename: str = "supabase_schema.sql"):
        """Salva o schema SQL em arquivo"""
        schema = self.generate_supabase_schema()
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(schema)
        print(f"📋 Schema SQL salvo em: {filename}")


def main():
    """Função principal"""
    print("=" * 60)
    print("NBA INJURIES API - EXTRAÇÃO DE DADOS DE LESÕES")
    print("=" * 60)
    print()
    
    api = NBAInjuriesAPI()
    
    # Busca todos os jogadores lesionados
    injuries = api.get_all_injuries()
    
    # Resultados
    print("\n" + "=" * 60)
    print(f"📊 RESUMO: {len(injuries)} jogador(es) lesionado(s) encontrado(s)")
    print("=" * 60)
    
    if injuries:
        print("\n🏥 JOGADORES LESIONADOS:")
        print("-" * 60)
        for injury in injuries:
            status = injury.get('injury_status', 'N/A')
            player = injury.get('player_name', 'N/A')
            team = injury.get('team_abbreviation', 'N/A')
            details = injury.get('injury_details', 'N/A')
            print(f"  • {player} ({team}) - {status}")
            if details != 'N/A':
                print(f"    ↳ {details}")
    
    # Salva os dados
    api.save_to_json(injuries, "nba_injuries.json")
    api.save_supabase_schema("supabase_schema.sql")
    
    print("\n✅ Processo concluído!")
    print("\n📝 PRÓXIMOS PASSOS:")
    print("  1. Use o arquivo 'supabase_schema.sql' para criar a tabela no Supabase")
    print("  2. Use o arquivo 'nba_injuries.json' para inserir os dados")
    print("  3. Configure um cron job para atualizar os dados periodicamente")
    

if __name__ == "__main__":
    main()
