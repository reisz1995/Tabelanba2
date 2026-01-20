
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
