-- ============================================================================
-- Migração: Six Factors (Databallr) na tabela databallr_team_stats
-- Data: 2026-11-11
--
-- Contexto: o site Databallr calcula os "Six Factor Analysis" no cliente,
-- a partir do mesmo payload da API /v1/nba/teams/stats. O scraper v6.1 agora
-- replica esse cálculo e grava os impactos (pontos por 100 posses) em colunas
-- próprias. Rodar este script no SQL Editor do Supabase (service_role).
--
-- Semântica das colunas novas (positivo = desempenho favorável):
--   o_ts_impact  : impacto ofensivo da eficiência de arremessos  (= oTS do site)
--   o_tov_impact : impacto ofensivo do controle de turnovers     (= oTOV do site)
--   o_orb_impact : impacto ofensivo dos rebotes ofensivos        (= oORB do site)
--   d_ts_impact  : impacto defensivo sobre arremessos adversários (= dTS do site)
--   d_tov_impact : impacto defensivo de turnovers forçados       (= dTOV do site)
--   d_drb_impact : impacto defensivo dos rebotes defensivos      (= dDRB do site)
--
-- Redefinição de semântica (colunas já existentes):
--   net_eff  : ANTES = ORTG - DRTG  ->  AGORA = oTS + dTS  (NetEff do site)
--   net_poss : ANTES = OffPoss - DefPoss -> AGORA = oTOV + oORB + dTOV + dDRB
--              (NetPoss do site)
--   O rating líquido (ORTG - DRTG) continua disponível em net_rating.
--   Colunas cruas mantidas: o_ts (TS% x100), o_tov (TOV/100 posses),
--   orb (ORB% x100), drb (DRB% x100).
-- ============================================================================

ALTER TABLE databallr_team_stats
  ADD COLUMN IF NOT EXISTS o_ts_impact  DOUBLE PRECISION DEFAULT 0,
  ADD COLUMN IF NOT EXISTS o_tov_impact DOUBLE PRECISION DEFAULT 0,
  ADD COLUMN IF NOT EXISTS o_orb_impact DOUBLE PRECISION DEFAULT 0,
  ADD COLUMN IF NOT EXISTS d_ts_impact  DOUBLE PRECISION DEFAULT 0,
  ADD COLUMN IF NOT EXISTS d_tov_impact DOUBLE PRECISION DEFAULT 0,
  ADD COLUMN IF NOT EXISTS d_drb_impact DOUBLE PRECISION DEFAULT 0;

COMMENT ON COLUMN databallr_team_stats.o_ts_impact  IS 'Six Factor: impacto ofensivo da eficiencia de arremessos (pts/100 posses, = oTS do site)';
COMMENT ON COLUMN databallr_team_stats.o_tov_impact IS 'Six Factor: impacto ofensivo do controle de turnovers (pts/100 posses, = oTOV do site)';
COMMENT ON COLUMN databallr_team_stats.o_orb_impact IS 'Six Factor: impacto ofensivo dos rebotes ofensivos (pts/100 posses, = oORB do site)';
COMMENT ON COLUMN databallr_team_stats.d_ts_impact  IS 'Six Factor: impacto defensivo sobre arremessos adversarios (pts/100 posses, = dTS do site)';
COMMENT ON COLUMN databallr_team_stats.d_tov_impact IS 'Six Factor: impacto defensivo de turnovers forcados (pts/100 posses, = dTOV do site)';
COMMENT ON COLUMN databallr_team_stats.d_drb_impact IS 'Six Factor: impacto defensivo dos rebotes defensivos (pts/100 posses, = dDRB do site)';

COMMENT ON COLUMN databallr_team_stats.net_eff  IS 'NetEff do site = o_ts_impact + d_ts_impact bruto (impacto liquido de arremessos). Rating liquido fica em net_rating.';
COMMENT ON COLUMN databallr_team_stats.net_poss IS 'NetPoss do site = o_tov_impact + o_orb_impact + d_tov_impact bruto + d_drb_impact bruto (impacto liquido de posses/rebotes).';

-- Verificação (opcional):
-- SELECT column_name, data_type FROM information_schema.columns
--  WHERE table_name = 'databallr_team_stats'
--    AND column_name LIKE '%impact%';
