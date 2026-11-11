-- ============================================================================
-- Migração: Limpeza de períodos legados em databallr_team_stats
-- Data: 2026-11-11
--
-- Contexto: corridas antigas do scraper gravaram snapshots sob períodos como
-- 'last_14_days' e 'last_30_days' (com semântica/rótulo desatualizados, ex.:
-- dados rotulados de 14 dias que na prática eram season-to-date). Essas linhas
-- duplicam times, trazem impactos Six Factors zerados e confundem o
-- consumidor (predict_games). A fonte de verdade única é period='this_year'
-- (season-to-date), gravada pelo scraper v6.1.
--
-- O scraper agora também executa essa limpeza a cada ciclo, mas rode este
-- script uma vez para limpar o acúmulo atual.
-- ============================================================================

DELETE FROM databallr_team_stats
WHERE period <> 'this_year';

-- Verificação (opcional):
-- SELECT period, count(*) FROM databallr_team_stats GROUP BY period;
-- Deve retornar apenas: this_year | 30
