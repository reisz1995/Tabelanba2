import { createClient } from '@supabase/supabase-js';
import ws from 'ws';

// ─── Configuração ────────────────────────────────────────────────────────────

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ESPN_BASE    = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba';
const RATE_LIMIT_MS = 800;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('❌ COLAPSO: Credenciais ausentes da matriz de ambiente.');
  process.exit(1);
}

// Fix Node 20: passa o transport ws explicitamente para o RealtimeClient
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  realtime: { transport: ws },
});

// ─── Helpers ─────────────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function extractScore(competitor) {
  const { score } = competitor;
  if (typeof score === 'object') return score?.value ?? 0;
  return parseInt(score, 10) || 0;
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} ao buscar: ${url}`);
  return res.json();
}

// ─── Lógica de negócio ───────────────────────────────────────────────────────

async function fetchLast5Games(teamId) {
  const data = await fetchJSON(`${ESPN_BASE}/teams/${teamId}/schedule`);

  const finishedGames = (data.events ?? [])
    .filter((e) => e.competitions[0].status.type.state === 'post')
    .sort((a, b) => new Date(a.date) - new Date(b.date));

  return finishedGames.slice(-5).map((game) => {
    const comp     = game.competitions[0];
    const mainTeam = comp.competitors.find((c) => c.id === teamId);
    const oppTeam  = comp.competitors.find((c) => c.id !== teamId);

    const mainScore = extractScore(mainTeam);
    const oppScore  = extractScore(oppTeam);
    const dt        = new Date(game.date);

    return {
      date:     `${String(dt.getDate()).padStart(2, '0')}/${String(dt.getMonth() + 1).padStart(2, '0')}`,
      opponent: oppTeam.team.abbreviation,
      result:   mainTeam.winner ? 'V' : 'D',
      score:    `${Math.max(mainScore, oppScore)}-${Math.min(mainScore, oppScore)}`,
    };
  });
}

async function upsertTeamRecord(fullName, last5) {
  const { data, error } = await supabase
    .from('teams')
    .update({ record: last5, updated_at: new Date().toISOString() })
    .eq('name', fullName)
    .select();

  if (error) {
    console.error(`❌ Fissura ao injetar ${fullName}: ${error.message}`);
    return false;
  }

  if (!data || data.length === 0) {
    console.warn(`⚠️  ${fullName}: Nenhuma linha encontrada para atualizar.`);
    return false;
  }

  console.log(`✅ ${fullName}: Matriz temporal atualizada.`);
  return true;
}

// ─── Entry point ─────────────────────────────────────────────────────────────

async function updateTeams() {
  console.log('🏀 Inicializando atualização de Momentum Global (Topologia JSON Rica)...\n');

  const espnData  = await fetchJSON(`${ESPN_BASE}/teams?limit=100`);
  const espnTeams = espnData.sports[0].leagues[0].teams;

  let successCount = 0;
  let errorCount   = 0;

  for (const item of espnTeams) {
    const { id: teamId, displayName: fullName } = item.team;

    try {
      const last5   = await fetchLast5Games(teamId);
      const success = await upsertTeamRecord(fullName, last5);

      if (success) successCount++;
      else errorCount++;
    } catch (err) {
      console.error(`❌ Erro ao processar ${fullName}: ${err.message}`);
      errorCount++;
    }

    // 🛡️ Dissipador térmico: evita rate limit da ESPN
    await sleep(RATE_LIMIT_MS);
  }

  console.log(`\n🏁 Ciclo encerrado: ${successCount} atualizados, ${errorCount} falhas.`);

  if (errorCount > 0) process.exit(1);
}

updateTeams().catch((err) => {
  console.error('❌ Colapso termodinâmico:', err);
  process.exit(1);
});
