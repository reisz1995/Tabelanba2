import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
const supabase = createClient(
  process.env.SUPABASE_URL,
  SUPABASE_SERVICE_KEY
);

// NOTA: Esta URL pode retornar 404 ou não conter os dados esperados.
// O endpoint oficial da ESPN para estatísticas de jogadores pode variar.
const PLAYERS_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/players";

async function run() {
  try {
    console.log("⏳ Buscando estatísticas dos jogadores (ESPN)...");
    const res = await fetch(PLAYERS_URL);

    if (!res.ok) {
      throw new Error(`Erro ao acessar API da ESPN: ${res.status} ${res.statusText}`);
    }

    const data = await res.json();

    if (!data.players) {
      console.error("⚠️ Estrutura de dados inesperada: 'players' não encontrado.", data);
      return;
    }

    const players = data.players.map(p => ({
      id: p.id,
      nome: p.fullName,
      time: p.team?.displayName,
      posicao: typeof p.position === 'object' ? p.position.displayName : p.position,
      pontos: p.statistics?.avgPoints ?? 0,
      rebotes: p.statistics?.avgRebounds ?? 0,
      assistencias: p.statistics?.avgAssists ?? 0,
    }));

    console.log(`📊 ${players.length} jogadores processados. Enviando para o Supabase...`);

    const { error } = await supabase.from("nba_jogadores_stats").upsert(players);

    if (error) {
      throw error;
    }

    console.log("✅ Importação concluída com sucesso!");
  } catch (err) {
    console.error("❌ Erro durante a importação:", err.message);
    process.exit(1);
  }
}

run();
