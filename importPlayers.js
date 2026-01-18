import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const PLAYERS_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/players";

async function run() {
  const res = await fetch(PLAYERS_URL);
  const data = await res.json();

  const players = data.players.map(p => ({
    id: p.id,
    nome: p.fullName,
    time: p.team?.displayName,
    posicao: p.position,
    pontos: p.statistics?.avgPoints ?? 0,
    rebotes: p.statistics?.avgRebounds ?? 0,
    assistencias: p.statistics?.avgAssists ?? 0,
  }));

  await supabase.from("nba_jogadores_stats").upsert(players);
  console.log("Importação concluída!");
}

run();
