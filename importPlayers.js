import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
const supabase = createClient(
  process.env.SUPABASE_URL,
  SUPABASE_SERVICE_KEY
);

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Fetch error ${res.status} on ${url}`);
  return res.json();
}

async function run() {
  try {
    console.log("⏳ Iniciando importação de jogadores via Core API...");

    // 1. Obter temporada atual
    const leagueData = await fetchJson("https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba");
    const seasonUrl = leagueData.season["$ref"].split("?")[0];
    console.log(`🏀 Temporada atual: ${seasonUrl}`);

    // 2. Obter todos os times para cache de nomes
    console.log("🏙️ Buscando times...");
    const teamsData = await fetchJson(`${seasonUrl}/teams?limit=32`);
    const teamMap = {};
    await Promise.all(teamsData.items.map(async (item) => {
      const team = await fetchJson(item["$ref"]);
      teamMap[team.id] = team.displayName;
    }));
    console.log(`✅ ${Object.keys(teamMap).length} times carregados.`);

    // 3. Obter líderes de várias categorias para ter uma lista abrangente
    console.log("🏆 Buscando líderes de estatísticas...");
    const leadersData = await fetchJson(`${seasonUrl}/types/2/leaders?limit=100`);

    const categoriesToFetch = ["pointsPerGame", "reboundsPerGame", "assistsPerGame"];
    const athleteRefs = new Set();
    const leadersToProcess = [];

    leadersData.categories.forEach(cat => {
      if (categoriesToFetch.includes(cat.name)) {
        cat.leaders.forEach(l => {
          const ref = l.athlete["$ref"];
          if (!athleteRefs.has(ref)) {
            athleteRefs.add(ref);
            leadersToProcess.push(l);
          }
        });
      }
    });

    console.log(`📊 ${athleteRefs.size} jogadores únicos encontrados nas categorias principais.`);

    const players = [];

    // Para evitar sobrecarga, processamos sequencialmente
    for (const entry of leadersToProcess) {
      try {
        const athleteUrl = entry.athlete["$ref"];
        const statsUrl = entry.statistics["$ref"];
        const athlete = await fetchJson(athleteUrl);
        const statsData = await fetchJson(statsUrl);

        const teamId = athlete.team["$ref"].split("/").pop().split("?")[0];
        const teamName = teamMap[teamId] || "Desconhecido";

        const stats = {};
        statsData.splits.categories.forEach(cat => {
          cat.stats.forEach(s => {
            stats[s.name] = s.value;
          });
        });

        players.push({
          id: parseInt(athlete.id),
          nome: athlete.fullName,
          time: teamName,
          posicao: athlete.position?.displayName || athlete.position?.name || "N/A",
          pontos: stats.avgPoints || 0,
          rebotes: stats.avgRebounds || 0,
          assistencias: stats.avgAssists || 0,
        });

        console.log(`✅ ${athlete.fullName} processado.`);
      } catch (err) {
        console.error(`❌ Erro ao processar atleta: ${err.message}`);
      }
    }

    if (players.length === 0) {
      console.log("⚠️ Nenhum jogador processado.");
      return;
    }

    console.log(`📥 Enviando ${players.length} jogadores para o Supabase...`);
    const { error } = await supabase.from("nba_jogadores_stats").upsert(players);

    if (error) throw error;

    console.log("✅ Importação concluída com sucesso!");
  } catch (err) {
    console.error("❌ Erro fatal durante a importação:", err.message);
    process.exit(1);
  }
}

run();
