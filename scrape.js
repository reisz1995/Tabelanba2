import { createClient } from "@supabase/supabase-js";

/**
 * ======================================================
 * SUPABASE
 * ======================================================
 */
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("❌ Variáveis do Supabase não encontradas");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

/**
 * ======================================================
 * ESPN NBA API
 * ======================================================
 */
const ESPN_API =
  "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/standings";

async function atualizarNBA() {
  console.log("⏳ Buscando classificação NBA (ESPN)...");

  const response = await fetch(ESPN_API, {
    headers: {
      "User-Agent": "Mozilla/5.0",
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    console.error("Status HTTP:", response.status);
    throw new Error("Erro ao acessar API da ESPN");
  }

  const json = await response.json();

  const entries = [
  ...(json.children?.[0]?.standings?.entries || []),
  ...(json.children?.[1]?.standings?.entries || [])
];

  if (!entries || entries.length === 0) {
    throw new Error("Nenhum dado retornado pela ESPN");
  }

  const dados = entries.map((e) => {
  const stats = Object.fromEntries(
    e.stats.map((s) => [s.name, s.displayValue || s.value])
  );

return {
  time: e.team.displayName,
  v: stats.wins,
  d: stats.losses,
  pct_vit: stats.winPercent,
  ja: stats.gamesplayed,
  casa: stats.Home,
  visitante: stats.Road,
  div: stats.vsdiv,
  conf: stats.vsconf,

  pts: stats.pointsForPerGame || stats.avgPointsFor || stats.pointsFor,
  pts_contra: stats.pointsAgainstPerGame || stats.avgPointsAgainst || stats.pointsAgainst,

  dif: stats.pointDifferential,
  strk: stats.streak,
  u10: stats.L10,
};
      
});
  

  console.log(`📊 ${dados.length} times encontrados`);

  // Limpa tabela
  const { error: delError } = await supabase
    .from("classificacao_nba")
    .delete()
    .neq("id", 0);

  if (delError) throw delError;

  // Insere novos dados
  const { error: insError } = await supabase
    .from("classificacao_nba")
    .insert(dados);

  if (insError) throw insError;

  console.log("🏀 Classificação NBA atualizada com sucesso (ESPN)");
}

atualizarNBA().catch((err) => {
  console.error("❌ Erro:", err.message);
  process.exit(1);
});

