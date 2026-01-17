import "dotenv/config";
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
    // Cria um objeto de estatísticas mais robusto, mapeando por nome, abreviação e tipo
    const stats = {};
    e.stats.forEach((s) => {
      if (s.name) stats[s.name] = s.displayValue || s.value;
      if (s.abbreviation) stats[s.abbreviation] = s.displayValue || s.value;
      if (s.type) stats[s.type] = s.displayValue || s.value;
    });

    return {
      time: e.team.displayName,
      v: stats.wins,
      d: stats.losses,
      pct_vit: stats.winPercent,
      ja: stats.gamesplayed || stats.GP || (Number(stats.wins) + Number(stats.losses)).toString(),
      casa: stats.Home || stats.home,
      visitante: stats.Road || stats.road,
      div: stats.vsdiv || stats["vs. Div."] || stats.DIV,
      conf: stats.vsconf || stats["vs. Conf."] || stats.CONF,

      pts: stats.pointsForPerGame || stats.avgPointsFor || stats.pointsFor,
      pts_contra: stats.pointsAgainstPerGame || stats.avgPointsAgainst || stats.pointsAgainst,

      dif: stats.pointDifferential,
      strk: stats.streak,
      u10: stats.L10 || stats.lasttengames || stats["Last Ten Games"],
    };
  });
  

  console.log(`📊 ${dados.length} times encontrados`);

  // Atualiza os dados usando upsert (mais seguro que delete + insert)
  // Nota: Para funcionar corretamente, a coluna 'time' deve ter uma restrição de unicidade no Supabase.
  const { error: upsertError } = await supabase
    .from("classificacao_nba")
    .upsert(dados, { onConflict: 'time' });

  if (upsertError) {
    console.error("❌ Erro ao atualizar dados (Upsert):", upsertError.message);
    throw upsertError;
  }

  console.log("🏀 Classificação NBA atualizada com sucesso (ESPN)");
}

atualizarNBA().catch((err) => {
  console.error("❌ Erro:", err.message);
  process.exit(1);
});

