import { createClient } from "@supabase/supabase-js";

/**
 * ======================================================
 * SUPABASE
 * ======================================================
 */
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("âŒ VariÃ¡veis do Supabase nÃ£o encontradas");
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
  console.log("â³ Buscando classificaÃ§Ã£o NBA (ESPN)...");

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
      e.stats.map((s) => [s.name, s.value])
    );

    return {
      time: e.team.displayName,
      vitorias: stats.wins,
      derrotas: stats.losses,
      pts_pro: stats.ptsFor,
    };
  });

  console.log(`ðŸ“Š ${dados.length} times encontrados`);

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

  console.log("ðŸ€ ClassificaÃ§Ã£o NBA atualizada com sucesso (ESPN)");
}

atualizarNBA().catch((err) => {
  console.error("âŒ Erro:", err.message);
  process.exit(1);
});
      
