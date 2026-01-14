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
 * NBA API
 * ======================================================
 */
const NBA_API =
  "https://cdn.nba.com/static/json/liveData/standings/leagueStandings.json";

async function atualizarNBA() {
  console.log("⏳ Buscando classificação NBA...");

  const response = await fetch(NBA_API);
  if (!response.ok) {
    throw new Error("Erro ao acessar API da NBA");
  }

  const json = await response.json();

  const times = json.league.standard.teams;

  if (!times || times.length === 0) {
    throw new Error("Nenhum dado retornado pela API da NBA");
  }

  const dados = times.map((t) => ({
    time: t.teamName,
    vitorias: t.wins,
    derrotas: t.losses,
  }));

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

  console.log("🏀 Classificação NBA atualizada com sucesso");
}

atualizarNBA().catch((err) => {
  console.error("❌ Erro:", err.message);
  process.exit(1);
});
