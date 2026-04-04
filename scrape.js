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
 * ESPN NBA API - Classificação
 * ======================================================
 */
const ESPN_API =
  "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/standings";

/**
 * ======================================================
 * ESPN Hollinger Stats - Pace Factor
 * ======================================================
 */
const HOLLINGER_API =
  "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/hollinger/teamstats?sort=paceFactor";

/**
 * Busca o Pace Factor de cada time via Hollinger Stats da ESPN
 * Retorna um Map: { "Team Name" => paceValue }
 */
async function buscarPaceFactor() {
  console.log("⏳ Buscando Pace Factor (Hollinger Stats)...");

  const response = await fetch(HOLLINGER_API, {
    headers: {
      "User-Agent": "Mozilla/5.0",
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    console.warn(
      `⚠️ Hollinger API retornou status ${response.status}. Pace não será preenchido.`
    );
    return new Map();
  }

  const json = await response.json();

  // A estrutura típica da API do Hollinger é json.teams ou json.athletes
  // Pode variar; inspecionamos os dois caminhos mais comuns
  const teams =
    json.teams ||
    json.athletes ||
    json.entries ||
    [];

  if (!teams.length) {
    console.warn("⚠️ Nenhum dado de Pace encontrado na resposta Hollinger.");
    return new Map();
  }

  const paceMap = new Map();

  teams.forEach((t) => {
    // O nome do time pode estar em caminhos diferentes dependendo da resposta
    const nome =
      t.team?.displayName ||
      t.athlete?.displayName ||
      t.displayName ||
      null;

    // Procura a stat "paceFactor" dentro do array de stats
    const stats = t.stats || t.categories?.[0]?.stats || [];
    const paceStat = stats.find(
      (s) =>
        s.name === "paceFactor" ||
        s.abbreviation === "PACE" ||
        s.label === "Pace Factor"
    );

    const pace = paceStat
      ? parseFloat(paceStat.displayValue ?? paceStat.value)
      : null;

    if (nome && pace !== null) {
      paceMap.set(nome, pace);
    }
  });

  console.log(`🏃 Pace coletado para ${paceMap.size} times.`);
  return paceMap;
}

/**
 * ======================================================
 * MAIN
 * ======================================================
 */
async function atualizarNBA() {
  console.log("⏳ Buscando classificação NBA (ESPN)...");

  // ── 1. Classificação ──────────────────────────────────
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
    ...(json.children?.[1]?.standings?.entries || []),
  ];

  if (!entries || entries.length === 0) {
    throw new Error("Nenhum dado retornado pela ESPN");
  }

  // ── 2. Pace Factor ────────────────────────────────────
  const paceMap = await buscarPaceFactor();

  // ── 3. Merge dos dados ────────────────────────────────
  const dados = entries.map((e) => {
    const stats = {};
    e.stats.forEach((s) => {
      if (s.name) stats[s.name] = s.displayValue || s.value;
      if (s.abbreviation) stats[s.abbreviation] = s.displayValue || s.value;
      if (s.type) stats[s.type] = s.displayValue || s.value;
    });

    const nomeTime = e.team.displayName;

    return {
      time: nomeTime,
      v: stats.wins,
      d: stats.losses,
      pct_vit: stats.winPercent,
      ja:
        stats.gamesplayed ||
        stats.GP ||
        (Number(stats.wins) + Number(stats.losses)).toString(),
      casa: stats.Home || stats.home,
      visitante: stats.Road || stats.road,
      div: stats.vsdiv || stats["vs. Div."] || stats.DIV,
      conf: stats.vsconf || stats["vs. Conf."] || stats.CONF,
      pts: stats.pointsForPerGame || stats.avgPointsFor || stats.pointsFor,
      pts_contra:
        stats.pointsAgainstPerGame ||
        stats.avgPointsAgainst ||
        stats.pointsAgainst,
      dif: stats.pointDifferential,
      strk: stats.streak,
      u10: stats.L10 || stats.lasttengames || stats["Last Ten Games"],
      // ✅ Pace adicionado aqui
      pace: paceMap.get(nomeTime) ?? null,
    };
  });

  console.log(`📊 ${dados.length} times encontrados`);

  // ── 4. Upsert no Supabase ────────────────────────────
  console.log("🧹 Limpando dados antigos...");
  const { error: deleteError } = await supabase
    .from("classificacao_nba")
    .delete()
    .neq("time", "");

  if (deleteError) {
    console.error("❌ Erro ao limpar dados antigos:", deleteError.message);
  }

  console.log("📥 Inserindo novos dados...");
  const { error: insertError } = await supabase
    .from("classificacao_nba")
    .insert(dados);

  if (insertError) {
    console.error("❌ Erro ao inserir novos dados:", insertError.message);
    throw insertError;
  }

  console.log("🏀 Classificação NBA atualizada com sucesso (ESPN + Pace)!");

  // ── 5. Preview dos dados (debug) ──────────────────────
  const semPace = dados.filter((d) => d.pace === null).map((d) => d.time);
  if (semPace.length) {
    console.warn(`⚠️ Times sem Pace: ${semPace.join(", ")}`);
  }
}

atualizarNBA().catch((err) => {
  console.error("❌ Erro:", err.message);
  process.exit(1);
});
