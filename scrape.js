import { createClient } from "@supabase/supabase-js";

/**
 * SUPABASE
 */
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("❌ Variáveis do Supabase não encontradas");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

/**
 * ESPN NBA API
 */
const ESPN_API =
  "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/standings";

function safeNumber(v) {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

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
    ...(json.children?.[1]?.standings?.entries || []),
  ];

  if (!entries || entries.length === 0) {
    throw new Error("Nenhum dado retornado pela ESPN");
  }

  const dados = entries.map((e) => {
    const statsArray = Array.isArray(e.stats) ? e.stats : [];
    const stats = Object.fromEntries(
      statsArray.map((s) => [s.name, s.value])
    );

    const time = e.team?.displayName ?? null;
    const vitorias = safeNumber(stats.wins ?? stats.W ?? stats.WINS);
    const derrotas = safeNumber(stats.losses ?? stats.L ?? stats.LOSSES);
    const pts_pro = safeNumber(stats.pointsFor ?? stats.ptsFor ?? stats.pts);
    const pts_contra = safeNumber(
      stats.pointsAgainst ?? stats.ptsAgainst ?? stats.oppPts
    );

    // tenta ler streak ou últimas 10 — depende do payload
    const streak = stats.streak?.description ?? stats.streak || null;
    const ultimos_10 = stats.lastTen ? String(stats.lastTen) : null;

    // division / conference / home/away names: tentar extrair das propriedades do objeto
    const divisao =
      e.team?.division?.name ??
      e.division?.name ??
      e.group?.name ??
      null;
    const conferencia =
      e.team?.conference?.name ??
      e.conference?.name ??
      json.name ??
      null;

    // jogos_atras (games behind) pode vir como 'gb' ou 'gamesBehind'
    const jogos_atras = safeNumber(stats.gb ?? stats.gamesBehind);

    // pct_vitoria calculado se possível
    let pct_vitoria = null;
    if (vitorias !== null && derrotas !== null) {
      const total = vitorias + derrotas;
      pct_vitoria = total > 0 ? Number((vitorias / total).toFixed(3)) : null;
    } else if (safeNumber(stats["pct"])) {
      pct_vitoria = safeNumber(stats["pct"]);
    }

    const diferenca =
      pts_pro !== null && pts_contra !== null
        ? pts_pro - pts_contra
        : null;

    return {
      time,
      vitorias,
      derrotas,
      pct_vitoria,
      jogos_atras,
      casa: null, // a API de standings tipicamente não tem esta info por time; deixar null por enquanto
      visitante: null,
      divisao,
      conferencia,
      pts_pro,
      pts_contra,
      diferenca,
      streak,
      ultimos_10,
    };
  });

  console.log(`📊 ${dados.length} times encontrados`);

  // Upsert (idempotente) — assume que existe constraint UNIQUE(time)
  const { data, error: upsertError } = await supabase
    .from("classificacao_nba")
    .upsert(dados, { onConflict: "time" }); // ajuste onConflict se a PK/unique for diferente

  if (upsertError) {
    console.error("Erro no upsert:", upsertError);
    throw upsertError;
  }

  console.log("🏀 Classificação NBA atualizada com sucesso (ESPN)");
}

atualizarNBA().catch((err) => {
  console.error("❌ Erro:", err.message || err);
  process.exit(1);
});
