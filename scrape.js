import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import * as cheerio from "cheerio";

// -----------------------------------------------------------------------------
// [ HUD: INICIALIZAÇÃO DE PARÂMETROS GLOBAIS ]
// -----------------------------------------------------------------------------
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("❌ Erro Crítico: Matriz de variáveis do Supabase ausente.");
  process.exit(1);
}

// Supressão explícita de WebSockets e sessões para execução em CI/CD efêmero
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
  realtime: { enabled: false }
});

const ESPN_API = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/standings";
const HOLLINGER_URL = "https://www.espn.com/nba/hollinger/teamstats/_/sort/paceFactor";

// -----------------------------------------------------------------------------
// [ HUD: ROTINAS DE EXTRAÇÃO DE DADOS (SCRAPING) ]
// -----------------------------------------------------------------------------
const DISPLAY_TO_HOLLINGER = {
  "Los Angeles Lakers":    "LA Lakers",
  "Los Angeles Clippers":  "LA Clippers",
  "Golden State Warriors": "Golden State",
  "Oklahoma City Thunder": "Oklahoma City",
  "New Orleans Pelicans":  "New Orleans",
  "Portland Trail Blazers":"Portland",
  "San Antonio Spurs":     "San Antonio",
  "New York Knicks":       "New York",
  "Brooklyn Nets":         "Brooklyn",
  "Boston Celtics":        "Boston",
  "Houston Rockets":       "Houston",
  "Chicago Bulls":         "Chicago",
  "Cleveland Cavaliers":   "Cleveland",
  "Dallas Mavericks":      "Dallas",
  "Denver Nuggets":        "Denver",
  "Detroit Pistons":       "Detroit",
  "Indiana Pacers":        "Indiana",
  "Memphis Grizzlies":     "Memphis",
  "Miami Heat":            "Miami",
  "Milwaukee Bucks":       "Milwaukee",
  "Minnesota Timberwolves":"Minnesota",
  "Orlando Magic":         "Orlando",
  "Philadelphia 76ers":    "Philadelphia",
  "Phoenix Suns":          "Phoenix",
  "Sacramento Kings":      "Sacramento",
  "Toronto Raptors":       "Toronto",
  "Utah Jazz":             "Utah",
  "Washington Wizards":    "Washington",
  "Charlotte Hornets":     "Charlotte",
  "Atlanta Hawks":         "Atlanta",
};

/**
 * Busca o coeficiente de Pace de forma assíncrona.
 * @returns {Promise<Map<string, number>>}
 */
async function buscarPaceFactor() {
  console.log("⏳ Iniciando extração termal do Pace Factor (Hollinger)...");
  try {
    const res = await fetch(HOLLINGER_URL, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.espn.com/nba/",
      },
    });

    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const html = await res.text();
    const $ = cheerio.load(html);
    const paceMap = new Map();

    $("tr.oddrow, tr.evenrow").each((_, row) => {
      const $row = $(row);
      const nome = $row.find("td a").first().text().trim();
      const pace = parseFloat($row.find("td.sortcell").first().text().trim());

      if (nome && !isNaN(pace)) paceMap.set(nome, pace);
    });

    console.log(`🏃 Pace coletado com sucesso: ${paceMap.size} nós registrados.`);
    return paceMap;
  } catch (error) {
    console.warn(`⚠️ Anomalia no sub-sistema Hollinger: ${error.message}. Processando sem Pace.`);
    return new Map();
  }
}

/**
 * Resolução heurística e estrutural do identificador da entidade (Time).
 */
function resolverNome(displayName, paceMap) {
  const chavePrimaria = DISPLAY_TO_HOLLINGER[displayName];
  
  if (chavePrimaria && paceMap.has(chavePrimaria)) return chavePrimaria;

  const identificadorRadical = displayName.split(" ").pop();
  const chaveHeuristica = Array.from(paceMap.keys()).find(k => k.includes(identificadorRadical));
  
  return chaveHeuristica || displayName.split(" ")[0];
}

// -----------------------------------------------------------------------------
// [ HUD: ROTINA PRINCIPAL (CONTROLE DE FLUXO) ]
// -----------------------------------------------------------------------------
async function atualizarNBA() {
  console.log("⏳ Engajando rotinas de atualização da NBA...");

  // Execução paralela para eficiência máxima de I/O de rede
  const [espnRes, paceMap] = await Promise.all([
    fetch(ESPN_API, { headers: { "User-Agent": "Mozilla/5.0", "Accept": "application/json" } }),
    buscarPaceFactor()
  ]);

  if (!espnRes.ok) throw new Error(`Falha Crítica na ESPN API. Status HTTP: ${espnRes.status}`);

  const json = await espnRes.json();
  const entries = [
    ...(json.children?.[0]?.standings?.entries || []),
    ...(json.children?.[1]?.standings?.entries || []),
  ];

  if (!entries.length) throw new Error("A matriz de dados da ESPN retornou um vetor vazio.");

  // Transformação de dados
  const dados = entries.map((e) => {
    const stats = {};
    e.stats.forEach((s) => {
      if (s.name)         stats[s.name]         = s.displayValue || s.value;
      if (s.abbreviation) stats[s.abbreviation] = s.displayValue || s.value;
      if (s.type)         stats[s.type]         = s.displayValue || s.value;
    });
    
    const displayName   = e.team.displayName;
    const hollingerNome = resolverNome(displayName, paceMap);
    const pace          = paceMap.get(hollingerNome) ?? null;

    return {
      time:       displayName,
      v:          stats.wins,
      d:          stats.losses,
      pct_vit:    stats.winPercent,
      ja:         stats.gamesplayed || stats.GP || (Number(stats.wins) + Number(stats.losses)).toString(),
      casa:       stats.Home  || stats.home,
      visitante:  stats.Road  || stats.road,
      div:        stats.vsdiv || stats["vs. Div."] || stats.DIV,
      conf:       stats.vsconf || stats["vs. Conf."] || stats.CONF,
      pts:        stats.pointsForPerGame  || stats.avgPointsFor   || stats.pointsFor,
      pts_contra: stats.pointsAgainstPerGame || stats.avgPointsAgainst || stats.pointsAgainst,
      dif:        stats.pointDifferential,
      strk:       stats.streak,
      u10:        stats.L10 || stats.lasttengames || stats["Last Ten Games"],
      pace:       pace,
    };
  });

  console.log(`📊 Matriz processada: ${dados.length} blocos de entidade.`);

  // -----------------------------------------------------------------------------
  // [ HUD: PERSISTÊNCIA DE DADOS NO SUPABASE ]
  // -----------------------------------------------------------------------------
  console.log("🧹 Purgando configurações antigas da realidade (DELETE)...");
  const { error: deleteError } = await supabase.from("classificacao_nba").delete().neq("time", "");
  if (deleteError) console.error("❌ Erro de purga:", deleteError.message);

  console.log("📥 Gravando nova configuração na base (INSERT)...");
  const { error: insertError } = await supabase.from("classificacao_nba").insert(dados);
  if (insertError) throw insertError;

  console.log("🔄 Executando espelhamento do Pace Factor (RPC)...");
  const { error: rpcError } = await supabase.rpc('sincronizar_pace');
  if (rpcError) {
    console.error("❌ Falha crítica no espelhamento térmico:", rpcError.message);
  } else {
    console.log("✅ Pace Factor propagado com sucesso.");
  }

  // Validação final de integridade
  const semPace = dados.filter((d) => d.pace === null).map((d) => d.time);
  if (semPace.length > 0) {
    console.warn(`⚠️ Anomalia detectada: Falta de dados do Pace para [${semPace.join(", ")}]`);
  } else {
    console.log("🎯 Integridade 100%: Pace mapeado para todas as instâncias.");
  }

  console.log("✅ Ciclo de atualização finalizado sem anomalias persistentes.");
}

atualizarNBA().catch((err) => {
  console.error("❌ Abortando ciclo devido à falha fatal:", err.message);
  process.exit(1);
});
