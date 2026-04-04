import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import * as cheerio from "cheerio";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("❌ Variáveis do Supabase não encontradas");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const ESPN_API =
  "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/standings";

const HOLLINGER_URL =
  "https://www.espn.com/nba/hollinger/teamstats/_/sort/paceFactor";

/**
 * Scraping do HTML da tabela Hollinger.
 * Estrutura real da página:
 *   <a href="...espn.com/nba/team...">NOME</a></td>
 *   <td class="sortcell">PACE</td>
 *
 * Retorna Map<nomeHollinger, pace>
 */
async function buscarPaceFactor() {
  console.log("⏳ Buscando Pace Factor (Hollinger HTML)...");

  const res = await fetch(HOLLINGER_URL, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      Referer: "https://www.espn.com/nba/",
    },
  });

  if (!res.ok) {
    console.warn(`⚠️ Hollinger retornou HTTP ${res.status}. Pace não preenchido.`);
    return new Map();
  }

  const html = await res.text();
  const $ = cheerio.load(html);
  const paceMap = new Map();

  // Cada linha tem classe "oddrow" ou "evenrow"
  $("tr.oddrow, tr.evenrow").each((_, row) => {
    const $row = $(row);
    const nome = $row.find("td a").first().text().trim();
    const paceStr = $row.find("td.sortcell").first().text().trim();
    const pace = parseFloat(paceStr);

    if (nome && !isNaN(pace)) {
      paceMap.set(nome, pace);
    }
  });

  console.log(`🏃 Pace coletado para ${paceMap.size} times.`);

  if (paceMap.size > 0) {
    console.log("📋 Amostra:");
    let i = 0;
    for (const [k, v] of paceMap) {
      console.log(`   "${k}" → ${v}`);
      if (++i >= 5) break;
    }
  }

  return paceMap;
}

/**
 * Mapeia o displayName da ESPN Standings API para o nome
 * curto usado na tabela Hollinger.
 */
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


function resolverNome(displayName, paceMap) {
  const chavePrimaria = DISPLAY_TO_HOLLINGER[displayName];
  
  // 1. Busca estrita na malha de dados (Otimização O(1))
  if (chavePrimaria && paceMap.has(chavePrimaria)) {
    return chavePrimaria;
  }

  // 2. Busca heurística (Extrai a última palavra do vetor de texto: "Clippers")
  const identificadorRadical = displayName.split(" ").pop();
  const chaveHeuristica = Array.from(paceMap.keys()).find(k => 
    k.includes(identificadorRadical)
  );
  
  if (chaveHeuristica) {
    return chaveHeuristica;
  }

  // 3. Padrão de fallback original
  return displayName.split(" ")[0];
}
  
async function atualizarNBA() {
  console.log("⏳ Buscando classificação NBA (ESPN Standings API)...");

  const response = await fetch(ESPN_API, {
    headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json" },
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

  if (!entries.length) throw new Error("Nenhum dado retornado pela ESPN");

  const paceMap = await buscarPaceFactor();

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

    if (pace === null) {
      console.warn(`⚠️  Sem pace para: "${displayName}" (buscou "${hollingerNome}")`);
    }

    return {
      time: displayName,
      v: stats.wins,
      d: stats.losses,
      pct_vit:    stats.winPercent,
      ja:
        stats.gamesplayed ||
        stats.GP ||
        (Number(stats.wins) + Number(stats.losses)).toString(),
      casa:       stats.Home  || stats.home,
      visitante:  stats.Road  || stats.road,
      div:        stats.vsdiv || stats["vs. Div."] || stats.DIV,
      conf:       stats.vsconf || stats["vs. Conf."] || stats.CONF,
      pts:        stats.pointsForPerGame  || stats.avgPointsFor   || stats.pointsFor,
      pts_contra: stats.pointsAgainstPerGame || stats.avgPointsAgainst || stats.pointsAgainst,
      dif:        stats.pointDifferential,
      strk:       stats.streak,
      u10:        stats.L10 || stats.lasttengames || stats["Last Ten Games"],
      pace,  // ✅ Pace Factor
    };
  });

  console.log(`📊 ${dados.length} times encontrados`);

  console.log("🧹 Limpando dados antigos...");
  const { error: deleteError } = await supabase
    .from("classificacao_nba")
    .delete()
    .neq("time", "");

  if (deleteError) {
    console.error("❌ Erro ao limpar:", deleteError.message);
  }

  console.log("📥 Inserindo novos dados...");
  const { error: insertError } = await supabase
    .from("classificacao_nba")
    .insert(dados);

  if (insertError) {
    console.error("❌ Erro ao inserir:", insertError.message);
    throw insertError;
  }

  const semPace = dados.filter((d) => d.pace === null).map((d) => d.time);
  if (semPace.length) {
    console.warn(`⚠️  Times sem Pace (${semPace.length}): ${semPace.join(", ")}`);
  } else {
    console.log("🎯 Pace preenchido para todos os 30 times!");
  }

  console.log("✅ Classificação NBA atualizada com sucesso!");
}

atualizarNBA().catch((err) => {
  console.error("❌ Erro:", err.message);
  process.exit(1);
});
