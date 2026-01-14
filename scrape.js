import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function buscarClassificacao() {
  console.log("⏳ Buscando classificação NBA (ESPN)...");

  try {
    const response = await fetch(
      "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/standings"
    );

    if (!response.ok) {
      console.log("Status HTTP:", response.status);
      throw new Error("Erro ao acessar API da ESPN");
    }

    const json = await response.json();

    const equipes = json.children[0].standings.entries;

    const dadosTratados = equipes.map((time) => {
      const stats = time.stats;

      const getStat = (name) =>
        stats.find((s) => s.name === name)?.displayValue || "-";

      return {
        time: time.team.displayName,
        v: parseInt(getStat("wins")) || 0,
        d: parseInt(getStat("losses")) || 0,
        pct_vit: parseFloat(getStat("winPercent")) || 0,
        ja: getStat("gamesPlayed"),
        casa: getStat("Home"),
        visitante: getStat("Road"),
        div: getStat("vsDiv"),
        conf: getStat("vsConf"),
        pts: parseFloat(getStat("pointsFor")) || 0,
        pts_contra: parseFloat(getStat("pointsAgainst")) || 0,
        dif: getStat("pointDifferential"),
        strk: getStat("streak"),
        u10: getStat("Last10")
      };
    });

    console.log(`📊 ${dadosTratados.length} times encontrados`);

    await salvarNoSupabase(dadosTratados);
  } catch (erro) {
    console.log("❌ Erro:", erro.message);
    process.exit(1);
  }
}

async function salvarNoSupabase(times) {
  try {
    // Limpa a tabela antes de inserir novos dados
    await supabase.from("classificacao_nba").delete().neq("time", "");

    const { error } = await supabase
      .from("classificacao_nba")
      .insert(times);

    if (error) {
      throw error;
    }

    console.log("🏀 Classificação NBA atualizada com sucesso no Supabase!");
  } catch (erro) {
    console.log("❌ Erro ao salvar no Supabase:", erro.message);
    process.exit(1);
  }
}

buscarClassificacao();
