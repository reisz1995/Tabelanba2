import { load } from "cheerio";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

async function run() {
  const res = await fetch(
    "https://www.espn.com.br/nba/classificacao/_/ordenar/wins/dir/desce/grupo/liga"
  );

  const html = await res.text();
  const $ = load(html);

  const data = [];

  $("table tbody tr").each((_, row) => {
    const cols = $(row).find("td");
    if (cols.length < 14) return;

    data.push({
      time: $(cols[0]).text().trim(),
      vitorias: Number($(cols[1]).text()),
      derrotas: Number($(cols[2]).text()),
      casa: $(cols[5]).text(),
      visitante: $(cols[6]).text(),
      pontos: Number($(cols[9]).text()),
      pontos_contra: Number($(cols[10]).text()),
      sequencia: $(cols[12]).text(),
      ultimos_10: $(cols[13]).text()
    });
  });

  await supabase.from("classificacao_nba").delete().neq("id", 0);

   const { error } = await supabase.from("classificacao_nba").insert(data);

  if (error) {
    console.error(error);
    process.exit(1);
  }

  console.log("NBA atualizada com sucesso");
}

run();
