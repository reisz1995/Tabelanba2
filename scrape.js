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
      team: $(cols[0]).text().trim(),
      wins: Number($(cols[1]).text()),
      losses: Number($(cols[2]).text()),
      win_pct: Number($(cols[3]).text()),
      games_back: $(cols[4]).text() === "-" ? 0 : Number($(cols[4]).text()),
      home_record: $(cols[5]).text(),
      away_record: $(cols[6]).text(),
      division_record: $(cols[7]).text(),
      conference_record: $(cols[8]).text(),
      points_for: Number($(cols[9]).text()),
      points_against: Number($(cols[10]).text()),
      point_diff: Number($(cols[11]).text()),
      streak: $(cols[12]).text(),
      last_10: $(cols[13]).text(),
      season: "2025-26"
    });
  });

  await supabase.from("nba_classificacao").delete().neq("id", "");
  await supabase.from("nba_classificacao").insert(data);

  console.log("NBA atualizada com sucesso");
}

run();
