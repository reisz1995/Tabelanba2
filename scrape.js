import * as cheerio from "cheerio";
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
 * SCRAPER NBA
 * ======================================================
 */
const URL = "https://www.nba.com/standings";

async function scrapeNBA() {
  console.log("⏳ Buscando classificação NBA...");

  const response = await fetch(URL);
  if (!response.ok) {
    throw new Error("Erro ao acessar site da NBA");
  }

  const html = await response.text();
  const $ = cheerio.load(html);

  const dados = [];

  $("table tbody tr").each((_, el) => {
    const cols = $(el).find("td");
    if (cols.length < 5) return;

    const time = $(cols[0]).text().trim();
    const vitorias = Number($(cols[1]).text().trim());
    const derrotas = Number($(cols[2]).text().trim());

    if (!time || isNaN(vitorias) || isNaN(derrotas)) return;

    dados.push({ time, vitorias, derrotas });
  });

  if (dados.length === 0) {
    throw new Error("Nenhum dado coletado — layout da NBA mudou");
  }

  console.log(`📊 ${dados.length} times coletados`);

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

  console.log("🏀 NBA atualizada com sucesso");
}

scrapeNBA().catch((err) => {
  console.error("❌ Erro:", err.message);
  process.exit(1);
});
                  
