import fetch from "node-fetch";
import cheerio from "cheerio";
import { createClient } from "@supabase/supabase-js";

/**
 * ======================================================
 * CONFIGURAÇÃO SUPABASE
 * ======================================================
 */
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("❌ Variáveis SUPABASE não configuradas");
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
    console.error("❌ Erro ao acessar site NBA");
    process.exit(1);
  }

  const html = await response.text();
  const $ = cheerio.load(html);

  const dados = [];

  $("table tbody tr").each((_, el) => {
    const cols = $(el).find("td");

    if (cols.length < 6) return;

    const time = $(cols[0]).text().trim();
    const vitorias = parseInt($(cols[1]).text().trim(), 10);
    const derrotas = parseInt($(cols[2]).text().trim(), 10);

    if (!time || isNaN(vitorias) || isNaN(derrotas)) return;

    dados.push({
      time,
      vitorias,
      derrotas
    });
  });

  if (dados.length === 0) {
    console.error("❌ Nenhum dado coletado. Layout pode ter mudado.");
    process.exit(1);
  }

  console.log(`📊 ${dados.length} times coletados`);

  /**
   * ======================================================
   * SALVAR NO SUPABASE
   * ======================================================
   */

  // Limpa tabela antes de inserir
  const { error: deleteError } = await supabase
    .from("classificacao_nba")
    .delete()
    .neq("id", 0);

  if (deleteError) {
    console.error("❌ Erro ao limpar tabela:", deleteError);
    process.exit(1);
  }

  const { data, error } = await supabase
    .from("classificacao_nba")
    .insert(dados);

  if (error) {
    console.error("❌ Erro ao inserir dados:", error);
    process.exit(1);
  }

  console.log(`✅ ${data.length} times inseridos no Supabase`);
}

scrapeNBA()
  .then(() => {
    console.log("🏀 NBA atualizada com sucesso");
    process.exit(0);
  })
  .catch((err) => {
    console.error("❌ Erro geral:", err);
    process.exit(1);
  });
