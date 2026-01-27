import { createClient } from '@supabase/supabase-js';

// Verifica credenciais
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('❌ Erro Crítico: Credenciais do Supabase não encontradas nos Segredos do GitHub.');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

async function updateTeams() {
  console.log('🏀 Iniciando atualização da NBA (Modo: Nome Completo)...');
  
  try {
    // 1. Busca todos os times da ESPN
    const espnResponse = await fetch('https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams?limit=100');
    const espnData = await espnResponse.json();
    const espnTeams = espnData.sports[0].leagues[0].teams;

    let successCount = 0;
    let errorCount = 0;

    for (const item of espnTeams) {
      const teamData = item.team;
      const teamId = teamData.id;
      const fullName = teamData.displayName; // Ex: "Boston Celtics" (Combina com seu novo Banco de Dados)

      // 2. Busca o calendário (últimos jogos) para este time
      const scheduleResp = await fetch(`https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/${teamId}/schedule`);
      const scheduleData = await scheduleResp.json();
      
      // Filtra apenas jogos finalizados ('post') e ordena por data
      const finishedGames = (scheduleData.events || [])
        .filter(e => e.competitions[0].status.type.state === 'post')
        .sort((a, b) => new Date(a.date) - new Date(b.date));

      // Pega os últimos 5 resultados
      const last5 = finishedGames.slice(-5).map(game => {
        const competitor = game.competitions[0].competitors.find(c => c.id === teamId);
        // Se ganhou = 'V', senão 'D'
        return (competitor && competitor.winner) ? 'V' : 'D'; 
      });

      // Preenche com 'D' se tiver menos de 5 jogos na temporada (início de season)
      while (last5.length < 5) last5.unshift('D');

      // 3. Atualiza no Supabase usando COMPARAÇÃO EXATA (.eq)
      const { data, error } = await supabase
        .from('teams')
        .update({ 
          record: last5,
          updated_at: new Date().toISOString()
        }) 
        .eq('name', fullName) // Agora busca exato: "Boston Celtics" == "Boston Celtics"
        .select();

      if (error) {
        console.error(`❌ Erro ao salvar ${fullName}:`, error.message);
        errorCount++;
      } else if (data && data.length > 0) {
        console.log(`✅ ${fullName}: [${last5.join('-')}] atualizado.`);
        successCount++;
      } else {
        console.warn(`⚠️ ${fullName}: Time não encontrado no banco de dados (Verifique se o nome está idêntico).`);
        errorCount++;
      }
    }
    
    console.log(`\n🏁 Resumo: ${successCount} atualizados com sucesso, ${errorCount} erros/não encontrados.`);

  } catch (error) {
    console.error('❌ Erro fatal no script:', error);
    process.exit(1);
  }
}

updateTeams();
