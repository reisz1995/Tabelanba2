import { createClient } from '@supabase/supabase-js';

// Verifica se as chaves existem para evitar erros silenciosos
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('❌ Erro Crítico: SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY não foram definidos nos Segredos do GitHub.');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

async function updateTeams() {
  console.log('🏀 Iniciando atualização da NBA via GitHub Actions...');
  
  try {
    // 1. Busca times da ESPN
    const espnResponse = await fetch('https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams?limit=35');
    const espnData = await espnResponse.json();
    const espnTeams = espnData.sports[0].leagues[0].teams;

    for (const item of espnTeams) {
      const teamData = item.team;
      const teamId = teamData.id;
      const teamName = teamData.displayName;

      // 2. Busca o calendário (últimos jogos) para este time
      const scheduleResp = await fetch(`https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/${teamId}/schedule`);
      const scheduleData = await scheduleResp.json();
      
      // Filtra jogos finalizados
      const finishedGames = (scheduleData.events || [])
        .filter(e => e.competitions[0].status.type.state === 'post')
        .sort((a, b) => new Date(a.date) - new Date(b.date)); // Ordena por data (Antigo -> Recente)

      // Pega os últimos 5
      const last5 = finishedGames.slice(-5).map(game => {
        const competitor = game.competitions[0].competitors.find(c => c.id === teamId);
        // Se não achar o competidor (erro da API), assume derrota por segurança
        return (competitor && competitor.winner) ? 'V' : 'D'; 
      });

      // Se tiver menos de 5, preenche com 'D' (ou deixe vazio se preferir)
      while (last5.length < 5) last5.unshift('D');

      console.log(`Atualizando ${teamName}: [${last5.join('-')}]`);

      // 3. Salva no Supabase
      // Usamos ILIKE para encontrar o time pelo nome, já que os IDs podem variar
      const { error } = await supabase
        .from('teams')
        .update({ 
          record: last5,
          updated_at: new Date().toISOString()
        }) 
        .ilike('name', `%${teamName}%`); // Ex: "Lakers" dá match em "Los Angeles Lakers"

      if (error) {
        console.error(`❌ Erro ao salvar ${teamName}:`, error.message);
      }
    }
    
    console.log('✅ Atualização concluída com sucesso!');

  } catch (error) {
    console.error('❌ Erro fatal no script:', error);
    process.exit(1);
  }
}

updateTeams();
