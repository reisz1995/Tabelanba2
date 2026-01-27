// scripts/update-nba-stats.js
const { createClient } = require('@supabase/supabase-js');

// Você vai configurar essas chaves nos Segredos do GitHub depois
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY; // Chave secreta (não a pública!)
const supabase = createClient(supabaseUrl, supabaseKey);

async function updateTeams() {
  console.log('🏀 Iniciando atualização da NBA...');
  
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
      .sort((a, b) => new Date(a.date) - new Date(b.date)); // Ordena por data

    // Pega os últimos 5
    const last5 = finishedGames.slice(-5).map(game => {
      const competitor = game.competitions[0].competitors.find(c => c.id === teamId);
      return competitor.winner ? 'V' : 'D';
    });

    // Se tiver menos de 5, preenche com 'D' ou vazio
    while (last5.length < 5) last5.unshift('D');

    console.log(`Atualizando ${teamName}: ${last5.join('-')}`);

    // 3. Salva no Supabase
    // IMPORTANTE: Seu banco precisa ter uma coluna para identificar o time corretamente (ex: nome ou um ID mapeado)
    // Aqui estou assumindo que você vai atualizar baseado no nome ou criar uma coluna 'espn_id' no seu banco
    const { error } = await supabase
      .from('teams')
      .update({ 
        record: last5, 
        // Você também pode atualizar vitórias/derrotas aqui se quiser
      }) 
      .ilike('name', `%${teamData.name}%`); // Tenta casar pelo nome (ex: "Lakers")

    if (error) console.error(`Erro ao salvar ${teamName}:`, error);
  }
  
  console.log('✅ Atualização concluída!');
}

updateTeams();
