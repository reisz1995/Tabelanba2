import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.SUPABASE_URL;
// Mapeamento retificado para consumir a variável exata injetada pelo workflow
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('❌ COLAPSO: Credenciais ausentes da matriz de ambiente.');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

async function updateTeams() {
  console.log('🏀 Inicializando atualização de Momentum Global (Topologia JSON Rica)...');
  
  try {
    const espnResponse = await fetch('https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams?limit=100');
    const espnData = await espnResponse.json();
    const espnTeams = espnData.sports[0].leagues[0].teams;

    let successCount = 0;
    let errorCount = 0;

    for (const item of espnTeams) {
      const teamId = item.team.id;
      const fullName = item.team.displayName; 

      const scheduleResp = await fetch(`https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/${teamId}/schedule`);
      const scheduleData = await scheduleResp.json();
      
      const finishedGames = (scheduleData.events || [])
        .filter(e => e.competitions[0].status.type.state === 'post')
        .sort((a, b) => new Date(a.date) - new Date(b.date));

            // ... [Código anterior até a filtragem finishedGames]

      const last5 = finishedGames.slice(-5).map(game => {
        const comp = game.competitions[0];
        const mainTeam = comp.competitors.find(c => c.id === teamId);
        const oppTeam = comp.competitors.find(c => c.id !== teamId);
        
        // Extrator Determinístico de Score (Ignora diferenças de tipo da API)
        const getScore = (c) => typeof c.score === 'object' ? (c.score.value || 0) : (parseInt(c.score) || 0);
        const mainScore = getScore(mainTeam);
        const oppScore = getScore(oppTeam);
        
        const dt = new Date(game.date);
        return {
          date: `${String(dt.getDate()).padStart(2, '0')}/${String(dt.getMonth() + 1).padStart(2, '0')}`,
          opponent: oppTeam.team.abbreviation,
          result: mainTeam.winner ? 'V' : 'D',
          score: `${Math.max(mainScore, oppScore)}-${Math.min(mainScore, oppScore)}`
        };
      });

      // ... [Segue o código de injeção no Supabase]


      const { data, error } = await supabase
        .from('teams')
        .update({ record: last5, updated_at: new Date().toISOString() }) 
        .eq('name', fullName)
        .select();

      if (error) {
        console.error(`❌ Fissura ao injetar ${fullName}:`, error.message);
        errorCount++;
      } else if (data && data.length > 0) {
        console.log(`✅ ${fullName}: Matriz temporal atualizada.`);
        successCount++;
      }
    }
    console.log(`\n🏁 Ciclo encerrado: ${successCount} atualizados.`);
  } catch (error) {
    console.error('❌ Colapso termodinâmico:', error);
    process.exit(1);
  }
}
updateTeams();
