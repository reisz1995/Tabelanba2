# 🏀 Tabelanba 2.0

Pipeline analítico e preditivo automatizado para a NBA. O sistema realiza extração de estatísticas oficiais, classificação, métricas térmicas/avançadas de eficiência, rastreamento de lesões em tempo real e geração de análises táticas e palpites de jogos com IA (LLaMA 3.3 70B via Groq Cloud).

---

## 🛠️ Stack Tecnológica

* **Node.js (ES Modules)**: Coletores de classificação (ESPN API), métricas de ritmo/Pace (Hollinger) e momentum recente das equipes.
* **Python (3.10+)**: Raspagem de dados avançados (Databallr), rastreador de lesões (ESPN Roster API) e motor de predição baseado em IA (Groq Cloud).
* **Supabase (PostgreSQL + RLS)**: Armazenamento persistente, triggers e views em tempo real.
* **Groq Cloud (LLaMA 3.3 70B Versatile)**: Geração de predições com foco em Over/Under, Handicap e Fatores Chave.
* **GitHub Actions**: Orquestração e automação diária dos ciclos de coleta e análise.

---

## 📂 Estrutura de Arquivos

```
├── .github/workflows/
│   ├── update_data.yml           # Pipeline principal consolidado (diário às 11:00 UTC)
│   ├── databallr_scraper.yml     # Extração de tensores Databallr
│   ├── nba_injuries.yml          # Atualização periódica de lesões
│   ├── nba_scraper.yml           # Raspagem de predições Scores24
│   └── update.yml / nba-updater  # Workflows específicos
├── scripts/
│   └── update-nba-stats.js       # Atualiza momentum dos últimos 5 jogos (teams.record)
├── src/
│   └── opencode.js               # Utilitários de sincronia
├── scrape.js                     # Coleta standings da ESPN + Pace Factor (classificacao_nba)
├── importPlayers.js              # Coleta líderes de pontuação/rebotes/assistências
├── nba_injuries_api.py           # Varredura de elencos ESPN e sync de lesões (nba_injured_players)
├── scraper_databallr.py          # Extrator multi-período de métricas avançadas (databallr_team_stats)
├── predict_games.py              # Motor Preditivo de IA com Groq (game_predictions)
├── nba_scraper.py                # Scraper NLP Scores24 (nba_games_schedule)
├── flask_api.py                  # API REST local para consulta de lesões
├── supabase_schema.sql           # Schema SQL para a tabela de lesões
├── requirements.txt              # Dependências Python
├── package.json                  # Dependências Node.js
└── .env.example                  # Modelo de variáveis de ambiente
```

---

## 🗄️ Esquema do Banco de Dados (Supabase)

| Tabela | Responsável | Descrição |
| :--- | :--- | :--- |
| `classificacao_nba` | `scrape.js` | Tabela geral de classificação, vitórias, derrotas e Pace Factor |
| `teams` | `update-nba-stats.js` | Informações de cada time e histórico dos últimos 5 jogos (`record`) |
| `nba_jogadores_stats` | `importPlayers.js` | Estatísticas dos líderes da liga (pontos, rebotes, assistências) |
| `nba_injured_players` | `nba_injuries_api.py` | Jogadores lesionados em tempo real da ESPN com detalhes clínicos |
| `databallr_team_stats`| `scraper_databallr.py`| Tensores analíticos: ORTG, DRTG, Net Rating, True Shooting, Rebounds (season-to-date da temporada em curso, `period='this_year'`) |
| `nba_odds_matrix`     | Manual / Feed | Linhas de odds do mercado |
| `game_predictions`    | `predict_games.py` | Predições da IA (Over/Under, Handicap, Confiança, Alerta de Lesão) |
| `nba_games_schedule`  | `nba_scraper.py` | Slugs, horários e textos táticos raspados do Scores24 |

---

## ⚙️ Configuração do Ambiente

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/Tabelanba2.git
cd Tabelanba2
```

2. Crie o arquivo `.env` baseado no modelo:
```bash
cp .env.example .env
```

3. Preencha suas credenciais no `.env`:
```env
SUPABASE_URL=https://xxxxxxxxxxxxxxxxxxxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY=sua-service-role-secret-key
GROQ_API_KEY=gsk_sua_chave_groq
```

---

## 🚀 Como Executar Localmente

### 1. Dependências
```bash
# Node.js
npm install

# Python
pip install -r requirements.txt
```

### 2. Executar Rotinas de Dados

* **Classificação e Pace:**
  ```bash
  node scrape.js
  ```

* **Líderes Individuais e Momentum:**
  ```bash
  node importPlayers.js
  node scripts/update-nba-stats.js
  ```

* **Lesões da NBA:**
  ```bash
  python nba_injuries_api.py
  ```

* **Métricas Avançadas Databallr:**
  ```bash
  python scraper_databallr.py
  ```

* **Motor Preditivo IA (Estatístico Chefe):**
  ```bash
  python predict_games.py
  ```
