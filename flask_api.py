"""
API REST para Jogadores Lesionados da NBA
Fornece endpoints para consultar dados de lesões
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import json
from datetime import datetime
from typing import List, Dict, Any

app = Flask(__name__)
CORS(app)  # Permite requisições de qualquer origem

# Arquivo de dados
DATA_FILE = "nba_injuries.json"


def load_injuries() -> List[Dict[str, Any]]:
    """Carrega dados de lesões do arquivo JSON"""
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []


@app.route('/')
def home():
    """Endpoint raiz com documentação"""
    return jsonify({
        'name': 'NBA Injuries API',
        'version': '1.0.0',
        'description': 'API para consultar jogadores lesionados da NBA',
        'endpoints': {
            '/api/injuries': 'Lista todas as lesões',
            '/api/injuries/team/<abbreviation>': 'Lesões de um time específico',
            '/api/injuries/player/<player_id>': 'Lesões de um jogador específico',
            '/api/injuries/status/<status>': 'Lesões por status (Out, Day-To-Day, etc)',
            '/api/teams': 'Lista todos os times com lesões',
            '/api/stats': 'Estatísticas gerais de lesões'
        },
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/injuries')
def get_all_injuries():
    """
    Lista todas as lesões
    Query params: 
        - limit: número máximo de resultados (padrão: 100)
    """
    injuries = load_injuries()
    limit = request.args.get('limit', 100, type=int)
    if limit is None:
        limit = 100
    limit = max(1, limit)
    
    return jsonify({
        'success': True,
        'count': len(injuries[:limit]),
        'total': len(injuries),
        'data': injuries[:limit]
    })


@app.route('/api/injuries/team/<abbreviation>')
def get_injuries_by_team(abbreviation: str):
    """
    Busca lesões de um time específico
    
    Args:
        abbreviation: Sigla do time (ex: LAL, GSW, BOS)
    """
    injuries = load_injuries()
    team_injuries = [
        injury for injury in injuries 
        if injury.get('team_abbreviation', '').upper() == abbreviation.upper()
    ]
    
    return jsonify({
        'success': True,
        'team': abbreviation.upper(),
        'count': len(team_injuries),
        'data': team_injuries
    })


@app.route('/api/injuries/player/<player_id>')
def get_injury_by_player(player_id: str):
    """
    Busca lesões de um jogador específico
    
    Args:
        player_id: ID do jogador na ESPN
    """
    injuries = load_injuries()
    player_injuries = [
        injury for injury in injuries 
        if str(injury.get('player_id', '')) == str(player_id)
    ]
    
    if player_injuries:
        return jsonify({
            'success': True,
            'player_id': player_id,
            'data': player_injuries[0]
        })
    else:
        return jsonify({
            'success': False,
            'message': 'Jogador não encontrado ou sem lesões'
        }), 404


@app.route('/api/injuries/status/<status>')
def get_injuries_by_status(status: str):
    """
    Busca lesões por status
    
    Args:
        status: Status da lesão (Out, Day-To-Day, Questionable, etc)
    """
    injuries = load_injuries()
    status_injuries = [
        injury for injury in injuries 
        if injury.get('injury_status', '').lower() == status.lower()
    ]
    
    return jsonify({
        'success': True,
        'status': status,
        'count': len(status_injuries),
        'data': status_injuries
    })


@app.route('/api/teams')
def get_teams_with_injuries():
    """Lista todos os times com jogadores lesionados"""
    injuries = load_injuries()
    
    teams = {}
    for injury in injuries:
        team_abbr = injury.get('team_abbreviation')
        if team_abbr:
            if team_abbr not in teams:
                teams[team_abbr] = {
                    'team_abbreviation': team_abbr,
                    'team_name': injury.get('team_name'),
                    'injured_count': 0,
                    'players': []
                }
            
            teams[team_abbr]['injured_count'] += 1
            teams[team_abbr]['players'].append({
                'player_name': injury.get('player_name'),
                'position': injury.get('position'),
                'injury_status': injury.get('injury_status')
            })
    
    teams_list = sorted(teams.values(), key=lambda x: x['injured_count'], reverse=True)
    
    return jsonify({
        'success': True,
        'count': len(teams_list),
        'data': teams_list
    })


@app.route('/api/stats')
def get_statistics():
    """Retorna estatísticas gerais sobre lesões"""
    injuries = load_injuries()
    
    # Conta por status
    status_count = {}
    for injury in injuries:
        status = injury.get('injury_status', 'Unknown')
        status_count[status] = status_count.get(status, 0) + 1
    
    # Conta por time
    team_count = {}
    for injury in injuries:
        team = injury.get('team_abbreviation', 'Unknown')
        team_count[team] = team_count.get(team, 0) + 1
    
    # Conta por posição
    position_count = {}
    for injury in injuries:
        pos = injury.get('position', 'Unknown')
        position_count[pos] = position_count.get(pos, 0) + 1
    
    # Top 5 times com mais lesões
    top_teams = sorted(team_count.items(), key=lambda x: x[1], reverse=True)[:5]
    
    return jsonify({
        'success': True,
        'total_injuries': len(injuries),
        'by_status': status_count,
        'by_position': position_count,
        'top_5_teams': [
            {'team': team, 'count': count} 
            for team, count in top_teams
        ],
        'last_updated': injuries[0].get('last_updated') if injuries else None
    })


@app.route('/api/search')
def search_players():
    """
    Busca jogadores por nome
    Query params:
        - q: termo de busca
    """
    query = request.args.get('q', '').lower()
    
    if not query:
        return jsonify({
            'success': False,
            'message': 'Parâmetro "q" é obrigatório'
        }), 400
    
    injuries = load_injuries()
    results = [
        injury for injury in injuries 
        if query in injury.get('player_name', '').lower()
    ]
    
    return jsonify({
        'success': True,
        'query': query,
        'count': len(results),
        'data': results
    })


@app.errorhandler(404)
def not_found(error):
    """Handler para rotas não encontradas"""
    return jsonify({
        'success': False,
        'message': 'Endpoint não encontrado',
        'error': str(error)
    }), 404


@app.errorhandler(500)
def internal_error(error):
    """Handler para erros internos"""
    return jsonify({
        'success': False,
        'message': 'Erro interno do servidor',
        'error': str(error)
    }), 500


if __name__ == '__main__':
    print("=" * 60)
    print("🏀 NBA INJURIES API")
    print("=" * 60)
    print("\n📡 Servidor iniciado em: http://localhost:5000")
    print("\n📚 Documentação disponível em: http://localhost:5000")
    print("\n🔗 Endpoints disponíveis:")
    print("  • GET  /api/injuries")
    print("  • GET  /api/injuries/team/<abbreviation>")
    print("  • GET  /api/injuries/player/<player_id>")
    print("  • GET  /api/injuries/status/<status>")
    print("  • GET  /api/teams")
    print("  • GET  /api/stats")
    print("  • GET  /api/search?q=<nome>")
    print("\n✨ Para instalar dependências: pip install flask flask-cors")
    print("=" * 60)
    print()
    
    app.run(debug=True, host='0.0.0.0', port=5000)
