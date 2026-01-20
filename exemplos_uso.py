#!/usr/bin/env python3
"""
Exemplos Práticos de Uso da NBA Injuries API
Demonstra diferentes casos de uso
"""

import requests
import json
from datetime import datetime

# Configuração
API_URL = "http://localhost:5000/api"
JSON_FILE = "nba_injuries.json"


def exemplo_1_buscar_todas_lesoes():
    """Exemplo 1: Buscar todas as lesões"""
    print("\n" + "="*60)
    print("EXEMPLO 1: Buscar Todas as Lesões")
    print("="*60)
    
    response = requests.get(f"{API_URL}/injuries")
    data = response.json()
    
    if data['success']:
        print(f"\n✅ Total de lesões: {data['count']}")
        print("\nPrimeiros 5 jogadores:")
        for injury in data['data'][:5]:
            print(f"  • {injury['player_name']} ({injury['team_abbreviation']}) - {injury['injury_status']}")


def exemplo_2_filtrar_por_time():
    """Exemplo 2: Filtrar lesões de um time específico"""
    print("\n" + "="*60)
    print("EXEMPLO 2: Lesões dos Lakers (LAL)")
    print("="*60)
    
    response = requests.get(f"{API_URL}/injuries/team/LAL")
    data = response.json()
    
    if data['success']:
        print(f"\n✅ Lakers têm {data['count']} jogador(es) lesionado(s)")
        for injury in data['data']:
            print(f"\n  Jogador: {injury['player_name']}")
            print(f"  Posição: {injury['position']}")
            print(f"  Status: {injury['injury_status']}")
            print(f"  URL: {injury['espn_player_url']}")


def exemplo_3_estatisticas():
    """Exemplo 3: Obter estatísticas gerais"""
    print("\n" + "="*60)
    print("EXEMPLO 3: Estatísticas Gerais")
    print("="*60)
    
    response = requests.get(f"{API_URL}/stats")
    data = response.json()
    
    if data['success']:
        print(f"\n📊 Total de lesões: {data['total_injuries']}")
        
        print("\n📋 Por Status:")
        for status, count in data['by_status'].items():
            print(f"  • {status}: {count}")
        
        print("\n🏀 Top 5 Times:")
        for team_data in data['top_5_teams']:
            print(f"  • {team_data['team']}: {team_data['count']} lesões")


def exemplo_4_buscar_jogador():
    """Exemplo 4: Buscar jogador específico"""
    print("\n" + "="*60)
    print("EXEMPLO 4: Buscar Jogador por Nome")
    print("="*60)
    
    search_term = "LeBron"
    response = requests.get(f"{API_URL}/search?q={search_term}")
    data = response.json()
    
    if data['success']:
        print(f"\n🔍 Resultados para '{search_term}': {data['count']}")
        for injury in data['data']:
            print(f"\n  {injury['player_name']}")
            print(f"  Time: {injury['team_name']}")
            print(f"  Status: {injury['injury_status']}")


def exemplo_5_times_afetados():
    """Exemplo 5: Listar todos os times com lesões"""
    print("\n" + "="*60)
    print("EXEMPLO 5: Times Afetados por Lesões")
    print("="*60)
    
    response = requests.get(f"{API_URL}/teams")
    data = response.json()
    
    if data['success']:
        print(f"\n📊 {data['count']} times com jogadores lesionados\n")
        
        for team in data['data'][:10]:  # Top 10
            print(f"\n{team['team_name']} ({team['team_abbreviation']})")
            print(f"  Total: {team['injured_count']} lesões")
            print("  Jogadores:")
            for player in team['players']:
                print(f"    • {player['player_name']} ({player['position']}) - {player['injury_status']}")


def exemplo_6_filtrar_por_status():
    """Exemplo 6: Filtrar apenas jogadores "Out"""
    print("\n" + "="*60)
    print("EXEMPLO 6: Apenas Jogadores 'Out'")
    print("="*60)
    
    response = requests.get(f"{API_URL}/injuries/status/Out")
    data = response.json()
    
    if data['success']:
        print(f"\n🔴 {data['count']} jogadores fora de jogo")
        print("\nAgrupado por time:")
        
        teams = {}
        for injury in data['data']:
            team = injury['team_abbreviation']
            if team not in teams:
                teams[team] = []
            teams[team].append(injury['player_name'])
        
        for team, players in sorted(teams.items()):
            print(f"\n  {team} ({len(players)} jogadores):")
            for player in players:
                print(f"    • {player}")


def exemplo_7_processar_json_local():
    """Exemplo 7: Processar JSON local sem API"""
    print("\n" + "="*60)
    print("EXEMPLO 7: Processar JSON Local")
    print("="*60)
    
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            injuries = json.load(f)
        
        # Análise customizada
        guards_out = [
            i for i in injuries 
            if i['position'] == 'G' and i['injury_status'] == 'Out'
        ]
        
        print(f"\n🏀 {len(guards_out)} guards fora de jogo:")
        for injury in guards_out[:10]:
            print(f"  • {injury['player_name']} ({injury['team_abbreviation']})")
        
    except FileNotFoundError:
        print(f"❌ Arquivo {JSON_FILE} não encontrado")


def exemplo_8_montar_time_fantasy():
    """Exemplo 8: Verificar disponibilidade para Fantasy"""
    print("\n" + "="*60)
    print("EXEMPLO 8: Verificar Time Fantasy")
    print("="*60)
    
    # Meu time fantasy (exemplo)
    meu_time = ["LeBron James", "Stephen Curry", "Kevin Durant", "Luka Doncic"]
    
    print("\n📋 Verificando disponibilidade do meu time:\n")
    
    for player_name in meu_time:
        response = requests.get(f"{API_URL}/search?q={player_name.split()[0]}")
        data = response.json()
        
        if data['success'] and data['count'] > 0:
            # Encontrou lesão
            injury = data['data'][0]
            if player_name.lower() in injury['player_name'].lower():
                print(f"⚠️  {injury['player_name']}")
                print(f"    Status: {injury['injury_status']}")
                print(f"    Recomendação: Considere substituir\n")
        else:
            print(f"✅ {player_name}")
            print(f"    Status: Disponível\n")


def exemplo_9_comparar_conferencias():
    """Exemplo 9: Comparar lesões por conferência"""
    print("\n" + "="*60)
    print("EXEMPLO 9: Lesões por Conferência")
    print("="*60)
    
    # Times por conferência (simplificado)
    east = ['ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DET', 'IND', 
            'MIA', 'MIL', 'NY', 'ORL', 'PHI', 'TOR', 'WSH']
    west = ['DAL', 'DEN', 'GS', 'HOU', 'LAC', 'LAL', 'MEM', 'MIN', 
            'NO', 'OKC', 'PHX', 'POR', 'SAC', 'SA', 'UTAH']
    
    response = requests.get(f"{API_URL}/teams")
    data = response.json()
    
    if data['success']:
        east_injuries = 0
        west_injuries = 0
        
        for team in data['data']:
            abbr = team['team_abbreviation']
            count = team['injured_count']
            
            if abbr in east:
                east_injuries += count
            elif abbr in west:
                west_injuries += count
        
        print(f"\n🏀 Conferência Leste: {east_injuries} lesões")
        print(f"🏀 Conferência Oeste: {west_injuries} lesões")
        print(f"\n{'Oeste' if west_injuries > east_injuries else 'Leste'} está mais afetado!")


def exemplo_10_exportar_csv():
    """Exemplo 10: Exportar dados para CSV"""
    print("\n" + "="*60)
    print("EXEMPLO 10: Exportar para CSV")
    print("="*60)
    
    import csv
    
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            injuries = json.load(f)
        
        output_file = "nba_injuries_export.csv"
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'player_name', 'team_abbreviation', 'position', 
                'injury_status', 'jersey_number'
            ])
            writer.writeheader()
            
            for injury in injuries:
                writer.writerow({
                    'player_name': injury['player_name'],
                    'team_abbreviation': injury['team_abbreviation'],
                    'position': injury['position'],
                    'injury_status': injury['injury_status'],
                    'jersey_number': injury['jersey_number']
                })
        
        print(f"\n✅ Dados exportados para: {output_file}")
        print(f"   Total de registros: {len(injuries)}")
        
    except Exception as e:
        print(f"❌ Erro ao exportar: {e}")


def main():
    """Menu principal"""
    print("\n" + "="*60)
    print("🏀 NBA INJURIES API - EXEMPLOS PRÁTICOS")
    print("="*60)
    
    exemplos = [
        ("Buscar todas as lesões", exemplo_1_buscar_todas_lesoes),
        ("Filtrar por time (Lakers)", exemplo_2_filtrar_por_time),
        ("Estatísticas gerais", exemplo_3_estatisticas),
        ("Buscar jogador por nome", exemplo_4_buscar_jogador),
        ("Times afetados", exemplo_5_times_afetados),
        ("Filtrar por status (Out)", exemplo_6_filtrar_por_status),
        ("Processar JSON local", exemplo_7_processar_json_local),
        ("Verificar time fantasy", exemplo_8_montar_time_fantasy),
        ("Comparar conferências", exemplo_9_comparar_conferencias),
        ("Exportar para CSV", exemplo_10_exportar_csv),
    ]
    
    print("\n📚 Escolha um exemplo:\n")
    for idx, (desc, _) in enumerate(exemplos, 1):
        print(f"  {idx}. {desc}")
    print("  0. Executar todos")
    
    try:
        escolha = input("\n👉 Digite o número do exemplo (0-10): ").strip()
        
        if escolha == "0":
            for desc, func in exemplos:
                try:
                    func()
                except Exception as e:
                    print(f"\n❌ Erro no exemplo '{desc}': {e}")
        elif escolha.isdigit() and 1 <= int(escolha) <= len(exemplos):
            idx = int(escolha) - 1
            exemplos[idx][1]()
        else:
            print("\n❌ Opção inválida!")
            
    except KeyboardInterrupt:
        print("\n\n👋 Até logo!")
    except Exception as e:
        print(f"\n❌ Erro: {e}")
    
    print("\n" + "="*60)
    print("✅ Exemplos concluídos!")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
