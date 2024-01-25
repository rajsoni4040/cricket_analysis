import json
import sqlite3
import zipfile
import requests
from io import BytesIO

def get_player_id(cursor, player_name):
    cursor.execute("SELECT registry FROM players WHERE name = ?", (player_name,))
    player_id = cursor.fetchone()
    if player_id:
        return player_id[0]
    else:
        print(f"Player {player_name} not found in the database.")
        return None

def create_database_tables(cursor):
    cursor.execute('''
        DROP TABLE IF EXISTS matches
    ''')
    cursor.execute('''
        DROP TABLE IF EXISTS innings
    ''')
    cursor.execute('''
        DROP TABLE IF EXISTS deliveries
    ''')
    cursor.execute('''
        DROP TABLE IF EXISTS players
    ''')

    # Create Players table if not exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS players (
            name TEXT UNIQUE,
            registry TEXT PRIMARY KEY
        )
    ''')

    # Create Matches table if not exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            date TEXT,
            event_name TEXT,
            match_number INTEGER,
            gender TEXT,
            match_type TEXT,
            match_type_number INTEGER,
            match_referee TEXT,
            reserve_umpire TEXT,
            tv_umpire TEXT,
            umpire1 TEXT,
            umpire2 TEXT,
            winner TEXT,
            win_by_wickets INTEGER,
            overs INTEGER,
            player_of_match TEXT,
            season TEXT,
            team_type TEXT,
            venue TEXT,
            toss_decision TEXT,
            toss_winner TEXT
        )
    ''')

    # Create Innings table if not exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS innings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team TEXT,
            target_overs INTEGER,
            target_runs INTEGER,
            match_id INTEGER,
            FOREIGN KEY (match_id) REFERENCES matches(id)
        )
    ''')

    # Create Deliveries table if not exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS deliveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batter_id TEXT,
            bowler_id TEXT,
            non_striker_id TEXT,
            over REAL,
            runs_batter INTEGER,
            wides INTEGER,
            no_balls INTEGER,
            byes INTEGER,
            leg_byes INTEGER,
            runs_extra INTEGER,
            runs_total INTEGER,
            inning_id INTEGER,
            wicket BOOLEAN,
            wicket_kind TEXT,
            player_out TEXT,
            fielder TEXT,
            match_id INTEGER,
            FOREIGN KEY (match_id) REFERENCES matches (id),
            FOREIGN KEY (inning_id) REFERENCES innings (id)
        )
    ''')

def initialize_database():
    connection = sqlite3.connect('cricket_data.db')
    cursor = connection.cursor()
    create_database_tables(cursor)
    connection.commit()
    connection.close()

def insert_player_into_database(cursor, player_name, player_id):
    cursor.execute('INSERT OR IGNORE INTO players (registry, name) VALUES (?, ?)', (player_name, player_id))

def insert_match_into_database(cursor, match_info):
    officials = match_info.get('officials', {})
    toss_info = match_info.get('toss', {})

    cursor.execute('''
        INSERT INTO matches (
            city, date, event_name, match_number, gender, match_type, match_type_number,
            match_referee, reserve_umpire, tv_umpire, umpire1, umpire2,
            winner, win_by_wickets, overs, player_of_match, season, team_type,
            venue, toss_decision, toss_winner
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''',
    (match_info.get('city', ''),
     match_info.get('dates', [''])[0],
     match_info.get('event', {}).get('name', ''),
     match_info.get('event', {}).get('match_number', 0),
     match_info.get('gender', ''),
     match_info.get('match_type', ''),
     match_info.get('match_type_number', 0),
     officials.get('match_referees', [''])[0],
     officials.get('reserve_umpires', [''])[0],
     officials.get('tv_umpires', [''])[0],
     officials.get('umpires', [''])[0],
     officials.get('umpires', [''])[1] if len(officials.get('umpires', [])) > 1 else '', 
     match_info.get('outcome', {}).get('winner', ''),
     match_info.get('outcome', {}).get('by', {}).get('wickets', 0),
     match_info.get('overs', 0),
     match_info.get('player_of_match', [''])[0],
     match_info.get('season', ''),
     match_info.get('team_type', ''),
     match_info.get('venue', ''),
     toss_info.get('decision', ''),
     toss_info.get('winner', '')))

def insert_inning_into_database(cursor, inning_data, match_id):
    cursor.execute('INSERT INTO innings (team, target_overs, target_runs, match_id) VALUES (?, ?, ?, ?)',
                   (inning_data.get('team', ''),
                    inning_data.get('target', {}).get('overs', 0),
                    inning_data.get('target', {}).get('runs', 0),
                    match_id))

def insert_delivery_into_database(connection, cursor, innings_data, inning_id, match_id):
    overs_data = innings_data.get('overs', [])
    
    for over_data in overs_data:
        over = over_data.get('over', 0)
        deliveries_data = over_data.get('deliveries', [])
        for delivery_info in deliveries_data:
            batter_info = delivery_info.get('batter', '')
            batter_name = batter_info if isinstance(batter_info, str) else batter_info.get('name', '')
            print(f"Processing delivery for inning_id: {inning_id}, batter_name: {batter_name}")

            batter_id = get_player_id(cursor, batter_name)
            bowler_id = get_player_id(cursor, delivery_info.get('bowler', ''))
            non_striker_id = get_player_id(cursor, delivery_info.get('non_striker', ''))

            runs_data = delivery_info.get('runs', {})
            runs_batter = runs_data.get('batter', 0)

            extras_data = delivery_info.get('extras', {})
            wides = extras_data.get('wides', 0) if isinstance(extras_data, dict) else 0
            no_balls = extras_data.get('noballs', 0) if isinstance(extras_data, dict) else 0
            byes = extras_data.get('byes', 0) if isinstance(extras_data, dict) else 0
            leg_byes = extras_data.get('legbyes', 0) if isinstance(extras_data, dict) else 0

            runs_extra = sum([wides, no_balls, byes, leg_byes])
            runs_total = delivery_info.get('runs', {}).get('total', 0)

            wicket_info = delivery_info.get('wickets', [])
            if wicket_info:
                wicket = 1
                wicket_kind = wicket_info[0].get('kind', '')
                player_out = wicket_info[0].get('player_out', '')
                player_out_id = get_player_id(cursor, player_out)
                fielder = wicket_info[0].get('fielders', [{}])[0].get('name', '') if wicket_info[0].get('fielders', [{}]) else ''
                fielder_id = get_player_id(cursor, fielder)
            else:
                wicket = 0
                wicket_kind = None
                player_out_id = None
                fielder_id = None

            cursor.execute('''
                INSERT INTO deliveries (
                    batter_id, bowler_id, non_striker_id, over, runs_batter, wides, no_balls, byes, leg_byes,
                    runs_extra, runs_total, inning_id, wicket, wicket_kind, player_out, fielder, match_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (batter_id, bowler_id, non_striker_id, over, runs_batter, wides, no_balls, byes, leg_byes,
             runs_extra, runs_total, inning_id, wicket, wicket_kind, player_out_id, fielder_id, match_id))
            
    connection.commit()

def read_json_files_from_zip_and_insert_data(connection, cursor, zip_url):
    response = requests.get(zip_url)
    
    if response.status_code == 200:
        zip_content = BytesIO(response.content)
        
        with zipfile.ZipFile(zip_content, 'r') as zip_file:
            json_files = [name for name in zip_file.namelist() if name.endswith('.json')]

            for json_file_name in json_files:
                with zip_file.open(json_file_name) as json_file:
                    data = json.load(json_file)

                    registry_data = data.get('info', {}).get('registry', {}).get('people', {})
                    for player_name, player_id in registry_data.items():
                        insert_player_into_database(cursor, player_id, player_name)

                    insert_match_into_database(cursor, data.get('info', {}))
                    
                    innings_data = data.get('innings', [])
                    match_id = cursor.lastrowid
                    
                    for inning_number, inning_data in enumerate(innings_data, start=1):
                        insert_inning_into_database(cursor, inning_data, match_id)

                        inning_id = cursor.lastrowid

                        if isinstance(inning_data, dict) and 'overs' in inning_data:
                            overs_data = inning_data.get('overs', [])
                            insert_delivery_into_database(connection, cursor, inning_data, inning_id, match_id)
                            
    connection.commit()

def main():
    initialize_database()
    zip_file_url = 'https://cricsheet.org/downloads/t20s_male_json.zip'
    
    connection = sqlite3.connect('cricket_data.db')
    cursor = connection.cursor()

    read_json_files_from_zip_and_insert_data(connection, cursor, zip_file_url)

    connection.commit()
    connection.close()

if __name__ == "__main__":
    main()

import sqlite3
from prettytable import PrettyTable

def print_table(table_name):
    connection = sqlite3.connect('cricket_data.db')
    cursor = connection.cursor()

    # Execute a simple SELECT query to fetch all rows from the table
    cursor.execute(f'SELECT * FROM {table_name}')
    rows = cursor.fetchall()

    # Fetch column names
    cursor.execute(f'PRAGMA table_info({table_name})')
    columns = [column[1] for column in cursor.fetchall()]

    # Create a PrettyTable instance
    table = PrettyTable(columns)

    # Add rows to the table
    for row in rows:
        table.add_row(row)

    # Print the formatted table
    print(table)

# Call the function with the table name you want to print
print("Players:")
print_table('players')
print("Matches:")
print_table('matches')
print("Innings:")
print_table('innings')
print("Deliveries:")
print_table('deliveries')

import sqlite3
import networkx as nx
import matplotlib.pyplot as plt

def create_batsmen_bowlers_graph(cursor):
    G = nx.Graph()

    # Get all distinct batsmen and bowlers from the deliveries table
    cursor.execute("SELECT DISTINCT batter_id FROM deliveries")
    batsmen_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT bowler_id FROM deliveries")
    bowlers_ids = [row[0] for row in cursor.fetchall()]

    # Add nodes to the graph for batsmen and bowlers
    G.add_nodes_from(batsmen_ids, node_type="batsman")
    G.add_nodes_from(bowlers_ids, node_type="bowler")

    # Query deliveries to get relationships between batsmen and bowlers
    cursor.execute("SELECT batter_id, bowler_id, COUNT(*) as count FROM deliveries GROUP BY batter_id, bowler_id")
    relationships = cursor.fetchall()

    # Add weighted edges to the graph based on the number of deliveries faced by each batsman against each bowler
    for batter_id, bowler_id, count in relationships:
        G.add_edge(batsmen_ids.index(batter_id), bowlers_ids.index(bowler_id), weight=count)

    return G

def find_bowler_registry(cursor, bowler_name):
    # Get bowler_id for the given bowler_name
    cursor.execute("SELECT registry FROM players WHERE name = ?", (bowler_name,))
    bowler_id = cursor.fetchone()

    if bowler_id:
        print(bowler_id[0])
        return bowler_id[0]
    else:
        print(f"Bowler {bowler_name} not found in the database.")
        return None

def plot_strike_rate_batsmen_against_bowler(cursor, bowler_name):
    # Find the registry ID of the given bowler
    bowler_id = find_bowler_registry(cursor, bowler_name)

    if bowler_id:
        # Query deliveries to get relationships between batsmen and the specified bowler
        cursor.execute("""
            SELECT players.name, 
                   COUNT(*) as deliveries, 
                   SUM(runs_batter) as total_runs
            FROM deliveries
            JOIN players ON players.registry = deliveries.batter_id
            WHERE deliveries.bowler_id = ?
            GROUP BY deliveries.batter_id
        """, (bowler_id,))

        # Fetch the results
        batsmen_stats = cursor.fetchall()

        # Check if any batsmen found
        if not batsmen_stats:
            print(f"No batsmen found who have faced {bowler_name}.")
            return

        # Calculate strike rates
        batsmen_strike_rates = []
        for batter_name, deliveries, total_runs in batsmen_stats:
            strike_rate = (total_runs / deliveries) * 100
            batsmen_strike_rates.append((batter_name, strike_rate))

        # Debugging print statement
        print("Batsmen Strike Rates Against", bowler_name, ":", batsmen_strike_rates)

        # Plot scatter plot
        batsmen, strike_rates = zip(*batsmen_strike_rates)
        plt.scatter(batsmen, strike_rates, marker='o', color='green')
        plt.xlabel('Batsmen')
        plt.ylabel('Strike Rate')
        plt.title(f'Strike Rates of Batsmen against {bowler_name}')
        plt.xticks(rotation=90)
        plt.show()
    else:
        print(f"Bowler {bowler_name} not found in the database.")

def main():
    connection = sqlite3.connect('cricket_data.db')
    cursor = connection.cursor()

    # Your target bowler stored in a variable
    target_bowler = "R Ashwin"

    plot_strike_rate_batsmen_against_bowler(cursor, target_bowler)

    connection.close()

if __name__ == "__main__":
    main()

import sqlite3
from prettytable import PrettyTable

def execute_query_and_print(query):
    connection = sqlite3.connect('cricket_data.db')
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    print("OUTPUT :")
    if results:
        # Extract column names
        columns = [desc[0] for desc in cursor.description]

        # Create a PrettyTable instance
        table = PrettyTable(columns)

        # Add rows to the table 
    
        for row in results:
            table.add_row(row)

        # Print the table
        
        print(table)
    else:
        print("No results found.")

    connection.close()

# a. Win Records by Year and Gender
query_a = '''
WITH TeamWinCounts AS (
    SELECT
        m.season AS year,
        m.gender,
        m.winner AS team,
        COUNT(*) AS total_wins
    FROM
        matches m
    WHERE
        m.winner IS NOT NULL
    GROUP BY
        m.season,
        m.gender,
        m.winner
),
TeamTotalMatches AS (
    SELECT
        m.season AS year,
        m.gender,
        i.team,
        COUNT(DISTINCT m.id) AS total_matches
    FROM
        matches m
        JOIN innings i ON m.id = i.match_id
    GROUP BY
        m.season,
        m.gender,
        i.team
)
SELECT
    twc.year,
    twc.gender,
    twc.team,
    twc.total_wins,
    ttm.total_matches,
    ROUND(twc.total_wins * 100.0 / ttm.total_matches, 2) AS win_percentage
FROM
    TeamWinCounts twc
    INNER JOIN TeamTotalMatches ttm ON twc.year = ttm.year AND twc.gender = ttm.gender AND twc.team = ttm.team
ORDER BY
    twc.year DESC, twc.gender, win_percentage DESC;
'''

#execute_query_and_print(query_a)



# b. Highest Win Percentages in 2019 for each gender (displaying only the top team)
query_b = '''
-- Highest Win Percentages in 2019 for each gender (displaying only the top team)
WITH TeamsWinCounts AS (
    SELECT
        m.winner AS team,
        m.gender,
        COUNT(*) AS total_wins
    FROM
        matches m
    WHERE
        m.winner IS NOT NULL AND m.season = '2019'
    GROUP BY
        m.winner, m.gender
),
TeamsTotalMatches AS (
    SELECT
        m.gender,
        i.team,
        COUNT(DISTINCT m.id) AS total_matches
    FROM
        matches m
        JOIN innings i ON m.id = i.match_id
    WHERE
        m.season = '2019'
    GROUP BY
        m.gender, i.team
)
-- Calculate win percentage and select the team with the highest win percentage for each gender
SELECT
    t.gender,
    t.team,
    MAX(ROUND(t.total_wins * 100.0 / tm.total_matches, 2)) AS max_win_percentage
FROM
    TeamsWinCounts t
    JOIN TeamsTotalMatches tm ON t.gender = tm.gender AND t.team = tm.team
GROUP BY
    t.gender
ORDER BY
    t.gender;
'''

execute_query_and_print(query_b)



# c. Highest Strike Rates in 2019
query_c = '''
-- For players' strike rates in 2019
WITH PlayerStrikeRates AS (
    SELECT
        p.name AS player_name,
        SUM(d.runs_batter) AS total_runs,
        COUNT(*) AS total_balls,
        (SUM(d.runs_batter) * 100.0 / COUNT(*)) AS strike_rate
    FROM
        players p
    JOIN
        deliveries d ON p.registry = d.batter_id
    JOIN
        innings i ON d.inning_id = i.id
    JOIN
        matches m ON i.match_id = m.id
    WHERE
        m.season = '2019/20'
    GROUP BY
        p.name
    ORDER BY
        strike_rate DESC
    LIMIT 10  -- Adjust the limit based on how many top players you want to retrieve
)

-- Retrieve the top players' strike rates
SELECT
    psr.player_name,
    psr.strike_rate
FROM
    PlayerStrikeRates psr;


'''

execute_query_and_print(query_c)

