LINEUPS_SCHEMA = """
CREATE TABLE IF NOT EXISTS lineups (
    lineup_id String,
    match_id UInt32,
    team_id UInt32,
    team_name String,
    player_id UInt32,
    player_name String,
    player_nickname Nullable(String),
    jersey_number UInt8,
    country_id UInt32,
    country_name String,
    positions String CODEC(ZSTD),
    cards String CODEC(ZSTD),
    file_path String,
    load_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, team_id, player_id);
"""


import json
import uuid
def load_lineups(ch, data, file_path):
    records = []
    for lineup in data:
        for player in lineup['lineup']:
            records.append({
                'lineup_id': str(uuid.uuid4()),
                'match_id': extract_match_id_from_path(file_path),
                'team_id': lineup['team_id'],
                'team_name': lineup['team_name'],
                'player_id': player['player_id'],
                'player_name': player['player_name'],
                'player_nickname': player.get('player_nickname'),
                'jersey_number': player['jersey_number'],
                'country_id': player['country']['id'],
                'country_name': player['country']['name'],
                'positions': json.dumps(player.get('positions', [])),
                'cards': json.dumps(player.get('cards', [])),
                'file_path': file_path
            })
    ch.execute("INSERT INTO lineups VALUES", records)
    return len(records)

import re
def extract_match_id_from_path(path):
    match = re.search(r'(\d+)\.json$', path)
    if not match:
        raise ValueError(f"Impossible d'extraire le match_id du chemin: {path}")
    return int(match.group(1))