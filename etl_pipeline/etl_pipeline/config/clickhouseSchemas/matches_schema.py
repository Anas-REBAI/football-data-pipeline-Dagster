MATCHES_SCHEMA = """
CREATE TABLE IF NOT EXISTS matches (
    match_id UInt32,
    match_date Date,
    kick_off String,
    competition_id UInt32,
    competition_name String,
    country_name String,
    season_id UInt32,
    season_name String,
    home_team_id UInt32,
    home_team_name String,
    home_team_gender LowCardinality(String),
    home_manager_id UInt32,
    home_manager_name String,
    away_team_id UInt32,
    away_team_name String,
    away_team_gender LowCardinality(String),
    away_manager_id UInt32,
    away_manager_name String,
    home_score UInt8,
    away_score UInt8,
    match_status LowCardinality(String),
    match_week UInt8,
    competition_stage_id UInt8,
    competition_stage_name String,
    stadium_id UInt32,
    stadium_name String,
    referee_id UInt32,
    referee_name String,
    metadata String CODEC(ZSTD),
    file_path String,
    load_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id);
"""


import json
BATCH_SIZE = 1000

def load_matches(ch, data, file_path):
    if not isinstance(data, list):
        data = [data]
    records = []
    for i, m in enumerate(data, 1):
        home_manager = m['home_team'].get('managers', [{}])[0]
        away_manager = m['away_team'].get('managers', [{}])[0]
        records.append({
            'match_id': m['match_id'],
            'match_date': m['match_date'],
            'kick_off': f"{m['match_date']} {m['kick_off']}",
            'competition_id': m['competition']['competition_id'],
            'competition_name': m['competition']['competition_name'],
            'country_name': m['competition']['country_name'],
            'season_id': m['season']['season_id'],
            'season_name': m['season']['season_name'],
            'home_team_id': m['home_team']['home_team_id'],
            'home_team_name': m['home_team']['home_team_name'],
            'home_team_gender': m['home_team']['home_team_gender'],
            'home_manager_id': home_manager.get('id'),
            'home_manager_name': home_manager.get('name'),
            'away_team_id': m['away_team']['away_team_id'],
            'away_team_name': m['away_team']['away_team_name'],
            'away_team_gender': m['away_team']['away_team_gender'],
            'away_manager_id': away_manager.get('id'),
            'away_manager_name': away_manager.get('name'),
            'home_score': m['home_score'],
            'away_score': m['away_score'],
            'match_status': m['match_status'],
            'match_week': m.get('match_week', 0),
            'competition_stage_id': m['competition_stage']['id'],
            'competition_stage_name': m['competition_stage']['name'],
            'stadium_id': m['stadium']['id'],
            'stadium_name': m['stadium']['name'],
            'referee_id': m['referee']['id'],
            'referee_name': m['referee']['name'],
            'metadata': json.dumps(m.get('metadata', {})),
            'file_path': file_path
        })
        if i % BATCH_SIZE == 0:
            ch.execute("INSERT INTO matches VALUES", records)
            records = []
    if records:
        ch.execute("INSERT INTO matches VALUES", records)
    return len(data)