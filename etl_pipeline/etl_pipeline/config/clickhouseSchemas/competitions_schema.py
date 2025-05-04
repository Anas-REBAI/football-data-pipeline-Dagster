COMPETITIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS competitions (
    competition_id UInt32,
    season_id UInt32,
    country_name String,
    competition_name String,
    competition_gender String,
    competition_youth UInt8 DEFAULT 0,
    competition_international UInt8 DEFAULT 0,
    season_name String,
    match_updated DateTime,
    match_available DateTime,
    file_path String,
    load_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (season_id, competition_id);
"""



def load_competitions(ch, data, file_path):
    if not isinstance(data, list):
        data = [data]
    records = [{
        'competition_id': c['competition_id'],
        'season_id': c['season_id'],
        'country_name': c['country_name'],
        'competition_name': c['competition_name'],
        'competition_gender': c['competition_gender'],
        'competition_youth': c['competition_youth'],
        'competition_international': c['competition_international'],
        'season_name': c['season_name'],
        'match_updated': c['match_updated'],
        'match_available': c['match_available'],
        'file_path': file_path
    } for c in data]
    ch.execute("INSERT INTO competitions VALUES", records)
    return len(data)