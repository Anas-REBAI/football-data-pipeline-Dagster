EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id String,
    match_id UInt32,
    index UInt16,
    period UInt8,
    timestamp String,
    minute UInt8,
    second UInt8,
    `type` String CODEC(ZSTD),  # Échappé avec backticks
    possession UInt8,
    possession_team String CODEC(ZSTD),
    play_pattern String CODEC(ZSTD),
    team String CODEC(ZSTD),
    duration Float32,
    tactics String CODEC(ZSTD),
    under_pressure UInt8 DEFAULT 0,
    out UInt8 DEFAULT 0,
    extra String CODEC(ZSTD),
    file_path String,
    load_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, period, index)
"""


import json
def load_events(ch, data, file_path):
    records = []
    for e in data:
        try:
            record = {
                'event_id': str(e['id']),
                'match_id': e['match_id'],
                'index': e.get('index', 0),
                'period': e['period'],
                'timestamp': e['timestamp'],
                'minute': e['minute'],
                'second': e['second'],
                'type': json.dumps(e.get('type', {})),
                'possession': e.get('possession', 0),
                'possession_team': json.dumps(e.get('possession_team', {})),
                'play_pattern': json.dumps(e.get('play_pattern', {})),
                'team': json.dumps(e.get('team', {})),
                'duration': e.get('duration', 0.0),
                'tactics': json.dumps(e.get('tactics', {})),
                'under_pressure': 1 if e.get('under_pressure', False) else 0,
                'out': 1 if e.get('out', False) else 0,
                'extra': json.dumps(e.get('extra', {})),
                'file_path': file_path
            }
            records.append(record)
        except Exception as e:
            print(f"Error processing event: {e}")
            continue
    
    if records:
        try:
            ch.execute("INSERT INTO events VALUES", records)
        except Exception as e:
            print(f"Batch insert failed: {e}")
            # Fallback to single inserts
            for record in records:
                try:
                    ch.execute("INSERT INTO events VALUES", [record])
                except Exception as single_error:
                    print(f"Failed to insert single record: {single_error}")
    
    return len(records)