THREE_SIXTY_SCHEMA = """
CREATE TABLE IF NOT EXISTS three_sixty (
    event_uuid String,
    match_id UInt32,
    event_id String,
    visible_area String CODEC(ZSTD),
    freeze_frame String CODEC(ZSTD),
    file_path String,
    load_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, event_id);
"""


import json
def load_three_sixty(ch, data, file_path):
    records = [{
        'event_uuid': str(e['event_uuid']),
        'match_id': extract_match_id_from_path(file_path),
        'event_id': e.get('event_id', 0),
        'visible_area': json.dumps(e['visible_area']),
        'freeze_frame': json.dumps(e['freeze_frame']),
        'file_path': file_path
    } for e in data]
    ch.execute("INSERT INTO three_sixty VALUES", records)
    return len(data)

import re
def extract_match_id_from_path(path):
    match = re.search(r'(\d+)\.json$', path)
    if not match:
        raise ValueError(f"Impossible d'extraire le match_id du chemin: {path}")
    return int(match.group(1))