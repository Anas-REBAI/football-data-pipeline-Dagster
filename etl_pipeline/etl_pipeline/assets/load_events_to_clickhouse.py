import json
import pandas as pd
from dagster import asset, Output, MetadataValue
from etl_pipeline.config.settings import MINIO_BUCKET

def flatten_event(event: dict) -> dict:
    base = {
        "id": event.get("id"),
        "index": event.get("index"),
        "period": event.get("period"),
        "timestamp": event.get("timestamp"),
        "minute": event.get("minute"),
        "second": event.get("second"),
        "type_id": event.get("type", {}).get("id"),
        "type_name": event.get("type", {}).get("name"),
        "possession": event.get("possession"),
        "team_id": event.get("team", {}).get("id"),
        "team_name": event.get("team", {}).get("name"),
        "possession_team_id": event.get("possession_team", {}).get("id"),
        "possession_team_name": event.get("possession_team", {}).get("name"),
        "play_pattern_id": event.get("play_pattern", {}).get("id"),
        "play_pattern_name": event.get("play_pattern", {}).get("name"),
        "duration": event.get("duration"),
        "formation": event.get("tactics", {}).get("formation"),
    }
    return base

@asset(required_resource_keys={"minio_client", "clickhouse_client"})
def load_events_to_clickhouse(context):
    """
    Charge les fichiers events/*.json depuis MinIO et les insère dans ClickHouse.
    """
    minio_client = context.resources.minio_client
    clickhouse = context.resources.clickhouse_client
    
    objects = minio_client.list_objects(MINIO_BUCKET, prefix="data/events", recursive=True)
    total_files = 0
    inserted_rows = 0

    # Préparation ClickHouse
    clickhouse.execute('''
    CREATE TABLE IF NOT EXISTS football_statsbomb.events (
            id String,
            `index` UInt32,
            period UInt8,
            timestamp String,
            minute UInt8,
            second UInt8,
            type_id UInt16,
            type_name String,
            possession UInt16,
            team_id UInt16,
            team_name String,
            possession_team_id UInt16,
            possession_team_name String,
            play_pattern_id UInt16,
            play_pattern_name String,
            duration Float32,
            formation UInt16
        ) ENGINE = MergeTree()
        ORDER BY (id)
    ''')


    for obj in objects:
        if not obj.object_name.endswith(".json"):
            continue
        try:
            data = minio_client.get_object(MINIO_BUCKET, obj.object_name).read()
            raw_json = json.loads(data)

            records = [flatten_event(e) for e in raw_json]
            df = pd.DataFrame.from_records(records)

            # Conversion types ClickHouse
            df.fillna({"duration": 0.0, "formation": 0}, inplace=True)
            df = df.astype({
                "index": "int",
                "period": "int",
                "minute": "int",
                "second": "int",
                "type_id": "int",
                "possession": "int",
                "team_id": "int",
                "possession_team_id": "int",
                "play_pattern_id": "int",
                "formation": "int",
                "duration": "float"
            })

            clickhouse.insert_dataframe("INSERT INTO football_statsbomb.events VALUES", df)
            inserted_rows += len(df)
            total_files += 1
        except Exception as e:
            context.log.warning(f"Erreur lors de l'import de {obj.object_name}: {str(e)}")

    return Output(
        None,
        metadata={
            "fichiers traités": total_files,
            "lignes insérées": inserted_rows,
            "preview": MetadataValue.md(f"✅ {inserted_rows} lignes insérées depuis {total_files} fichiers")
        }
    )
