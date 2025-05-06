import json
import pandas as pd
from dagster import asset, Output, MetadataValue
from etl_pipeline.config.settings import MINIO_BUCKET

def flatten_lineup(lineup: dict) -> list[dict]:
    team_id = lineup.get("team_id", 0)
    team_name = lineup.get("team_name", "UNKNOWN_TEAM")
    players = lineup.get("lineup", [])

    records = []
    for player in players:
        base = {
            "team_id": team_id or 0,
            "team_name": team_name or "UNKNOWN_TEAM",
            "player_id": player.get("player_id") or 0,
            "player_name": player.get("player_name") or "UNKNOWN_PLAYER",
            "jersey_number": player.get("jersey_number") or 0,
            "country": player.get("country", {}).get("name") or "UNKNOWN_COUNTRY",
        }

        positions = player.get("positions") or []
        if not positions:
            base.update({
                "position_id": 0,
                "position_name": "UNKNOWN_POSITION"
            })
            records.append(base)
        else:
            for pos in positions:
                record = base.copy()
                record.update({
                    "position_id": pos.get("position_id") or 0,
                    "position_name": pos.get("position") or "UNKNOWN_POSITION"
                })
                records.append(record)
    return records

@asset(required_resource_keys={"minio_client", "clickhouse_client"})
def load_lineups_to_clickhouse(context):
    """
    Charge les fichiers lineups/*.json depuis MinIO et les insère dans ClickHouse.
    """
    minio_client = context.resources.minio_client
    clickhouse = context.resources.clickhouse_client

    objects = minio_client.list_objects(MINIO_BUCKET, prefix="data/lineups", recursive=True)
    total_files = 0
    inserted_rows = 0

    clickhouse.execute('''
    CREATE TABLE IF NOT EXISTS football_statsbomb.lineups (
        team_id UInt32,
        team_name String,
        player_id UInt32,
        player_name String,
        position_id UInt8,
        position_name String,
        jersey_number UInt8,
        country String
    ) ENGINE = MergeTree()
    ORDER BY (team_id, player_id)
    ''')

    for obj in objects:
        if not obj.object_name.endswith(".json"):
            continue
        try:
            data = minio_client.get_object(MINIO_BUCKET, obj.object_name).read()
            raw_json = json.loads(data)

            records = []
            for lineup in raw_json:
                records.extend(flatten_lineup(lineup))

            df = pd.DataFrame(records)

            numeric_columns = ["team_id", "player_id", "position_id", "jersey_number"]
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            df.fillna({
                "team_id": 0,
                "player_id": 0,
                "position_id": 0,
                "jersey_number": 0,
                "team_name": "",
                "player_name": "",
                "position_name": "",
                "country": ""
            }, inplace=True)

            df = df.astype({
                "team_id": "int",
                "player_id": "int",
                "position_id": "int",
                "jersey_number": "int"
            })

            clickhouse.insert_dataframe("INSERT INTO football_statsbomb.lineups VALUES", df)
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
