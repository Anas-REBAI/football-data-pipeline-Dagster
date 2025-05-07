import json
import pandas as pd
from dagster import asset, Output, MetadataValue
from etl_pipeline.config.settings import MINIO_BUCKET

def flatten_match(match: dict) -> dict:
    return {
        "match_id": match.get("match_id", 0),
        "match_date": match.get("match_date", ""),
        "kick_off": match.get("kick_off", ""),
        "competition_id": match.get("competition", {}).get("competition_id", 0),
        "competition_name": match.get("competition", {}).get("competition_name", ""),
        "competition_country": match.get("competition", {}).get("country_name", ""),
        "season_id": match.get("season", {}).get("season_id", 0),
        "season_name": match.get("season", {}).get("season_name", ""),
        "home_team_id": match.get("home_team", {}).get("home_team_id", 0),
        "home_team_name": match.get("home_team", {}).get("home_team_name", ""),
        "away_team_id": match.get("away_team", {}).get("away_team_id", 0),
        "away_team_name": match.get("away_team", {}).get("away_team_name", ""),
        "home_score": match.get("home_score", 0),
        "away_score": match.get("away_score", 0),
        "match_status": match.get("match_status", ""),
        "match_status_360": match.get("match_status_360", ""),
        "last_updated": match.get("last_updated", ""),
        "last_updated_360": match.get("last_updated_360", ""),
        "match_week": match.get("match_week", 0),
        "competition_stage": match.get("competition_stage", {}).get("name", ""),
        "stadium_id": match.get("stadium", {}).get("id", 0),
        "stadium_name": match.get("stadium", {}).get("name", ""),
        "referee_id": match.get("referee", {}).get("id", 0),
        "referee_name": match.get("referee", {}).get("name", "")
    }

@asset(required_resource_keys={"minio_client", "clickhouse_client"})
def load_matches_to_clickhouse(context):
    """
    Charge les fichiers matches/*.json depuis MinIO et les insère dans ClickHouse.
    """
    minio_client = context.resources.minio_client
    clickhouse = context.resources.clickhouse_client

    objects = minio_client.list_objects(MINIO_BUCKET, prefix="data/matches", recursive=True)
    total_files = 0
    inserted_rows = 0

    clickhouse.execute('''
    CREATE TABLE IF NOT EXISTS football_statsbomb.matches (
        match_id UInt32,
        match_date Date,
        kick_off String,
        competition_id UInt32,
        competition_name String,
        competition_country String,
        season_id UInt32,
        season_name String,
        home_team_id UInt32,
        home_team_name String,
        away_team_id UInt32,
        away_team_name String,
        home_score UInt8,
        away_score UInt8,
        match_status String,
        match_status_360 String,
        last_updated DateTime64,
        last_updated_360 DateTime64,
        match_week UInt8,
        competition_stage String,
        stadium_id UInt32,
        stadium_name String,
        referee_id UInt32,
        referee_name String
    ) ENGINE = MergeTree()
    ORDER BY match_id
    ''')

    for obj in objects:
        if not obj.object_name.endswith(".json"):
            continue
        try:
            data = minio_client.get_object(MINIO_BUCKET, obj.object_name).read()
            try:
                raw_json = json.loads(data)
            except json.JSONDecodeError as e:
                context.log.warning(f"Fichier JSON invalide: {obj.object_name} → {e}")
                continue

            records = [flatten_match(match) for match in raw_json]
            df = pd.DataFrame.from_records(records)

            df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce")
            df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
            df["last_updated_360"] = pd.to_datetime(df["last_updated_360"], errors="coerce")

            df.fillna("", inplace=True)

            clickhouse.insert_dataframe("INSERT INTO football_statsbomb.matches VALUES", df)
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
