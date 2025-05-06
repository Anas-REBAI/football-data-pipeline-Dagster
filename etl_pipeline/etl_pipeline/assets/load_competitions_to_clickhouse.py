import json
import pandas as pd
from dagster import asset, Output, MetadataValue
from etl_pipeline.config.settings import MINIO_BUCKET

@asset(required_resource_keys={"minio_client", "clickhouse_client"})
def load_competitions_to_clickhouse(context):
    """
    Charge les fichiers competitions.json depuis MinIO et les insère dans ClickHouse.
    """
    minio_client = context.resources.minio_client
    clickhouse = context.resources.clickhouse_client

    objects = minio_client.list_objects(MINIO_BUCKET, prefix="data/competitions", recursive=True)
    total_files = 0
    inserted_rows = 0

    clickhouse.execute('''
    CREATE TABLE IF NOT EXISTS football_statsbomb.competitions (
        competition_id UInt32,
        season_id UInt32,
        country_name String,
        competition_name String,
        competition_gender String,
        competition_youth UInt8,
        competition_international UInt8,
        season_name String,
        match_updated DateTime64,
        match_updated_360 DateTime64,
        match_available_360 DateTime64,
        match_available DateTime64
    ) ENGINE = MergeTree()
    ORDER BY (competition_id, season_id)
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

            df = pd.DataFrame(raw_json)

            df.fillna("", inplace=True)
            df["competition_youth"] = df["competition_youth"].astype("bool").astype("uint8")
            df["competition_international"] = df["competition_international"].astype("bool").astype("uint8")

            df["competition_id"] = pd.to_numeric(df["competition_id"], errors="coerce").fillna(0).astype("int")
            df["season_id"] = pd.to_numeric(df["season_id"], errors="coerce").fillna(0).astype("int")

            for col in ["match_updated", "match_updated_360", "match_available_360", "match_available"]:
                df[col] = pd.to_datetime(df[col], errors="coerce")

            clickhouse.insert_dataframe("INSERT INTO football_statsbomb.competitions VALUES", df)
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
