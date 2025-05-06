import json
import pandas as pd
from dagster import asset, Output, MetadataValue
from etl_pipeline.config.settings import MINIO_BUCKET

def flatten_three_sixty(event: dict) -> list[dict]:
    event_uuid = event.get("event_uuid")
    visible_area = event.get("visible_area", [])
    freeze_frame = event.get("freeze_frame", [])

    records = []
    for i, player in enumerate(freeze_frame):
        records.append({
            "event_uuid": event_uuid or "",
            "freeze_index": i,
            "teammate": player.get("teammate", False),
            "actor": player.get("actor", False),
            "keeper": player.get("keeper", False),
            "x": (player.get("location") or [0.0, 0.0])[0],
            "y": (player.get("location") or [0.0, 0.0])[1],
            "visible_area": json.dumps(visible_area)
        })
    return records

@asset(required_resource_keys={"minio_client", "clickhouse_client"})
def load_three_sixty_to_clickhouse(context):
    """
    Charge les fichiers three_sixty/*.json depuis MinIO et les insère dans ClickHouse.
    """
    minio_client = context.resources.minio_client
    clickhouse = context.resources.clickhouse_client

    objects = minio_client.list_objects(MINIO_BUCKET, prefix="data/three-sixty", recursive=True)
    total_files = 0
    inserted_rows = 0

    clickhouse.execute('''
    CREATE TABLE IF NOT EXISTS football_statsbomb.three_sixty (
        event_uuid String,
        freeze_index UInt16,
        teammate UInt8,
        actor UInt8,
        keeper UInt8,
        x Float32,
        y Float32,
        visible_area String
    ) ENGINE = MergeTree()
    ORDER BY (event_uuid, freeze_index)
    ''')

    for obj in objects:
        if not obj.object_name.endswith(".json"):
            continue
        try:
            data = minio_client.get_object(MINIO_BUCKET, obj.object_name).read()
            raw_json = json.loads(data)

            records = []
            for event in raw_json:
                records.extend(flatten_three_sixty(event))

            df = pd.DataFrame(records)
            df = df.astype({
                "freeze_index": "int",
                "teammate": "int",
                "actor": "int",
                "keeper": "int",
                "x": "float32",
                "y": "float32"
            })

            clickhouse.insert_dataframe("INSERT INTO football_statsbomb.three_sixty VALUES", df)
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
