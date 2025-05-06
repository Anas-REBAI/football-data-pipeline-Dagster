import json
import pandas as pd
from dagster import asset, Output, MetadataValue
from etl_pipeline.config.settings import MINIO_BUCKET


def flatten_three_sixty(item: dict) -> list[dict]:
    event_uuid = item.get("event_uuid")
    visible_area = json.dumps(item.get("visible_area", []))

    records = []
    for freeze in item.get("freeze_frame", []):
        location = freeze.get("location")
        if not location or len(location) != 2:
            continue

        records.append({
            "event_uuid": event_uuid,
            "visible_area": visible_area,
            "teammate": int(freeze.get("teammate", False)),
            "actor": int(freeze.get("actor", False)),
            "keeper": int(freeze.get("keeper", False)),
            "location_x": float(location[0]),
            "location_y": float(location[1]),
        })

    return records


@asset(required_resource_keys={"minio_client", "clickhouse_client"})
def load_three_sixty_to_clickhouse(context):
    minio_client = context.resources.minio_client
    clickhouse = context.resources.clickhouse_client

    objects = minio_client.list_objects(MINIO_BUCKET, prefix="data/three_sixty", recursive=True)
    total_files = 0
    inserted_rows = 0

    clickhouse.execute('''
    CREATE TABLE IF NOT EXISTS football_statsbomb.three_sixty (
        event_uuid UUID,
        visible_area String,
        teammate UInt8,
        actor UInt8,
        keeper UInt8,
        location_x Float64,
        location_y Float64
    ) ENGINE = MergeTree()
    ORDER BY (event_uuid, location_x, location_y)
    ''')

    for obj in objects:
        if not obj.object_name.endswith(".json"):
            continue
        try:
            data = minio_client.get_object(MINIO_BUCKET, obj.object_name).read()
            raw_json = json.loads(data)

            records = []
            for item in raw_json:
                records.extend(flatten_three_sixty(item))

            df = pd.DataFrame(records)
            if not df.empty:
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
            "preview": MetadataValue.md(f"📸 {inserted_rows} lignes insérées depuis {total_files} fichiers")
        }
    )
