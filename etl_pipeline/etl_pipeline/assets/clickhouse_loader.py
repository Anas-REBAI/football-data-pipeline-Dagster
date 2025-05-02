import json
import pandas as pd
from dagster import asset, Output, MetadataValue
from etl_pipeline.config.settings import MINIO_BUCKET

@asset(required_resource_keys={"minio_client", "clickhouse_client"})
def load_minio_to_clickhouse(context):
    """
    Charge les fichiers JSON depuis MinIO et les insère dans des tables séparées dans ClickHouse
    selon leur type : events, matches, lineups, three-sixty, competitions.
    """
    minio_client = context.resources.minio_client
    clickhouse = context.resources.clickhouse_client

    path_to_table = {
        "data/events/": "raw_events",
        "data/matches/": "raw_matches",
        "data/lineups/": "raw_lineups",
        "data/three-sixty/": "raw_three_sixty",
        "data/competitions.json": "raw_competitions"
    }

    # Crée les tables si elles n'existent pas
    for table in path_to_table.values():
        clickhouse.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                file_path String,
                sha256 String,
                raw_json String
            ) ENGINE = MergeTree() ORDER BY sha256
        """)

    inserted = {table: 0 for table in path_to_table.values()}
    failed_files = []

    objects = minio_client.list_objects(MINIO_BUCKET, recursive=True)

    for obj in objects:
        object_name = obj.object_name
        matched_table = None

        for prefix, table in path_to_table.items():
            if object_name.startswith(prefix):
                matched_table = table
                break

        if not matched_table:
            continue  # Ignore les fichiers non ciblés

        try:
            response = minio_client.get_object(MINIO_BUCKET, object_name)
            content = response.read()
            sha256 = obj.metadata.get("x-amz-meta-sha256") or "unknown"

            json_data = json.loads(content)

            if isinstance(json_data, list):
                rows = [(object_name, sha256, json.dumps(item)) for item in json_data]
            else:
                rows = [(object_name, sha256, json.dumps(json_data))]

            clickhouse.execute(f"""
                INSERT INTO {matched_table} (file_path, sha256, raw_json) VALUES
            """, rows)

            inserted[matched_table] += len(rows)

        except Exception as e:
            failed_files.append({"file": object_name, "error": str(e)})
            context.log.error(f"❌ Échec sur {object_name}: {e}")

    # Résumé Markdown
    df_summary = pd.DataFrame.from_dict(inserted, orient="index", columns=["inserted_rows"])
    summary_md = df_summary.to_markdown()

    return Output(
        value=None,
        metadata={
            "inserted_rows_per_table": MetadataValue.md(summary_md),
            "failed_files": failed_files
        }
    )
