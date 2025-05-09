from dagster import Definitions, load_assets_from_modules, AssetExecutionContext
from pathlib import Path
import os
from dagster_dbt import DbtCliResource, dbt_assets
from etl_pipeline import assets
from etl_pipeline.config.ressources.minio_resource import minio_resource
from etl_pipeline.config.ressources.clickhouse_resource import clickhouse_resource
from etl_pipeline.config.ressources.dbt_resource import dbt_resource
from etl_pipeline.config.settings import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB,
    DBT_PROJECT_DIR, DBT_PROFILES_DIR
)
from etl_pipeline.utils.sensor_scheduler import daily_schedule, my_file_sensor, pipeline_status_alert

# Assets DBT
@dbt_assets(manifest=Path(DBT_PROJECT_DIR) / "target/manifest.json")
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    try:
        # Exécutez d'abord un test simple
        context.log.info("Starting DBT build...")
        # Si réussi, lancez le build complet
        yield from dbt.cli(["run"], context=context).stream()
        
    except Exception as e:
        context.log.error(f"DBT execution failed: {str(e)}")
        # Log des fichiers disponibles
        context.log.error(f"Files in project dir: {os.listdir(DBT_PROJECT_DIR)}")
        raise

# Load all assets
all_assets = load_assets_from_modules([assets])

resources = {
    "minio_client": minio_resource.configured({
        "endpoint": MINIO_ENDPOINT,
        "access_key": MINIO_ACCESS_KEY,
        "secret_key": MINIO_SECRET_KEY,
        "bucket": MINIO_BUCKET
    }),
    "clickhouse_client": clickhouse_resource.configured({
        "host": CLICKHOUSE_HOST,
        "port": CLICKHOUSE_PORT,
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "database": CLICKHOUSE_DB
    }),
    "dbt": dbt_resource.configured({
        "project_dir": DBT_PROJECT_DIR,
        "profiles_dir": DBT_PROFILES_DIR
    })
}

defs = Definitions(
    assets=[*all_assets, dbt_models],
    resources=resources,
    schedules=[daily_schedule],
    sensors=[my_file_sensor, pipeline_status_alert]
)
