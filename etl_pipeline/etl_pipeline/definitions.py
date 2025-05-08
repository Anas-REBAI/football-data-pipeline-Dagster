from dagster import Definitions, define_asset_job, ScheduleDefinition, load_assets_from_modules
from etl_pipeline import assets
from etl_pipeline.config.ressources.minio_resource import minio_resource
from etl_pipeline.config.ressources.clickhouse_resource import clickhouse_resource
from etl_pipeline.config.settings import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB
)

# Load all assets
all_assets = load_assets_from_modules([assets])

# Order explicit asset execution
full_pipeline = define_asset_job(
    name="full_data_pipeline",
    selection=[
        "transfer_statsbomb_to_minio",
        "load_events_to_clickhouse",
        "load_lineups_to_clickhouse",
        "load_three_sixty_to_clickhouse",
        "load_competitions_to_clickhouse",
        "load_matches_to_clickhouse"
    ]
)

# Optionally, schedule the job
daily_schedule = ScheduleDefinition(
    job=full_pipeline,
    cron_schedule="0 2 * * *",  # Every day at 2AM
    name="daily_data_pipeline_schedule"
)

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
    })
}

defs = Definitions(
    assets=all_assets,
    resources=resources,
    jobs=[full_pipeline],
    schedules=[daily_schedule]
)
