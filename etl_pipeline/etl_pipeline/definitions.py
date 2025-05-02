from dagster import Definitions, load_assets_from_modules
from etl_pipeline import assets
from etl_pipeline.config.minio_resource import minio_resource, clickhouse_resource
from etl_pipeline.config.settings import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB,
)

all_assets = load_assets_from_modules([assets])

resources = {
    "minio_client": minio_resource.configured({
        "endpoint": MINIO_ENDPOINT,
        "access_key": MINIO_ACCESS_KEY,
        "secret_key": MINIO_SECRET_KEY,
        "bucket": MINIO_BUCKET
        #"minio_bucket": MINIO_BUCKET
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
    resources=resources
)
