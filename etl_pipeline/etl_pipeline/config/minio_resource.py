from dagster import resource, check
from minio import Minio
from clickhouse_driver import Client as ClickhouseClient

@resource
def minio_resource(init_context):
    """
    Resource that provides a configured MinIO client.
    """
    try:
        # Validate configuration
        check.str_param(init_context.resource_config["endpoint"], "endpoint")
        check.str_param(init_context.resource_config["access_key"], "access_key")
        check.str_param(init_context.resource_config["secret_key"], "secret_key")
        
        # Initialize client
        client = Minio(
            init_context.resource_config["endpoint"],
            access_key=init_context.resource_config["access_key"],
            secret_key=init_context.resource_config["secret_key"],
            secure=False,
        )
        
        # Test connection
        client.list_buckets()  # Will raise if connection fails
        return client
    except Exception as e:
        init_context.log.error(f"MinIO resource configuration error: {str(e)}")
        raise


@resource
def clickhouse_resource(init_context):
    """Resource for ClickHouse client"""
    try:
        check.str_param(init_context.resource_config["host"], "host")
        check.int_param(init_context.resource_config["port"], "port")
        
        client = ClickhouseClient(
            host=init_context.resource_config["host"],
            port=init_context.resource_config["port"],
            user=init_context.resource_config.get("user", "default"),
            password=init_context.resource_config.get("password", ""),
            database=init_context.resource_config.get("database", "default"),
            settings={'use_numpy': True}
        )
        client.execute("SELECT 1")
        return client
    except Exception as e:
        init_context.log.error(f"ClickHouse connection failed: {str(e)}")
        raise
