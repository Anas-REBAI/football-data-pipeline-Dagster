from dagster import resource, check
from clickhouse_driver import Client as ClickhouseClient


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
