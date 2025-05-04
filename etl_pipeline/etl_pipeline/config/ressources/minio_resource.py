from dagster import resource, check
from minio import Minio

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

