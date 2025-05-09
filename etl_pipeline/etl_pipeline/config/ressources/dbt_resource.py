from dagster import resource
from dagster_dbt import DbtCliResource

@resource
def dbt_resource(init_context):
    """Resource configurable pour DBT"""
    return DbtCliResource(
        project_dir=init_context.resource_config["project_dir"],
        profiles_dir=init_context.resource_config["profiles_dir"],
    )