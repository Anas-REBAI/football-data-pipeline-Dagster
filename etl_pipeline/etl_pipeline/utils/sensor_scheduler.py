import os
from dagster import define_asset_job, ScheduleDefinition, sensor, RunRequest
from etl_pipeline.utils.slack_notifications import send_slack_message


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

# Trigger to monitor a folder
@sensor(job=full_pipeline)
def my_file_sensor(context):
    for file in os.listdir("etl_pipeline"):
        if file.endswith(".py"):
            yield RunRequest(run_key=file)

# Trigger to monitor pipeline status
@sensor(job=full_pipeline)
def pipeline_status_alert(context):
    """Sensor that checks for pipeline run status and sends a Slack alert."""
    instance = context.instance
    run_records = instance.get_run_records(limit=1)  # Get the latest run

    if not run_records:
        return  # No runs found

    # Use `dagster_run` instead of `pipeline_run`
    latest_run = run_records[0].dagster_run  # Get the run
    run_status = latest_run.status  # Pipeline status
    pipeline_name = latest_run.job_name  # Pipeline name
    run_id = latest_run.run_id  # Get the run ID

    if run_status.value == "SUCCESS":  # Check if the run was successful
        send_slack_message(
            f"✅ Pipeline `{pipeline_name}` completed successfully! 🎉\n"
            f"• **Run ID**: `{run_id}`\n"
            f"• **Status**: `{run_status}`"
        )
    elif run_status.value == "FAILURE":  # Check if the run failed
        send_slack_message(
            f"❌ Pipeline `{pipeline_name}` failed. Check the logs. 🔥\n"
            f"• **Run ID**: `{run_id}`\n"
            f"• **Status**: `{run_status}`"
        )

    return []