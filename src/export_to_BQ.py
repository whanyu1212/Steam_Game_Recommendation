from typing import Dict

import yaml
from google.cloud import bigquery
from loguru import logger

# Constants
JOB_STATE_DONE = "DONE"
JOB_STATE_PENDING = "PENDING"
JOB_STATE_RUNNING = "RUNNING"

logger.add("./logs/logs_for_testing.log")


def validate_config(config: Dict):
    required_keys = ["gcp_auth_path", "dataset_id", "table_id", "raw_filepath"]

    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")

        value = config[key]
        if not isinstance(value, str):
            raise TypeError(
                "Expected string for config key {key},"
                f" but got {type(value).__name__}"
            )

        if not value:
            raise ValueError(f"Config key {key} must not be empty")


def create_bigquery_client(credential_path: str):
    return bigquery.Client.from_service_account_json(credential_path)


def create_job_config():
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    return job_config


def load_table_from_file(client, source_file, table_ref, job_config):
    try:
        job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )
        job.result()
        return job
    except Exception as e:
        # Log the full exception with stack trace
        logger.exception(
            f"Error: CSV export to Bigquery table failed with error {str(e)}"
        )
        return None


def handle_job_result(job, csv_file_path):
    if job.state == JOB_STATE_DONE:
        logger.success(
            f"CSV file '{csv_file_path}' successfully"
            "exported to Bigquery table, job ID: {job.job_id}"
        )
    elif job.state in [JOB_STATE_PENDING, JOB_STATE_RUNNING]:
        logger.info(
            f"Job {job.job_id} is still in progress,"
            f"current state: {job.state}"
        )
    else:
        logger.error(f"Job {job.job_id} ended with state: {job.state}")


def _upload_csv_BQ(
    credential_path: str, dataset_id: str, table_id: str, csv_file_path: str
):
    client = create_bigquery_client(credential_path)
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = create_job_config()

    with open(csv_file_path, "rb") as source_file:
        job = load_table_from_file(client, source_file, table_ref, job_config)
        if job is not None:
            handle_job_result(job, csv_file_path)


if __name__ == "__main__":
    with open("./cfg/config.yaml", "r") as f:
        config = yaml.safe_load(f)
    validate_config(config)

    credential_path = config["gcp_auth_path"]
    dataset_id = config["dataset_id"]
    table_id = config["table_id"]
    csv_file_path = config["raw_filepath"]

    _upload_csv_BQ(credential_path, dataset_id, table_id, csv_file_path)
