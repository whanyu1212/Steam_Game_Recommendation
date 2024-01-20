import time
from typing import IO, Dict, Union

import yaml
from colorama import Fore, Style
from google.cloud import bigquery
from loguru import logger

from src.utils.setup_logger import log_error, log_info, log_success

# Constants
JOB_STATE_DONE = "DONE"
JOB_STATE_PENDING = "PENDING"
JOB_STATE_RUNNING = "RUNNING"


def map_time_unit(time_taken: float) -> str:
    """
    Map the time unit to the appropriate string.

    Args:
        time_taken (float): Time taken in seconds

    Returns:
        str: Time unit
    """
    if time_taken < 60:
        return "seconds"
    elif time_taken < 3600:
        return "minutes"
    else:
        return "hours"


def load_config(config_path: str) -> Dict:
    """Load the configuration file."""
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            log_info(f"Successfully loaded config from {config_path}")
            return config
    except Exception as e:
        log_error(f"Failed to load config from {config_path}: {str(e)}")
        exit(1)


def validate_config(config: Dict) -> None:
    """
    Check if the necessary config keys are present and have the correct
    type.

    Args:
        config (Dict): Config in dict format

    Raises:
        ValueError: If the key is not found
        TypeError: If the values are not of type str
        ValueError: if the values are empty
    """
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


def create_bigquery_client(credential_path: str) -> bigquery.Client:
    """
    Initialize the bigquery client.

    Args:
        credential_path (str): Path to the GCP service account key

    Returns:
        bigquery.Client: as the name suggests
    """
    return bigquery.Client.from_service_account_json(credential_path)


def create_job_config() -> bigquery.LoadJobConfig:
    """
    Iniatialize the job config for for uploading CSV.

    Returns:
        bigquery.LoadJobConfig: Config defining the job
    """
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    return job_config


def load_table_from_file(
    client: bigquery.Client,
    source_file: IO[bytes],
    table_ref: bigquery.TableReference,
    job_config: bigquery.LoadJobConfig,
) -> Union[bigquery.LoadJob, None]:
    try:
        job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )
        job.result()
        return job
    except Exception as e:
        logger.exception(
            f"Error: CSV export to Bigquery table failed with error {str(e)}"
        )
        return None


def handle_job_result(job: bigquery.LoadJob, csv_file_path: str) -> None:
    """
    Check the job state and log the result.

    Args:
        job (bigquery.LoadJob): Whether it is still running
        csv_file_path (str): path where the raw csv is stored
    """

    if job.state == JOB_STATE_DONE:
        log_success(
            f"CSV file '{csv_file_path}' successfully "
            f"exported to Bigquery table, job ID: {job.job_id}"
        )
    elif job.state in [JOB_STATE_PENDING, JOB_STATE_RUNNING]:
        log_info(
            f"Job {job.job_id} is still in progress,"
            f"current state: {job.state}"
        )
    else:
        log_error(f"Job {job.job_id} ended with state: {job.state}")


def _upload_csv_BQ(
    credential_path: str, dataset_id: str, table_id: str, csv_file_path: str
) -> None:
    """
    Trigger the upload of the CSV to Bigquery.

    Args:
        credential_path (str): GCP_auth_file_path
        dataset_id (str): dataset created on BQ
        table_id (str): name given to the new table
        csv_file_path (str): path where the raw csv is stored
    """
    client = create_bigquery_client(credential_path)
    log_info("BigQuery client initialized.")
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = create_job_config()

    # the file is opened in binary mode
    try:
        with open(csv_file_path, "rb") as source_file:
            log_info(f"Successfully opened file {csv_file_path}.")
            job = load_table_from_file(
                client, source_file, table_ref, job_config
            )
            if job is not None:
                handle_job_result(job, csv_file_path)
            else:
                log_error(
                    "Error: Export"
                    f"to Bigquery table failed for {csv_file_path}"
                )
    except (IOError, FileNotFoundError) as e:
        log_error(f"Failed to open file {csv_file_path}: {str(e)}")


if __name__ == "__main__":
    log_info("Starting the export to Bigquery")
    start = time.time()
    config = load_config("./cfg/config.yaml")

    validate_config(config)
    log_info("Config validated.")

    credential_path = config["gcp_auth_path"]
    dataset_id = config["dataset_id"]
    table_id = config["table_id"]
    csv_file_path = config["raw_filepath"]

    _upload_csv_BQ(credential_path, dataset_id, table_id, csv_file_path)
    end = time.time()
    time_taken = end - start
    time_unit = map_time_unit(time_taken)
    log_info(
        f"Process finished. Time taken: {Fore.GREEN}{time_taken} "
        f"{time_unit}{Style.RESET_ALL}. Exiting..."
    )
