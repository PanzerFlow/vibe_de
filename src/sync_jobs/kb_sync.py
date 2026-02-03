import logging
import os
import time

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from retry import retry

load_dotenv()

logger = logging.getLogger(__name__)

REGION = os.getenv("AWS_REGION", "us-east-1")

# Required identifiers
KNOWLEDGE_BASE_ID = os.getenv("AWS_KNOWLEDGE_BASE_ID")
DATA_SOURCE_ID = os.getenv("AWS_DATA_SOURCE_ID")

# Polling configuration
POLL_EVERY_SECONDS = 10
TIMEOUT_SECONDS = 30 * 60  # 30 minutes


@retry(
    ClientError,
    tries=2,
    delay=10,
    backoff=2,  # multiplier
)
def start_ingestion_job(client, knowledge_base_id: str, data_source_id: str) -> str:
    """
    Starts an ingestion job and returns the ingestionJobId.
    Same as calling this
    """
    resp = client.start_ingestion_job(
        knowledgeBaseId=knowledge_base_id,
        dataSourceId=data_source_id,
    )
    job_id = resp["ingestionJob"]["ingestionJobId"]
    status = resp["ingestionJob"]["status"]
    logger.info(f"Started ingestion job: {job_id} (initial status: {status})")
    return job_id


def get_ingestion_job_status(
    client, knowledge_base_id: str, data_source_id: str, ingestion_job_id: str
) -> dict:
    """
    Returns the ingestionJob object (includes status, timestamps, statistics, failure reasons).
    """
    resp = client.get_ingestion_job(
        knowledgeBaseId=knowledge_base_id,
        dataSourceId=data_source_id,
        ingestionJobId=ingestion_job_id,
    )
    return resp["ingestionJob"]


def wait_for_completion(
    client, knowledge_base_id: str, data_source_id: str, ingestion_job_id: str
) -> dict:
    """
    Polls until the job reaches a terminal state or times out.
    Terminal states are typically: COMPLETE / FAILED (and sometimes STOPPED).
    """
    start = time.time()

    terminal_states = {"COMPLETE", "FAILED", "STOPPED"}  # keep STOPPED just in case

    while True:
        job = get_ingestion_job_status(
            client, knowledge_base_id, data_source_id, ingestion_job_id
        )
        status = job["status"]

        logger.info(f"[{int(time.time() - start)}s] status={status}")

        # If you want more detail as it runs:
        # logger.debug(job)

        if status in terminal_states:
            return job

        if time.time() - start > TIMEOUT_SECONDS:
            raise TimeoutError(
                f"Ingestion job did not finish within {TIMEOUT_SECONDS} seconds."
            )

        time.sleep(POLL_EVERY_SECONDS)


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    client = boto3.client("bedrock-agent", region_name=REGION)

    try:
        job_id = start_ingestion_job(client, KNOWLEDGE_BASE_ID, DATA_SOURCE_ID)
        final_job = wait_for_completion(
            client, KNOWLEDGE_BASE_ID, DATA_SOURCE_ID, job_id
        )

        logger.info("Final result:")
        logger.info(f"  ingestionJobId: {final_job['ingestionJobId']}")
        logger.info(f"  status:         {final_job['status']}")

        # Helpful fields if present (depends on job + API evolution)
        for k in ("startedAt", "updatedAt", "statistics", "failureReasons"):
            if k in final_job:
                logger.info(f"  {k}: {final_job[k]}")

        if final_job["status"] != "COMPLETE":
            raise RuntimeError(f"Ingestion did not complete successfully: {final_job}")

    except ClientError:
        # Common causes: missing permissions, wrong region, wrong IDs, resource not found
        logger.error("AWS ClientError:", exc_info=True)
        raise
    except Exception:
        logger.error("Error:", exc_info=True)
        raise


if __name__ == "__main__":
    main()


# stats: 500 doc of 8kb each -> 16 mins
