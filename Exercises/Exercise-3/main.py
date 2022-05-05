import boto3
from pathlib import Path
import gzip
import shutil
import logging

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

STORE_DIR = Path(__file__).resolve().parent / "downloads"
Path.mkdir(STORE_DIR, exist_ok=True, parents=True)

S3_BUCKET = "commoncrawl"
S3_FILE_KEY = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"


def main():
    logger.info(f"Downloading {S3_FILE_KEY} from S3 {S3_BUCKET} bucket...")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id="",
        aws_secret_access_key="",
    )
    s3_client.download_file(
        S3_BUCKET, S3_FILE_KEY, (STORE_DIR / "wet.paths.gz").as_posix()
    )

    with gzip.open((STORE_DIR / "wet.paths.gz").as_posix(), "rb") as f_in:
        with open((STORE_DIR / "wet.paths").as_posix(), "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    logger.info(f"{S3_FILE_KEY} downloaded from S3 {S3_BUCKET} bucket OK")

    with open((STORE_DIR / "wet.paths").as_posix(), "rb") as file:
        paths = file.readlines()

    for path in paths:
        logger.info(f"Downloading {path} file from {S3_BUCKET}...")
        s3_client.download_file(
            S3_BUCKET,
            path.decode().replace("\n", ""),
            (STORE_DIR / path.decode().replace("\n", "").split("/")[-1]).as_posix(),
        )


if __name__ == "__main__":
    main()
