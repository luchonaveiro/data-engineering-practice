import logging
import requests
from pathlib import Path
import zipfile
import os
from concurrent.futures import ProcessPoolExecutor

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

STORE_DIR = Path(__file__).resolve().parent / "downloads"
Path.mkdir(STORE_DIR, exist_ok=True, parents=True)


def get_filename_from_url(url: str) -> str:
    """Get filename from URL."""
    file_name = url.split("/")[-1]
    return file_name


def download_url(url: str, chunk_size: int = 128) -> None:
    """Download .zip from URL and store it on file system."""
    logger.info(f"Downloading {url}...")
    r = requests.get(url, stream=True)

    file_name = get_filename_from_url(url)
    with open(STORE_DIR / file_name, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

    logger.info(f"{url} downloaded OK")


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():

    with ProcessPoolExecutor(max_workers=None) as executor:
        executor.map(download_url, download_uris)

    for url in download_uris:
        try:
            file_name = get_filename_from_url(url)

            with zipfile.ZipFile(STORE_DIR / file_name, "r") as zip_ref:
                zip_ref.extract(
                    file_name.replace(".zip", ".csv"),
                    STORE_DIR,
                )

            os.remove(STORE_DIR / file_name)

        except Exception as e:
            logger.error(f"Error while downloading {url}: {e}")


if __name__ == "__main__":
    main()
