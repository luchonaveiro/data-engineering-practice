import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging
from pathlib import Path, PosixPath
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

STORE_DIR = Path(__file__).resolve().parent / "downloads"
Path.mkdir(STORE_DIR, exist_ok=True, parents=True)

URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

INTERESTED_FILE = "2022-02-07 14:03"


def search_id_from_html(html: str, interested_date: str) -> str:
    """Searches the ID from the interested date, on the HTML file."""
    S = BeautifulSoup(html, "lxml")
    trs = S.find_all("tr")
    for tr in trs:
        tds = tr.find_all("td")
        if len(tds) >= 2:
            file_date = tds[1].text.strip()
            if file_date == interested_date:
                file_id = tr.find_all("a")[0].text
                break
    return file_id


def create_download_url(file_id: str) -> str:
    """Create the URL to download the file, given a file_id"""
    download_url = f"{URL}{file_id}"
    return download_url


def download_file(download_url: str, save_path: PosixPath) -> None:
    """Download file from URL and store it on the file system."""
    req = requests.get(download_url)

    if req.status_code == 200:
        downloaded_file = req.text
        with open(save_path.as_posix(), "w") as file:
            file.write(downloaded_file)
    else:
        raise ValueError(f"Error while downloading file from {download_url}")


def get_highest_hourly_dry_bulb_temperature(df: pd.DataFrame) -> np.int64:
    """Gets the maximum HourlyDryBulbTemperature value from the data frame"""
    return df["HourlyDryBulbTemperature"].max()


def get_highest_hourly_dry_bulb_temperature_records(df: pd.DataFrame) -> pd.DataFrame:
    """Gets the records from the data frame with the highest HourlyDryBulbTemperature value"""
    max_value = get_highest_hourly_dry_bulb_temperature(df)
    filtered_df = df.query(f"HourlyDryBulbTemperature == {max_value}")
    return filtered_df


def main():
    logger.info(f"Downloading data from {URL}...")
    req = requests.get(URL)
    if req.status_code != 200:
        raise ValueError(f"Error while downloading file from {URL}")
    logger.info(f"Parsing HTML response looking for {INTERESTED_FILE} file ID...")
    file_id = search_id_from_html(req.text, INTERESTED_FILE)
    download_url = create_download_url(file_id)
    logger.info(f"Downolading interested file from {download_url}...")
    download_file(download_url, STORE_DIR / file_id)
    downloaded_df = pd.read_csv(STORE_DIR / file_id)
    result_df = get_highest_hourly_dry_bulb_temperature_records(downloaded_df)
    logger.info(result_df)


if __name__ == "__main__":
    main()
