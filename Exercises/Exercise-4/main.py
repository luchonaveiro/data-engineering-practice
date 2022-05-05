import glob
import logging
import pandas as pd
from pandas.io.json import json_normalize
import json
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent


def main():
    logger.info("Searching all the .json files...")
    json_files = glob.glob("data/**/*.json", recursive=True)

    for json_file in json_files:
        logger.info(
            f"Transforming {json_file} to {json_file.replace('.json', 'csv')}..."
        )
        with open((BASE_DIR / json_file).as_posix(), "r") as file:
            loaded_file = json.load(file)

        df = json_normalize(loaded_file)
        df.to_csv(BASE_DIR / json_file.replace(".json", ".csv"), index=False)


if __name__ == "__main__":
    main()
