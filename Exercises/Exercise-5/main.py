import psycopg2
import logging
import os
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


def get_conn():
    logger.info("Creating Database Connection...")
    conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="postgres",
        host="postgres",
        port=5432,
    )
    cur = conn.cursor()
    logger.info("Connection to Database stablished")
    return conn, cur


def main():
    conn, cur = get_conn()
    logger.info("Creating tables on DB...")
    with open("db/init.sql", "r") as f:
        create_tables = f.read()
    cur.execute(create_tables)
    conn.commit()
    logger.info("Tables created in DB")

    for file in os.listdir("data"):

        data = pd.read_csv(f"data/{file}")
        tablename = file.replace(".csv", "")

        logger.info(f"Uploading {len(data)} records to the table {tablename}...")
        args_str = b",".join(
            cur.mogrify("(" + "%s," * (len(data.columns) - 1) + "%s)", x)
            for x in tuple(map(tuple, data.values))
        )
        cur.execute(
            f"INSERT INTO exercise_5.{tablename} VALUES " + args_str.decode("utf-8")
        )
        conn.commit()
        logger.info(f"{tablename} data uploaded to the Database")


if __name__ == "__main__":
    main()
