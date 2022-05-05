import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import window as W
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
import logging
from pathlib import Path
import zipfile
import io

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

STORE_DIR = Path(__file__).resolve().parent / "reports"
Path.mkdir(STORE_DIR, exist_ok=True, parents=True)


def get_average_trip_duration_per_day(
    df: pyspark.sql.dataframe.DataFrame,
) -> pyspark.sql.dataframe.DataFrame:
    """Calculates the average trip duration per day."""
    average_duration_per_day = (
        df.withColumn("date", F.to_date(F.col("start_time")))
        .withColumn("tripduration", F.col("tripduration").cast(DoubleType()))
        .groupby("date")
        .agg(F.mean(F.col("tripduration")).alias("average_tripduration"))
    )
    return average_duration_per_day


def get_daily_amount_of_trips(
    df: pyspark.sql.dataframe.DataFrame,
) -> pyspark.sql.dataframe.DataFrame:
    """Calculates the amount of trips taken each day."""
    trips_per_day = (
        df.withColumn("date", F.to_date(F.col("start_time")))
        .groupby("date")
        .agg(F.count("*").alias("trips_amount"))
    )
    return trips_per_day


def get_most_popular_station_per_month(
    df: pyspark.sql.dataframe.DataFrame,
) -> pyspark.sql.dataframe.DataFrame:
    """Calculates the most popular starting trip station for each month."""
    popular_station_per_month = (
        df.withColumn("date", F.to_date(F.col("start_time")))
        .withColumn(
            "month", F.concat_ws("-", F.year(F.col("date")), F.month(F.col("date")))
        )
        .groupby("month", "from_station_name")
        .agg(F.count("*").alias("trips_amount"))
        .withColumn(
            "monthly_rank",
            F.row_number().over(
                W.Window()
                .partitionBy(F.col("month"))
                .orderBy(F.col("trips_amount").desc())
            ),
        )
        .filter(F.col("monthly_rank") == F.lit(1))
    )
    return popular_station_per_month


def get_top_3_daily_stations_last_2_weeks(
    df: pyspark.sql.dataframe.DataFrame,
) -> pyspark.sql.dataframe.DataFrame:
    """Calculates the top 3 trip stations each day for the last two weeks."""
    top_3_daily_stations_last_2_weeks = (
        df.withColumn("date", F.to_date(F.col("start_time")))
        .groupby("date", "from_station_name")
        .agg(F.count("*").alias("trips_amount"))
        .withColumn("year", F.year(F.col("date")))
        .withColumn("week_of_year", F.weekofyear(F.col("date")))
        .withColumn(
            "year_rank", F.dense_rank().over(W.Window().orderBy(F.col("year").desc()))
        )
        .withColumn(
            "week_rank",
            F.dense_rank().over(
                W.Window()
                .partitionBy(F.col("year"))
                .orderBy(F.col("week_of_year").desc())
            ),
        )
        .filter(F.col("year_rank") == F.lit(1))
        .filter(F.col("week_rank").isin([1, 2]))
        .withColumn(
            "daily_rank",
            F.row_number().over(
                W.Window()
                .partitionBy(F.col("date"))
                .orderBy(F.col("trips_amount").desc())
            ),
        )
        .filter(F.col("daily_rank").isin([1, 2, 3]))
    )
    return top_3_daily_stations_last_2_weeks.select(
        "date", "from_station_name", "trips_amount", "daily_rank"
    )


def get_trip_duration_per_gender(
    df: pyspark.sql.dataframe.DataFrame,
) -> pyspark.sql.dataframe.DataFrame:
    """Calculates the average trip duration per gender."""
    average_duration_per_gender = (
        df.withColumn("tripduration", F.col("tripduration").cast(DoubleType()))
        .groupby("gender")
        .agg(F.mean(F.col("tripduration")).alias("average_tripduration"))
    )
    return average_duration_per_gender


def get_top_10_ages_per_longer_and_shorter_trips(
    df: pyspark.sql.dataframe.DataFrame,
) -> pyspark.sql.dataframe.DataFrame:
    """Calculates the top 10 ages of those that take the longest and shortest trips."""
    top_ages = (
        df.withColumn("tripduration", F.col("tripduration").cast(DoubleType()))
        .filter(F.col("tripduration").isNotNull())
        .withColumn(
            "longest_duration",
            F.dense_rank().over(W.Window().orderBy(F.col("tripduration").desc())),
        )
        .withColumn(
            "longest_duration",
            F.when(F.col("longest_duration") == F.lit(1), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "shortest_duration",
            F.dense_rank().over(W.Window().orderBy(F.col("tripduration").asc())),
        )
        .withColumn(
            "shortest_duration",
            F.when(F.col("shortest_duration") == F.lit(1), F.lit(1)).otherwise(
                F.lit(0)
            ),
        )
        .filter(
            (F.col("longest_duration") == F.lit(1))
            | (F.col("shortest_duration") == F.lit(1))
        )
        .withColumn("date", F.to_date(F.col("start_time")))
        .withColumn("age", F.year(F.col("date")) - F.col("birthyear"))
        .groupby("longest_duration", "shortest_duration", "age")
        .agg(F.count("*").alias("trips_amount"))
        .withColumn(
            "rank_ages",
            F.dense_rank().over(
                W.Window()
                .partitionBy(F.col("longest_duration"))
                .orderBy(F.col("trips_amount").desc())
            ),
        )
        .filter(F.col("rank_ages") <= F.lit(10))
    )
    return top_ages


def zip_extract(x):
    in_memory_data = io.BytesIO(x[1])
    file_obj = zipfile.ZipFile(in_memory_data, "r")
    files = [i for i in file_obj.namelist()]
    return dict(zip(files, [file_obj.open(file).read() for file in files]))


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    # zips = spark.sparkContext.binaryFiles("data/Divvy_Trips_2019_Q4.zip")
    # data_rdd = zips.map(zip_extract)
    # df = data_rdd.toDF()
    # data = spark.read.csv(
    #     ["data/Divvy_Trips_2019_Q4.csv", "data/Divvy_Trips_2020_Q1.csv"],
    #     header="true",
    #     inferSchema="true",
    # )
    data = spark.read.csv(
        "data/Divvy_Trips_2019_Q4.csv",
        header="true",
        inferSchema="true",
    )

    logger.info("Calculating the average duration per day...")
    average_duration_per_day = get_average_trip_duration_per_day(data)
    average_duration_per_day.write.mode("overwrite").csv(
        (STORE_DIR / "average_duration_per_day").as_posix()
    )
    logger.info(
        f'Average duration per day stored on {(STORE_DIR / "average_duration_per_day").as_posix()}'
    )

    logger.info("Calculating the daily amount of trips...")
    trips_per_day = get_daily_amount_of_trips(data)
    trips_per_day.write.mode("overwrite").csv((STORE_DIR / "trips_per_day").as_posix())
    logger.info(
        f'Daily amount of trips stored on {(STORE_DIR / "trips_per_day").as_posix()}'
    )

    logger.info("Calculating the most popular starting station per month...")
    popular_station = get_most_popular_station_per_month(data)
    popular_station.write.mode("overwrite").csv(
        (STORE_DIR / "popular_station").as_posix()
    )
    logger.info(
        f'Popular starting stations per month stored on {(STORE_DIR / "popular_station").as_posix()}'
    )

    logger.info(
        "Calculating the top 3 trip stations each day for the last two weeks..."
    )
    top_3_daily_stations_last_2_weeks = get_top_3_daily_stations_last_2_weeks(data)
    top_3_daily_stations_last_2_weeks.write.mode("overwrite").csv(
        (STORE_DIR / "top_3_daily_stations_last_2_weeks").as_posix()
    )
    logger.info(
        f'Top 3 trip stations each day for the last two weeks stored on {(STORE_DIR / "top_3_daily_stations_last_2_weeks").as_posix()}'
    )

    logger.info("Calculating the average trip duration per gender...")
    trip_duration_per_gender = get_trip_duration_per_gender(data)
    trip_duration_per_gender.write.mode("overwrite").csv(
        (STORE_DIR / "trip_duration_per_gender").as_posix()
    )
    logger.info(
        f'Average trip duration per gender stored on {(STORE_DIR / "get_trip_duration_per_gender").as_posix()}'
    )

    logger.info(
        "Calculating the top 10 ages of those that take the longest and shortest trips..."
    )
    top_ages = get_top_10_ages_per_longer_and_shorter_trips(data)
    top_ages.write.mode("overwrite").csv((STORE_DIR / "top_ages").as_posix())
    logger.info(
        f'Average trip duration per gender stored on {(STORE_DIR / "top_ages").as_posix()}'
    )

    logger.info(top_ages.printSchema())


if __name__ == "__main__":
    main()
