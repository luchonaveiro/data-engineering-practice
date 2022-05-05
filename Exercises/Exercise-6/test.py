import pyspark
from pyspark.sql import types as T
import pyspark.sql.functions as F

from main import (
    get_average_trip_duration_per_day,
    get_daily_amount_of_trips,
    get_most_popular_station_per_month,
    get_top_3_daily_stations_last_2_weeks,
    get_trip_duration_per_gender,
    get_top_10_ages_per_longer_and_shorter_trips,
)

spark = (
    pyspark.sql.SparkSession.builder.appName("Exercise6-test")
    .enableHiveSupport()
    .getOrCreate()
)


def test_get_average_trip_duration_per_day():
    minimal_df_schema = T.StructType(
        [
            T.StructField("start_time", T.StringType(), True),
            T.StructField("tripduration", T.StringType(), True),
        ]
    )
    minimal_df_data = [
        ["2022-01-01 00:00:00", "10"],
        ["2022-01-01 00:00:00", "12"],
        ["2022-01-02 00:00:00", "1"],
        ["2022-01-02 00:00:00", "3"],
    ]
    minimal_df = spark.createDataFrame(minimal_df_data, minimal_df_schema)

    expected_df = [
        ["2022-01-01", 11.0],
        ["2022-01-02", 2.0],
    ]
    expected_df_schema = T.StructType(
        [
            T.StructField("date", T.StringType(), True),
            T.StructField("average_tripduration", T.DoubleType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_df, expected_df_schema)
    expected_df = expected_df.withColumn("date", F.to_date(F.col("date")))

    evaluated_df = get_average_trip_duration_per_day(minimal_df)

    assert expected_df.schema == evaluated_df.schema
    assert expected_df.collect() == evaluated_df.collect()


def test_get_daily_amount_of_trips():
    minimal_df_schema = T.StructType(
        [
            T.StructField("start_time", T.StringType(), True),
            T.StructField("tripduration", T.StringType(), True),
        ]
    )
    minimal_df_data = [
        ["2022-01-01 00:00:00", "10"],
        ["2022-01-01 00:00:00", "12"],
        ["2022-01-02 00:00:00", "1"],
    ]
    minimal_df = spark.createDataFrame(minimal_df_data, minimal_df_schema)

    expected_df = [
        ["2022-01-01", 2],
        ["2022-01-02", 1],
    ]
    expected_df_schema = T.StructType(
        [
            T.StructField("date", T.StringType(), True),
            T.StructField("trips_amount", T.LongType(), False),
        ]
    )
    expected_df = spark.createDataFrame(expected_df, expected_df_schema)
    expected_df = expected_df.withColumn("date", F.to_date(F.col("date")))

    evaluated_df = get_daily_amount_of_trips(minimal_df)

    assert expected_df.schema == evaluated_df.schema
    assert expected_df.collect() == evaluated_df.collect()


def test_get_most_popular_station_per_month():
    minimal_df_schema = T.StructType(
        [
            T.StructField("start_time", T.StringType(), True),
            T.StructField("from_station_name", T.StringType(), True),
        ]
    )
    minimal_df_data = [
        ["2022-01-01 00:00:00", "A"],
        ["2022-01-01 00:00:00", "A"],
        ["2022-01-01 00:00:00", "B"],
        ["2022-02-01 00:00:00", "C"],
        ["2022-02-01 00:00:00", "C"],
        ["2022-02-01 00:00:00", "D"],
    ]
    minimal_df = spark.createDataFrame(minimal_df_data, minimal_df_schema)

    expected_df = [
        ["2022-1", "A", 2, 1],
        ["2022-2", "C", 2, 1],
    ]
    expected_df_schema = T.StructType(
        [
            T.StructField("month", T.StringType(), False),
            T.StructField("from_station_name", T.StringType(), True),
            T.StructField("trips_amount", T.LongType(), False),
            T.StructField("monthly_rank", T.IntegerType(), False),
        ]
    )
    expected_df = spark.createDataFrame(expected_df, expected_df_schema)

    evaluated_df = get_most_popular_station_per_month(minimal_df)

    assert expected_df.schema == evaluated_df.schema
    assert expected_df.collect() == evaluated_df.collect()


def test_get_top_3_daily_stations_last_2_weeks():
    minimal_df_schema = T.StructType(
        [
            T.StructField("start_time", T.StringType(), True),
            T.StructField("from_station_name", T.StringType(), True),
        ]
    )
    minimal_df_data = [
        ["2022-01-07 00:00:00", "A"],
        ["2022-01-07 00:00:00", "B"],
        ["2022-01-07 00:00:00", "C"],
        ["2022-02-01 00:00:00", "A"],
        ["2022-02-01 00:00:00", "B"],
        ["2022-02-01 00:00:00", "B"],
        ["2022-02-01 00:00:00", "C"],
        ["2022-02-01 00:00:00", "C"],
        ["2022-02-01 00:00:00", "C"],
        ["2022-02-01 00:00:00", "D"],
        ["2022-02-01 00:00:00", "D"],
        ["2022-02-01 00:00:00", "D"],
        ["2022-02-01 00:00:00", "D"],
        ["2022-02-01 00:00:00", "I"],
        ["2022-02-20 00:00:00", "E"],
        ["2022-02-20 00:00:00", "F"],
        ["2022-02-20 00:00:00", "F"],
        ["2022-02-20 00:00:00", "G"],
        ["2022-02-20 00:00:00", "G"],
        ["2022-02-20 00:00:00", "G"],
        ["2022-02-20 00:00:00", "H"],
        ["2022-02-20 00:00:00", "H"],
        ["2022-02-20 00:00:00", "H"],
        ["2022-02-20 00:00:00", "H"],
        ["2022-02-20 00:00:00", "I"],
    ]
    minimal_df = spark.createDataFrame(minimal_df_data, minimal_df_schema)

    expected_df = [
        ["2022-02-01", "D", 4, 1],
        ["2022-02-01", "C", 3, 2],
        ["2022-02-01", "B", 2, 3],
        ["2022-02-20", "H", 4, 1],
        ["2022-02-20", "G", 3, 2],
        ["2022-02-20", "F", 2, 3],
    ]
    expected_df_schema = T.StructType(
        [
            T.StructField("date", T.StringType(), False),
            T.StructField("from_station_name", T.StringType(), True),
            T.StructField("trips_amount", T.LongType(), False),
            T.StructField("daily_rank", T.IntegerType(), False),
        ]
    )
    expected_df = spark.createDataFrame(expected_df, expected_df_schema)
    expected_df = expected_df.withColumn("date", F.to_date(F.col("date")))

    evaluated_df = get_top_3_daily_stations_last_2_weeks(minimal_df)

    assert expected_df.schema == evaluated_df.schema
    assert expected_df.collect() == evaluated_df.collect()


def test_get_trip_duration_per_gender():
    minimal_df_schema = T.StructType(
        [
            T.StructField("gender", T.StringType(), True),
            T.StructField("tripduration", T.StringType(), True),
        ]
    )
    minimal_df_data = [
        ["Female", "10"],
        ["Female", "2"],
        ["Male", "1"],
        ["Male", "3"],
    ]
    minimal_df = spark.createDataFrame(minimal_df_data, minimal_df_schema)

    expected_df = [
        ["Female", 6.0],
        ["Male", 2.0],
    ]
    expected_df_schema = T.StructType(
        [
            T.StructField("gender", T.StringType(), True),
            T.StructField("average_tripduration", T.DoubleType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_df, expected_df_schema)

    evaluated_df = get_trip_duration_per_gender(minimal_df)

    assert expected_df.schema == evaluated_df.schema
    assert expected_df.collect() == evaluated_df.collect()


def test_get_top_10_ages_per_longer_and_shorter_trips():
    minimal_df_schema = T.StructType(
        [
            T.StructField("start_time", T.StringType(), True),
            T.StructField("tripduration", T.StringType(), True),
            T.StructField("birthyear", T.StringType(), True),
        ]
    )
    minimal_df_data = [
        ["2022-01-01", "10", "1982"],
        ["2022-01-01", "10", "1982"],
        ["2022-01-01", "10", "1982"],
        ["2022-01-01", "10", "1985"],
        ["2022-01-01", "100", "1985"],
        ["2022-01-01", "100", "1985"],
        ["2022-01-01", "100", "1985"],
        ["2022-01-01", "100", "1982"],
    ]
    minimal_df = spark.createDataFrame(minimal_df_data, minimal_df_schema)

    expected_df = [
        [0, 1, 40, 3, 1],
        [0, 1, 37, 1, 2],
        [1, 0, 37, 3, 1],
        [1, 0, 40, 1, 2],
    ]
    expected_df_schema = T.StructType(
        [
            T.StructField("longest_duration", T.IntegerType(), True),
            T.StructField("shortest_duration", T.IntegerType(), True),
            T.StructField("age", T.IntegerType(), True),
            T.StructField("trips_amount", T.LongType(), False),
            T.StructField("rank_ages:", T.IntegerType(), False),
        ]
    )
    expected_df = spark.createDataFrame(expected_df, expected_df_schema)

    evaluated_df = get_top_10_ages_per_longer_and_shorter_trips(minimal_df)

    assert expected_df.collect() == evaluated_df.collect()
