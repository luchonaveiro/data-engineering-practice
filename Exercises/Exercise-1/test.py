import pytest
from main import get_filename_from_url


@pytest.mark.parametrize(
    "url, file_name",
    [
        (
            "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
            "Divvy_Trips_2018_Q4.zip",
        ),
        (
            "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
            "Divvy_Trips_2019_Q1.zip",
        ),
        (
            "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
            "Divvy_Trips_2019_Q2.zip",
        ),
    ],
)
def test_get_filename_from_url(url, file_name):
    assert get_filename_from_url(url) == file_name
