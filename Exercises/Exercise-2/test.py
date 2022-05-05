import pandas as pd
from pandas.testing import assert_frame_equal
from numpy.testing import assert_array_equal
from main import (
    search_id_from_html,
    create_download_url,
    get_highest_hourly_dry_bulb_temperature,
    get_highest_hourly_dry_bulb_temperature_records,
)


def test_get_highest_hourly_dry_bulb_temperature():
    minimal_df = pd.DataFrame(
        {"id": [1, 2, 3], "HourlyDryBulbTemperature": [-2, 0, 45]}
    )
    max_expected_value = 45
    max_evaluated_value = get_highest_hourly_dry_bulb_temperature(minimal_df)

    assert max_expected_value == max_evaluated_value


def test_get_highest_hourly_dry_bulb_temperature_records():
    minimal_df = pd.DataFrame(
        {"id": [1, 2, 3], "HourlyDryBulbTemperature": [-2, 0, 45]}
    )
    minimal_expected_df = pd.DataFrame({"id": [3], "HourlyDryBulbTemperature": [45]})
    minimal_evaluated_df = get_highest_hourly_dry_bulb_temperature_records(minimal_df)

    assert_array_equal(minimal_expected_df.values, minimal_evaluated_df.values)


def test_search_id_from_html():
    minimal_html = """
    <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
    <html>
    <head>
    <title>Index of /data/local-climatological-data/access/2021</title>
    </head>
    <body>
    <h1>Index of /data/local-climatological-data/access/2021</h1>
    <table>
    <tr><th><a href="?C=N;O=D">Name</a></th><th><a href="?C=M;O=A">Last modified</a></th><th><a href="?C=S;O=A">Size</a></th><th><a href="?C=D;O=A">Description</a></th></tr>
    <tr><th colspan="4"><hr></th></tr>
    <tr><td><a href="/data/local-climatological-data/access/">Parent Directory</a></td><td>&nbsp;</td><td align="right">  - </td><td>&nbsp;</td></tr>
    <tr><td><a href="01001099999.csv">01001099999.csv</a></td><td align="right">2022-02-07 13:33  </td><td align="right">4.0M</td><td>&nbsp;</td></tr>
    <tr><td><a href="01001499999.csv">01001499999.csv</a></td><td align="right">2022-02-07 14:45  </td><td align="right">2.8M</td><td>&nbsp;</td></tr>
    <tr><td><a href="01002099999.csv">01002099999.csv</a></td><td align="right">2022-02-07 15:03  </td><td align="right">175K</td><td>&nbsp;</td></tr>
    <tr><td><a href="01003099999.csv">01003099999.csv</a></td><td align="right">2022-02-07 13:29  </td><td align="right">323K</td><td>&nbsp;</td></tr>
    <tr><td><a href="01006099999.csv">01006099999.csv</a></td><td align="right">2022-02-07 13:57  </td><td align="right">179K</td><td>&nbsp;</td></tr>
    <tr><th colspan="4"><hr></th></tr>
    </table>
    <script id="_fed_an_ua_tag" type="text/javascript" src="https://dap.digitalgov.gov/Universal-Federated-Analytics-Min.js?agency=DOC%26subagency=NOAA"></script></body></html>"""

    interested_file = "2022-02-07 15:03"
    expected_id = "01002099999.csv"
    evaluated_id = search_id_from_html(minimal_html, interested_file)

    assert expected_id == evaluated_id


def test_create_download_url():
    evaluate_id = "1234.csv"
    expected_url = (
        "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/1234.csv"
    )
    evaluated_url = create_download_url(evaluate_id)

    assert expected_url == evaluated_url
