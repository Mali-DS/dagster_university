import requests
from . import constants
from dagster import asset

import ssl
import certifi
from urllib.request import urlopen


@asset
def taxi_trips_file() -> None:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    month_to_fetch = '2023-03'
    request = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    urlopen(request, context=ssl.create_default_context(cafile=certifi.where()))
    raw_trips = requests.get(
        request   
    )

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
    ) as output_file:
        output_file.write(raw_trips.content)