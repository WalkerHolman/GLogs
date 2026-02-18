import openmeteo_requests
from datetime import datetime, timedelta
import pandas as pd
import snowflake.connector
import os
import requests
import math


def get_weather(locations: list[str | dict] = ["Atlanta", "New York", {"Name":"Washington", "Admin1": "District of Columbia"}, "San Francisco", "Daniel Boone National Forest"], 
                only_previous_hour: bool = False,
                start_date: datetime | None = None,
                end_date: datetime | None = None,
                save_csv: bool = True, 
                write_outputs: bool = True):

    # By default, get the weather for the previous 24 hours.
    # If only_previous_hour is True, get the weather for the previous hour.
    if only_previous_hour:
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=1)
    if end_date is None:
        end_date = datetime.now()
    if start_date is None:
        start_date = end_date - timedelta(hours=24)
    
    # intialize the client
    client = openmeteo_requests.Client()

    def geocode_location(location: str | dict) -> tuple[float, float, str]:
        if isinstance(location, dict):
            name = location["Name"]
            admin1 = location["Admin1"]
        else:
            name = location
            admin1 = None

        resp = requests.get(
            "https://geocoding-api.open-meteo.com/v1/search",
            params={"name": name, "admin1": admin1, "count": 1, "language": "en", "format": "json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results") or []
        if not results:
            raise ValueError(f"Could not geocode location: {name}")
        r0 = results[0]
        return float(r0["latitude"]), float(r0["longitude"]), r0.get("name") or name

    # archive api endpoint
    url = "https://archive-api.open-meteo.com/v1/archive"
    base_params = {
        "hourly": ["temperature_2m", "precipitation_probability", "precipitation", "is_day"],
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "timezone": "auto",
    }

    all_rows: list[dict] = []
    # Helpers to sanitize at construction time
    def safe_float(value):
        try:
            f = float(value)
            return f if math.isfinite(f) else None
        except Exception:
            return None

    def safe_int(value):
        try:
            # treat NaN/inf/None as NULL
            if value is None:
                return None
            if isinstance(value, float) and not math.isfinite(value):
                return None
            if pd.isna(value):
                return None
            return int(value)
        except Exception:
            return None

    for name in locations: # TODO parallelize
        lat, lon, resolved_name = geocode_location(name)
        params = dict(base_params)
        params["latitude"] = lat
        params["longitude"] = lon

        responses = client.weather_api(url, params=params)
        if not responses:
            continue
        response = responses[0]

        hourly = response.Hourly()
        # Build the time index from start/end/interval
        hourly_time = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )

        # Extract variables in the same order as requested
        temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        precipitation_probability = hourly.Variables(1).ValuesAsNumpy()
        precipitation = hourly.Variables(2).ValuesAsNumpy()
        is_day = hourly.Variables(3).ValuesAsNumpy()

        for i, when in enumerate(hourly_time):
            all_rows.append(
                {
                    "location": resolved_name,
                    "time": when.tz_convert(None).strftime("%Y-%m-%d %H:%M:%S"), # TODO convert to utc?,
                    "temperature": safe_float(temperature_2m[i]),
                    "precipitation_probability": safe_float(precipitation_probability[i]),
                    "precipitation": safe_float(precipitation[i]),
                    "is_day": safe_int(is_day[i]),
                }
            )

    # Build DataFrame
    df = pd.DataFrame(all_rows)
    if save_csv and not df.empty:
        df.to_csv("weather.csv", index=False)

    if not df.empty:
        print(df.head())
    else:
        print("No weather data returned.")

    if write_outputs and not df.empty:
        write_outputs_to_snowflake(df)


def write_outputs_to_snowflake(df: pd.DataFrame):
    # Initialize Snowflake connection using environment variables
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        host=os.getenv("SNOWFLAKE_HOST"),
    )
    try:
        with conn.cursor() as cur:
            # Ensure context (avoid quoting to prevent case-sensitive mismatches)
            cur.execute(f"USE ROLE {os.getenv('SNOWFLAKE_ROLE').upper()}")
            cur.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE').upper()}")
            cur.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_SCHEMA').upper()}")

            # Build fully-qualified table name if db/schema provided
            table_name = f"{os.getenv('SNOWFLAKE_DATABASE').upper()}.{os.getenv('SNOWFLAKE_SCHEMA').upper()}.WEATHER"

            # Ensure target table exists
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    location STRING,
                    time TIMESTAMP_NTZ,
                    temperature FLOAT,
                    precipitation_probability FLOAT,
                    precipitation FLOAT,
                    is_day INT
                )
                """
            )
            rows = df[["location", "time", "temperature", "precipitation_probability", "precipitation", "is_day"]].values.tolist()
            cur.executemany(
                f"INSERT INTO {table_name} (location, time, temperature, precipitation_probability, precipitation, is_day) VALUES (%s, %s, %s, %s, %s, %s)",
                rows,
            )
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    get_weather(save_csv=True, write_outputs=True)