from dagster import asset
import json
import requests
import pandas as pd
from google.cloud import bigquery
import time

@asset(required_resource_keys={"cfb_token"}, compute_kind="Extract")
def get_play_by_play_regular_data(context):
    url = "https://api.collegefootballdata.com/plays"
    headers = {"Authorization": context.resources.cfb_token}
    df = pd.DataFrame()
    for year in range(2018, 2023):
        for week in range(1, 17):
            while True:
                params = {"year": year, "seasonType": "regular", "week": week}
                response = requests.request("GET", url, headers=headers, params=params)
                if response.status_code != 200:
                    context.log.debug(response.status_code)
                    time.sleep(60)
                else:
                    response_text = json.loads(response.text)
                    df2 = pd.DataFrame(response_text)
                    if df2.shape[0] == 0:
                        break
                    df2["year"] = year
                    df2["week"] = week
                    df2["season_type"] = "regular"
                    df = pd.concat([df, df2])
                    time.sleep(10)
                    break
        context.log.debug(f"Regular Season: {year}")
    context.add_output_metadata({"Rows": df.shape[0], "Columns": df.shape[1]})
    return df

@asset(required_resource_keys={"cfb_token"}, compute_kind="Extract")
def get_play_by_play_postseason_data(context):
    url = "https://api.collegefootballdata.com/plays"
    headers = {"Authorization": context.resources.cfb_token}
    df = pd.DataFrame()
    for year in range(2018, 2023):
        while True:
            params = {"year": year, "seasonType": "postseason", "week": 1}
            response = requests.request("GET", url, headers=headers, params=params)
            if response.status_code != 200:
                context.log.debug(response.status_code)
                time.sleep(60)
            else:
                response_text = json.loads(response.text)
                df2 = pd.DataFrame(response_text)
                df2["year"] = year
                df2["week"] = 1
                df2["season_type"] = "postseason"
                df = pd.concat([df, df2])
                time.sleep(10)
                break
        context.log.debug(f"Postseason: {year}")
    context.add_output_metadata({"Rows": df.shape[0], "Columns": df.shape[1]})
    return df

@asset(compute_kind="Transform")
def clean_play_by_play_data(context, get_play_by_play_regular_data, get_play_by_play_postseason_data):
    df = pd.concat([get_play_by_play_regular_data, get_play_by_play_postseason_data]).reset_index(drop=True)
    df["game_id"] = df["game_id"].astype(int).astype(str)
    df["clock_minutes"] = df["clock"].apply(lambda x: f"{x['minutes']}").astype(int)
    df["clock_seconds"] = df["clock"].apply(lambda x: f"{x['seconds']}").astype(int)
    df = df.drop('clock', axis=1)
    df["ppa"] = df["ppa"].astype(float)
    df["wallclock"] = pd.to_datetime(df["wallclock"])
    col_order = [
        "year",
        "season_type",
        "week",
        "game_id",
        "drive_id",
        "id",
        "home",
        "away",
        "offense",
        "defense",
        "offense_conference",
        "defense_conference",
        "offense_score",
        "defense_score",
        "offense_timeouts",
        "defense_timeouts",
        "period",
        "drive_number",
        "play_number",
        "yard_line",
        "yards_to_goal",
        "down",
        "distance",
        "yards_gained",
        "wallclock",
        "clock_minutes",
        "clock_seconds",
        "scoring",
        "play_type",
        "play_text",
        "ppa"
    ]
    df = df.reindex(columns=col_order)
    context.add_output_metadata({"Rows": df.shape[0], "Columns": df.shape[1]})
    return df

@asset(required_resource_keys={"bigquery_api_token"}, compute_kind="Load")
def load_play_by_play_data(context, clean_play_by_play_data):
    client = bigquery.Client()
    table_id = "bigquerytest-373818.football.playbyplay"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("season_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("week", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("game_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("drive_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("home", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("away", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("offense", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("defense", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("offense_conference", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("defense_conference", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("offense_score", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("defense_score", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("offense_timeouts", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("defense_timeouts", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("period", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("drive_number", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("play_number", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("yard_line", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("yards_to_goal", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("down", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("distance", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("yards_gained", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("wallclock", bigquery.enums.SqlTypeNames.DATETIME),
            bigquery.SchemaField("clock_minutes", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("clock_seconds", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("scoring", bigquery.enums.SqlTypeNames.BOOL),
            bigquery.SchemaField("play_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("play_text", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("ppa", bigquery.enums.SqlTypeNames.FLOAT64)
        ],
        write_disposition="WRITE_TRUNCATE"
    )
    job = client.load_table_from_dataframe(clean_play_by_play_data, table_id, job_config=job_config)
    context.log.debug(job.result())