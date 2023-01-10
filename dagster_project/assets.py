from dagster import asset
import json
import requests
import pandas as pd
from google.cloud import bigquery

@asset(required_resource_keys={"cfb_token"})
def get_play_by_play_data(context):
    url = "https://api.collegefootballdata.com/plays"
    headers = {
        "Authorization": context.resources.cfb_token
    }
    df = pd.DataFrame()
    for week in range(1, 17):
        params = {"year": 2022, "seasonType": 'regular', 'week': week}
        response = requests.request("GET", url, headers=headers, params=params)
        context.log.debug(response.status_code)
        response.raise_for_status()
        response_text = json.loads(response.text)
        df2 = pd.DataFrame(response_text)
        if df2.shape[0] == 0:
            next
        df2['week'] = week
        df = pd.concat([df, df2])
    return df

@asset
def clean_play_by_play_data(get_play_by_play_data):
    df = get_play_by_play_data.reset_index(drop=True)
    df["game_id"] = df["game_id"].astype(str)
    df["clock"] = df["clock"].apply(lambda x: f"{x['minutes']}:{x['seconds']}")
    df["ppa"] = df["ppa"].astype(float)
    df["wallclock"] = pd.to_datetime(df["wallclock"])
    return df


@asset(required_resource_keys={"bigquery_api_token"})
def load_play_by_play_data(context, clean_play_by_play_data):
    client = bigquery.Client()
    table_id = "bigquerytest-373818.football.playbyplay"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("offense", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("offense_conference", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("defense", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("defense_conference", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("home", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("away", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("offense_score", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("defense_score", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("game_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("drive_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("drive_number", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("play_number", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("period", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("clock", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("offense_timeouts", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("defense_timeouts", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("yard_line", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("yards_to_goal", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("down", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("distance", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("scoring", bigquery.enums.SqlTypeNames.BOOL),
            bigquery.SchemaField("yards_gained", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("play_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("play_text", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("ppa", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("wallclock", bigquery.enums.SqlTypeNames.DATETIME),
            bigquery.SchemaField("week", bigquery.enums.SqlTypeNames.INT64)
        ],
        write_disposition="WRITE_TRUNCATE"
    )
    job = client.load_table_from_dataframe(clean_play_by_play_data, table_id, job_config=job_config)
    context.log.debug(job.result())