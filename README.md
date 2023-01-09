# Dagster College Football Project
This project is intended to learn dagster. It pulls data from the [College Football API](https://collegefootballdata.com/), and loads it into Google [BigQuery](https://cloud.google.com/bigquery)

## Setup:
There are two environmental variables needed for this project:
1. An API token from College Football API that you can sign up for [here](https://collegefootballdata.com/key)
   * I assigned this token to the environment variable `CFB_TOKEN`. **Important** bearer needs to be prepended to the token. (Ex. CFB_TOKEN=bearer thisismytoken)
   
2. Google credentials file (JSON) that I point my "GOOGLE_APPLICATION_CREDENTIALS" environment variable to the location of
