import requests
import json
from datetime import datetime, timezone
from pyspark.sql import Row

import sys
import os

add_directory = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)
if add_directory not in sys.path:
    sys.path.append(add_directory)
from utils.config import Config

# set environment
dbutils.widgets.dropdown("entorno", "dev", ["dev", "prod"], "Selecciona el Entorno")
env = dbutils.widgets.get("entorno")
print(f"Running on environment: {env}")

# configurations
def configurations():
    Config.configure_spark(spark)
    day = datetime.now().strftime("%m/%d/%Y")
    url_V1 = Config.BASE_URL_V1
    url = f'{url_V1}schedule?sportId=1&date={day}'
    bronze_schema = "mlb_{env}_bronze".format(env=env)
    table_schedule = f"{bronze_schema}.game_schedule"
    table_failed_game_schedule = f"{bronze_schema}.failed_game_schedule"
    return url, table_schedule, table_failed_game_schedule

def get_schedule(url):
    schedule = requests.get(url)
    try:
        schedule.raise_for_status()
        schedule = schedule.json().get('dates', [{}])[0].get('games', [])
        return schedule
    except requests.exceptions.HTTPError as err:
        raise err

def process_schedule(schedule):
    games_today = []
    failed_responses = []
    for game in schedule:
        try:
            game_id = str(game['gamePk'])
            home_team = game['teams']['home']['team']['name']
            away_team = game['teams']['away']['team']['name']
            game_scheduled_time = game['gameDate']
            game_scheduled_time = datetime.strptime(game_scheduled_time, "%Y-%m-%dT%H:%M:%SZ")
            status = 'registered'
            ingestion_time = datetime.now(timezone.utc)
            games_today.append(
                Row(
                    game_pk=game_id,
                    home_team=home_team,
                    away_team=away_team,
                    game_scheduled_time=game_scheduled_time,
                    status=status,
                    ingestion_timestamp=ingestion_time
                )
            )
        except Exception as e:
            failed_responses.append(
                Row(
                    response=json.dumps(game),
                    ingestion_timestamp=datetime.now(timezone.utc)
                )
            )
    return games_today, failed_responses

def save_to_bronze(games_today, failed_responses, table_schedule, table_failed_game_schedule):
    if games_today:
        df_games = spark.createDataFrame(games_today)
        df_games.write.format("delta").mode("append").saveAsTable(table_schedule)
    if failed_responses:
        df_failed = spark.createDataFrame(failed_responses)
        df_failed.write.format("delta").mode("append").saveAsTable(table_failed_game_schedule)

def main():
    url, table_schedule, table_failed_game_schedule = configurations()
    try:
        schedule = get_schedule(url)
        games_today, failed_responses = process_schedule(schedule)
    except Exception as e:
        games_today = []
        failed_responses = []
        failed_responses.append(
            Row(
                response=str(e),
                ingestion_timestamp=datetime.now(timezone.utc)
            )
        )
    save_to_bronze(games_today, failed_responses, table_schedule, table_failed_game_schedule)
    
if __name__ == "__main__":
    main()