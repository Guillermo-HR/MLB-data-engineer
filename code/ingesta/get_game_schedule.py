import requests
import json
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from datetime import datetime, timezone
from pyspark.sql import Row
import sys
from dotenv import load_dotenv
import os

# set environment
load_dotenv()
env = os.getenv('env')
print(f"Running on environment: {env}")

# configurations
def configurations(override_date=None):
    day = override_date if override_date else datetime.now(timezone.utc).strftime("%m/%d/%Y")
    url_V1 = 'https://statsapi.mlb.com/api/v1/'
    url = f'{url_V1}schedule?sportId=1&date={day}'
    bronze_schema = f'mlb_{env}_bronze'
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
    expected_schema = {
        "type": "object",
        "properties": { 
            "gamePk": {"type": "integer"},
            "teams": {
                "type": "object",
                "properties": {
                    "home": {
                        "type": "object",
                        "properties": {
                            "team": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"}
                                },
                                "required": ["name"]
                            }
                        },
                        "required": ["team"]
                    },
                    "away": {
                        "type": "object",
                        "properties": {
                            "team": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"}
                                },
                                "required": ["name"]
                            }
                        },
                        "required": ["team"]
                    }
                },
                "required": ["home", "away"]
            },
            "gameDate": {"type": "string"},
            "status": {
                "type": "object",
                "properties": {
                    "abstractGameCode": {"type": "string"}
                },
                "required": ["abstractGameCode"]
            }
        },
        "required": ["gamePk", "teams", "gameDate", "status"]
    }
    games_today = []
    failed_responses = []
    for game in schedule:
        try:
            validate(instance=game, schema=expected_schema)
            game_id = str(game['gamePk'])
            home_team = game.get('teams', {}).get('home', {}).get('team', {}).get('name', 'Unknown Home Team')
            away_team = game.get('teams', {}).get('away', {}).get('team', {}).get('name', 'Unknown Away Team')
            game_scheduled_time = game.get('gameDate')
            game_scheduled_time = datetime.strptime(game_scheduled_time, "%Y-%m-%dT%H:%M:%SZ")
            status = game.get('status', {}).get('abstractGameCode')
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
        except ValidationError as err:
            failed_responses.append(
                Row(
                    response=json.dumps(game),
                    error=f"Schema validation error: {str(err)}",
                    ingestion_timestamp=datetime.now(timezone.utc)
                )
            )
        except Exception as e:
            failed_responses.append(
                Row(
                    response=json.dumps(game),
                    error=f"Unexpected error: {str(e)}",
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

def main(date_param=None):
    url, table_schedule, table_failed_game_schedule = configurations(date_param)
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
    print(f'Games added: {len(games_today)}')
    print(f'Failed games: {len(failed_responses)}')
    
if __name__ == "__main__":
    try:
        date_input = sys.argv[1] if len(sys.argv) > 1 else None
    except:
        date_input = None
    main(date_input)
    print("Finished")