"""DAG that retrieves NBA games scheduled on ESPN and saves to local JSON"""

# --------------- #
# Package imports #
# --------------- #

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c
from include.global_variables import user_input_variables as uv
from include.meterology_utils import (
    call_espn_api,
)

# --- #
# DAG #
# --- #

# a dataframe decorated function turns a returned Pandas dataframe into
# a Astro SDK Table object
@aql.dataframe(pool="duckdb")
def turn_json_into_table(in_json):
    """Converts the list of JSON input into one pandas dataframe."""
    if type(in_json) == dict:
        df = pd.DataFrame(in_json)
    else:
        df = pd.concat([pd.DataFrame(d) for d in in_json], ignore_index=True)
    return df

@dag(
    start_date=datetime(2023, 5, 9),
    schedule="@hourly",
    catchup=False,
    default_args=gv.default_args,
    description="DAG that retrieves espn information and saves it to a local JSON.",
    render_template_as_native_obj=True,
    tags=["part_1"],
)
def extract_espn_data():
    @task
    def get_espn_data():
        games = (
            call_espn_api()
        )

        return games.to_dict()
    
     # set dependencies to get current weather
    espn_games = get_espn_data()

    # use the @aql.dataframe decorated function to write the JSON returned from
    # the get_current_weather task as a permanent table to DuckDB
    turn_json_into_table(
        in_json=espn_games,
        output_table=Table(
            name=c.ESPN_GAMES_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
    )

extract_espn_data()