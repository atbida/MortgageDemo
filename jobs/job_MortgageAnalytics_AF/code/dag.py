import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from _e5bjil_5ragw3npqzrlug_.tasks import mortgage_analytics, troubled_mortgage_HAF
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "_E5BJiL_5rAGW3NpQzrLug_", 
    schedule_interval = "0 0 * * *", 
    default_args = {"owner" : "Prophecy", "ignore_first_depends_on_past" : True, "do_xcom_push" : True, "pool" : "P1Mfmhdo"}, 
    start_date = pendulum.datetime(2024, 3, 13, tz = "UTC"), 
    end_date = pendulum.datetime(2024, 3, 13, tz = "UTC"), 
    catchup = True, 
    tags = []
) as dag:
    mortgage_analytics_op = mortgage_analytics()
    troubled_mortgage_HAF_op = troubled_mortgage_HAF()
    mortgage_analytics_op >> troubled_mortgage_HAF_op
