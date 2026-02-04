from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "Yashasvi",
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="weather_system_pipeline_3",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),#date from which DAG is allowed to run
    schedule_interval="@daily",
    catchup=False
    
) as dag:

    # SCRAPING
    scrape_weather = BashOperator(
        task_id="scrape_weather_data",
        bash_command="/home/sunbeam/airflow-venv/bin/python \
/home/sunbeam/BD_project_mine/live_data.py"

    )

    upload_to_hdfs = BashOperator(
        task_id="upload_daily_csv_to_hdfs",
        bash_command="""
        hadoop fs -appendToFile \
        /home/sunbeam/BD_project_mine/weather_today.csv \
        /weather/data/raw_data/weather_dataset.csv
         """
    )



    # PROCESSING
   
    process_weather = BashOperator(
     task_id="process_weather_data",
     bash_command="""
     export SPARK_HOME=/home/sunbeam/.local/lib/python3.10/site-packages/pyspark
     export PATH=$SPARK_HOME/bin:$PATH
     export PYSPARK_PYTHON=/home/sunbeam/airflow-venv/bin/python

     spark-submit --master local[2] \
     /home/sunbeam/BD_project_mine/pre_processing.py
    """
    )



    #ANALYSIS
    analyze_weather = BashOperator(
        task_id="analyze_weather_data",
        bash_command="""export SPARK_HOME=/home/sunbeam/.local/lib/python3.10/site-packages/pyspark
     export PATH=$SPARK_HOME/bin:$PATH
     export PYSPARK_PYTHON=/home/sunbeam/airflow-venv/bin/python

     spark-submit --master local[2] \
     /home/sunbeam/BD_project_mine/processing.py
        """
    )

    mark_done = BashOperator(
    task_id="mark_pipeline_done",
    bash_command="""
    hdfs dfs -touchz /weather/_SUCCESS
    """
)


    # PIPELINE ORDER
    scrape_weather >> upload_to_hdfs >> process_weather >> analyze_weather >> mark_done
