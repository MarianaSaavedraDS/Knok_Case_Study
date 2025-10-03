from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'dbt_dag',
    default_args=default_args,
    start_date=datetime(2022, 4, 1),   # start of your dataset
    schedule_interval='@daily',        # daily processing
    catchup=False,                       # process all historical dates
    max_active_runs=1,                  # one DAG run at a time
    tags=['dbt', 'etl'],
) as dag:

    # 1️⃣ Install dbt packages (always runs)
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt && dbt deps'
    )

    # 2️⃣ Run staging models (incremental: only run once per date)
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=(
            'cd /opt/airflow/dbt && '
            'dbt run --models staging --vars "execution_date: {{ ds }}"'
        )
    )

    # 3️⃣ Run marts (daily, depends on staging)
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=(
            'cd /opt/airflow/dbt && '
            'dbt run --models marts --vars "execution_date: {{ ds }}"'
        )
    )

    # 4️⃣ Run dbt tests
    dbt_tests = BashOperator(
        task_id='dbt_tests',
        bash_command='cd /opt/airflow/dbt && dbt test'
    )

    # Set dependencies
    dbt_deps >> dbt_run_staging >> dbt_run_marts >> dbt_tests
