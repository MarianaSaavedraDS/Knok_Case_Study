from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 1),  # start at the beginning of your dataset
    'retries': 1,
}

with DAG(
    'dbt_dag',
    default_args=default_args,
    schedule_interval='@daily',  # runs daily, adjust if needed
    catchup=False,  # will run for all dates between start_date and today
) as dag:

    DBT_DIR = '/opt/airflow/dbt'
    DBT_PROFILES_DIR = f'{DBT_DIR}'

    # 1️⃣ Run dbt deps (install packages)
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'cd {DBT_DIR} && DBT_PROFILES_DIR={DBT_PROFILES_DIR} dbt deps'
    )

    # 2️⃣ Run staging models
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'cd {DBT_DIR} && DBT_PROFILES_DIR={DBT_PROFILES_DIR} dbt run --models staging'
    )

    # 3️⃣ Run marts
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f'cd {DBT_DIR} && DBT_PROFILES_DIR={DBT_PROFILES_DIR} dbt run --models marts'
    )

    # 4️⃣ Run tests (if you add them)
    dbt_tests = BashOperator(
        task_id='dbt_tests',
        bash_command=f'cd {DBT_DIR} && DBT_PROFILES_DIR={DBT_PROFILES_DIR} dbt test'
    )

    # Set dependencies
    dbt_deps >> dbt_run_staging >> dbt_run_marts >> dbt_tests
