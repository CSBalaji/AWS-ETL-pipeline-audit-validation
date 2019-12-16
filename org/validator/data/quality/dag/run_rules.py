from airflow.models import DAG, Variable
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import TimeDeltaSensor, S3KeySensor
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook


ENV=Variable.get("ENV")
POSTFIX_DASH_ENV= '' if ENV == 'PROD' else '-dev'
POSTFIX_UNDERSCORE_ENV= '' if ENV == 'PROD' else '_dev'

args = {
    'owner': 'Balaji Shankar',
    'start_date': datetime(2016, 1, 1),
    'email': ['ConvergenceETL@hbo.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 210,
    'retry_delay': timedelta(hours=1),
}

dag = DAG(
    dag_id='rule_engine',
    default_args=args,
    schedule_interval="45 13 * * *",
)


run_rule = SSHExecuteOperator(
    task_id='to_parquet',
    bash_command="""
  python rule.py <rule_id>
    """,
    params={'pf': POSTFIX_DASH_ENV},
    pool='rule_engine',
    dag=dag)

run_rules = SSHExecuteOperator(
    task_id='to_parquet',
    bash_command="""
  python rule.py <rule_group_id>
    """,
    params={'pf': POSTFIX_DASH_ENV},
    pool='rule_engine',
    dag=dag)
