from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    "owner": "wagerflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _spark_command():
    rs_password = Variable.get("redshift_admin_password")
    return f"""
    source ~/kafka-mentor/venv/bin/activate
    cd ~/wagerflow
    export SPARK_LOCAL_IP=127.0.0.1
    export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile wagerflow)
    export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile wagerflow)
    export RS_ADMIN_PASSWORD='{rs_password}'
    python batch/ggr_ltv_batch.py
    """

with DAG(
    dag_id="wagerflow_nightly_batch",
    description="Nightly GGR/LTV batch job -> dbt run -> d bt test",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["wagerflow", "batch", "nightly"],
) as dag:

    run_spark_batch = SSHOperator(
        task_id="run_spark_ggr_ltv_batch",
        ssh_conn_id="ssh_wagerflow_host",
        command=_spark_command(),
        cmd_timeout=1800,
    )

    run_dbt = SSHOperator(
        task_id="dbt_run",
        ssh_conn_id="ssh_wagerflow_host",
        command="""
        source ~/kafka-mentor/venv/bin/activate
        cd ~/wagerflow/wagerflow_dbt
        dbt run
        """,
        cmd_timeout=600,
    )

    test_dbt = SSHOperator(
        task_id="dbt_test",
        ssh_conn_id="ssh_wagerflow_host",
        command="""
        source ~/kafka-mentor/venv/bin/activate
        cd ~/wagerflow/wagerflow_dbt
        dbt test
        """,
        cmd_timeout=300,
    )

    run_spark_batch >> run_dbt >> test_dbt