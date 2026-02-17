"""
Airflow DAG: Task Manager ETL Pipeline

Schedules Spark batch jobs for the task manager data lake:
- Hourly: ETL job (raw Avro → curated Parquet)
- Daily: Analytics aggregation job

Requires: Apache Airflow 2.7+ with KubernetesPodOperator
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.pod import Pod

# Default arguments for all tasks
default_args = {
    'owner': 'task-manager',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Kubernetes configuration
KUBERNETES_NAMESPACE = 'spark'
SPARK_IMAGE = 'task-manager-spark:latest'

# Secrets for Ozone and PostgreSQL
ozone_access_key = Secret(
    deploy_type='env',
    deploy_target='OZONE_ACCESS_KEY',
    secret='ozone-credentials',
    key='access-key'
)
ozone_secret_key = Secret(
    deploy_type='env',
    deploy_target='OZONE_SECRET_KEY',
    secret='ozone-credentials',
    key='secret-key'
)
pg_password = Secret(
    deploy_type='env',
    deploy_target='PG_PASSWORD',
    secret='task-manager-secrets',
    key='PG_PASSWORD'
)

# ─── Hourly ETL DAG ────────────────────────────────────────────────────────
with DAG(
    dag_id='task_manager_etl_hourly',
    default_args=default_args,
    description='ETL: Raw Avro → Curated Parquet (hourly)',
    schedule_interval='5 * * * *',  # Run at minute 5 of every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['task-manager', 'etl', 'spark'],
) as dag_etl:

    etl_job = KubernetesPodOperator(
        task_id='run_etl_batch_job',
        name='etl-batch-job',
        namespace=KUBERNETES_NAMESPACE,
        image=SPARK_IMAGE,
        cmds=['/opt/spark/bin/spark-submit'],
        arguments=[
            '--class', 'com.taskmanager.spark.EtlBatchJob',
            '--master', 'k8s://https://kubernetes.default.svc:443',
            '--deploy-mode', 'cluster',
            '--conf', 'spark.kubernetes.namespace=spark',
            '--conf', 'spark.kubernetes.container.image=' + SPARK_IMAGE,
            '--conf', 'spark.executor.instances=2',
            '--conf', 'spark.executor.memory=1g',
            '--conf', 'spark.driver.memory=1g',
            '--conf', 'spark.hadoop.fs.s3a.endpoint=http://ozone-s3g.ozone.svc.cluster.local:9878',
            '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
            '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            'local:///opt/spark/jars/task-manager-spark.jar',
            '{{ ds }}',  # Processing date (yesterday by default in job)
        ],
        secrets=[ozone_access_key, ozone_secret_key],
        get_logs=True,
        log_events_on_failure=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

# ─── Daily Analytics DAG ───────────────────────────────────────────────────
with DAG(
    dag_id='task_manager_analytics_daily',
    default_args=default_args,
    description='Analytics aggregation: compute daily metrics (daily)',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['task-manager', 'analytics', 'spark'],
) as dag_analytics:

    analytics_job = KubernetesPodOperator(
        task_id='run_analytics_aggregation_job',
        name='analytics-aggregation-job',
        namespace=KUBERNETES_NAMESPACE,
        image=SPARK_IMAGE,
        cmds=['/opt/spark/bin/spark-submit'],
        arguments=[
            '--class', 'com.taskmanager.spark.AnalyticsAggregationJob',
            '--master', 'k8s://https://kubernetes.default.svc:443',
            '--deploy-mode', 'cluster',
            '--conf', 'spark.kubernetes.namespace=spark',
            '--conf', 'spark.kubernetes.container.image=' + SPARK_IMAGE,
            '--conf', 'spark.executor.instances=2',
            '--conf', 'spark.executor.memory=1g',
            '--conf', 'spark.driver.memory=1g',
            '--conf', 'spark.hadoop.fs.s3a.endpoint=http://ozone-s3g.ozone.svc.cluster.local:9878',
            '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
            '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            'local:///opt/spark/jars/task-manager-spark.jar',
            '{{ ds }}',  # Processing date
        ],
        secrets=[ozone_access_key, ozone_secret_key, pg_password],
        get_logs=True,
        log_events_on_failure=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

# ─── Full Pipeline DAG (manual trigger) ────────────────────────────────────
with DAG(
    dag_id='task_manager_full_pipeline',
    default_args=default_args,
    description='Full pipeline: ETL + Analytics (manual trigger)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['task-manager', 'full-pipeline', 'manual'],
) as dag_full:

    etl_step = KubernetesPodOperator(
        task_id='run_etl_batch_job',
        name='etl-batch-job',
        namespace=KUBERNETES_NAMESPACE,
        image=SPARK_IMAGE,
        cmds=['/opt/spark/bin/spark-submit'],
        arguments=[
            '--class', 'com.taskmanager.spark.EtlBatchJob',
            '--master', 'k8s://https://kubernetes.default.svc:443',
            '--deploy-mode', 'cluster',
            '--conf', 'spark.kubernetes.namespace=spark',
            '--conf', 'spark.kubernetes.container.image=' + SPARK_IMAGE,
            '--conf', 'spark.executor.instances=2',
            '--conf', 'spark.hadoop.fs.s3a.endpoint=http://ozone-s3g.ozone.svc.cluster.local:9878',
            '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
            '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            'local:///opt/spark/jars/task-manager-spark.jar',
            '{{ ds }}',
        ],
        secrets=[ozone_access_key, ozone_secret_key],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    analytics_step = KubernetesPodOperator(
        task_id='run_analytics_aggregation_job',
        name='analytics-aggregation-job',
        namespace=KUBERNETES_NAMESPACE,
        image=SPARK_IMAGE,
        cmds=['/opt/spark/bin/spark-submit'],
        arguments=[
            '--class', 'com.taskmanager.spark.AnalyticsAggregationJob',
            '--master', 'k8s://https://kubernetes.default.svc:443',
            '--deploy-mode', 'cluster',
            '--conf', 'spark.kubernetes.namespace=spark',
            '--conf', 'spark.kubernetes.container.image=' + SPARK_IMAGE,
            '--conf', 'spark.executor.instances=2',
            '--conf', 'spark.hadoop.fs.s3a.endpoint=http://ozone-s3g.ozone.svc.cluster.local:9878',
            '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
            '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            'local:///opt/spark/jars/task-manager-spark.jar',
            '{{ ds }}',
        ],
        secrets=[ozone_access_key, ozone_secret_key, pg_password],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # Pipeline: ETL → Analytics
    etl_step >> analytics_step
