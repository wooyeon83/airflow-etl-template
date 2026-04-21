# /*********************************************************************************************
#  Program Name          :  AFEC_A001_EXT_SFTP_TO_S3_DAG
#  Program Description   :  외부 SFTP -> S3 (RAW) 적재 템플릿 DAG
# *********************************************************************************************
#  Parameters (Airflow Variable)
#  1. base_ymd                  :  기준일자 (YYYYMMDD, D-1)
#  2. s3_bucket_nm_ext          :  외부연계 S3 버킷
#  3. ext_sftp_authorization    :  SFTP 접속정보 (JSON)
# *********************************************************************************************
#  Target S3 Prefix      :  s3://{s3_bucket_nm_ext}/external/{table}/partn_ymd={base_ymd}/
# *********************************************************************************************
#  [Pattern] Extract
#  외부 SFTP 서버에 업로드된 CSV 파일을 스캔하여 S3 RAW 버킷으로 전송.
#  여러 테이블(파일군)을 TaskGroup 으로 묶어 병렬 처리한다.
# *********************************************************************************************/

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from dependencies.hooks.sftp import SFTPHook
from dependencies.operators.sftp_to_s3 import SFTPToS3Operator

# -----------------------------------------------------------------------------
# 전역 설정
# -----------------------------------------------------------------------------
KST = pendulum.timezone("Asia/Seoul")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

# SFTP 에서 가져올 테이블(파일 그룹) 목록
#   - loading_cycle: daily | monthly
SFTP_TABLES: list[dict] = [
    {"table_name": "source_table_a", "loading_cycle": "daily"},
    {"table_name": "source_table_b", "loading_cycle": "daily"},
    {"table_name": "source_table_c", "loading_cycle": "monthly"},
]

SFTP_VARIABLE_KEY = "ext_sftp_authorization"
SFTP_ROOT = "/upload/data"
S3_PREFIX = "external"

# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------
with DAG(
    dag_id="AFEC_A001_EXT_SFTP_TO_S3_DAG",
    description="외부 SFTP -> S3 RAW 적재 (Extract 템플릿)",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="0 3 * * *",  # 매일 03:00 KST
    catchup=False,
    tags=["extract", "sftp", "s3", "template"],
    is_paused_upon_creation=True,
) as dag:

    start = EmptyOperator(task_id="START")
    end = EmptyOperator(task_id="END")

    s3_bucket_nm_ext = Variable.get("s3_bucket_nm_ext")

    # SFTP 연결은 TaskGroup 간 재사용, 모든 Task 종료 후 close
    shared_sftp_hook = SFTPHook(sftp_conn_id=SFTP_VARIABLE_KEY)
    close_sftp = PythonOperator(
        task_id="T.CLOSE_SFTP_CONN",
        python_callable=shared_sftp_hook.close,
        trigger_rule="all_done",
    )

    # 테이블별 TaskGroup
    transfer_groups = []
    for item in SFTP_TABLES:
        table_name = item["table_name"]
        cycle = item["loading_cycle"]

        with TaskGroup(group_id=f"TG_{table_name.upper()}") as tg:
            SFTPToS3Operator(
                task_id=f"T.{table_name.upper()}_SFTP_TO_S3",
                table_name=table_name,
                loading_cycle=cycle,
                s3_bucket=s3_bucket_nm_ext,
                sftp_root=SFTP_ROOT,
                s3_prefix=S3_PREFIX,
                sftp_variable_key=SFTP_VARIABLE_KEY,
                hook=shared_sftp_hook,
            )

        transfer_groups.append(tg)

    # -------------------------------------------------------------------
    # 흐름:
    #   START -> (테이블별 SFTP→S3 전송 병렬) -> SFTP 연결 종료 -> END
    # -------------------------------------------------------------------
    start >> transfer_groups >> close_sftp >> end
