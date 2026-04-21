# /*********************************************************************************************
#  Program Name          :  AFZC_B001_CODE_TABLE_LOAD_DAG
#  Program Description   :  S3 코드/메타 CSV 파일 감지 → Glue Job 실행 → 적재 (on-demand)
# *********************************************************************************************
#  Parameters (Airflow Variable)
#  1. base_ymd                  :  기준일자
#  2. s3_bucket_nm_glue         :  Glue 스크립트/코드파일 S3 버킷
#  3. sec_manager_core_rds      :  타겟 RDS Secrets Manager ID
# *********************************************************************************************
#  Target Glue Jobs      :  GJ_CODE_GROUP_LOAD_1
#                           GJ_CODE_ITEM_LOAD_1
# *********************************************************************************************
#  [Pattern] Load (S3 Sensor -> Glue Job)
#  외부에서 업로드된 코드성 CSV 를 S3KeySensor 로 감지한 뒤,
#  GlueJobOperator 로 대상 RDS 에 적재한다.
# *********************************************************************************************/

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup

# -----------------------------------------------------------------------------
# 전역 설정
# -----------------------------------------------------------------------------
KST = pendulum.timezone("Asia/Seoul")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

# 적재 대상 코드 테이블 정의
CODE_TABLE_INFO: dict[str, dict] = {
    "CODE_GROUP": {
        "description": "통합 코드그룹 매핑",
        "schema_nm": "common_db",
        "glue_job_nm": "GJ_CODE_GROUP_LOAD_1",
    },
    "CODE_ITEM": {
        "description": "통합 코드 매핑",
        "schema_nm": "common_db",
        "glue_job_nm": "GJ_CODE_ITEM_LOAD_1",
    },
}

# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------
with DAG(
    dag_id="AFZC_B001_CODE_TABLE_LOAD_DAG",
    description="코드/메타 CSV S3 감지 후 Glue Job 으로 적재 (Load 템플릿)",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="@once",
    catchup=False,
    tags=["load", "glue", "s3", "template"],
    is_paused_upon_creation=True,
) as dag:

    start = EmptyOperator(task_id="START")
    end = EmptyOperator(task_id="END")

    s3_bucket_nm_glue = Variable.get("s3_bucket_nm_glue")
    sec_manager_core_rds = Variable.get("sec_manager_core_rds")

    group_list = []
    for table_key, meta in CODE_TABLE_INFO.items():
        table_nm = table_key.lower()
        schema_nm = meta["schema_nm"]
        glue_job_nm = meta["glue_job_nm"]

        with TaskGroup(group_id=f"TG_{table_key}") as tg:

            # 1) S3 파일 감지 (업스트림 적재 대기)
            wait_s3_file = S3KeySensor(
                task_id=f"S.{table_key}_S3_SENSOR",
                bucket_key=f"s3://{s3_bucket_nm_glue}/code-meta/{table_nm}/{table_nm}.csv",
                wildcard_match=True,
                poke_interval=30,
                timeout=60 * 10,  # 10분
                mode="reschedule",
            )

            # 2) Glue Job 실행 (CSV → 타겟 DB 적재)
            # base_ymd 는 Jinja 템플릿으로 런타임에 읽어 파싱 시점 고정 방지
            run_glue_job = GlueJobOperator(
                task_id=f"T.{glue_job_nm}",
                job_name=glue_job_nm,
                script_args={
                    "--JOB_NAME": glue_job_nm,
                    "--S3_BUCKET_NM": s3_bucket_nm_glue,
                    "--SECRET_MANAGER": sec_manager_core_rds,
                    "--CO_SCHEMA_NM": schema_nm,
                    "--CO_TABLE_NM": table_nm,
                    "--BASE_YMD": "{{ var.value.base_ymd }}",
                },
                wait_for_completion=True,
                verbose=False,
            )

            wait_s3_file >> run_glue_job

        group_list.append(tg)

    # -------------------------------------------------------------------
    # 흐름:
    #   START
    #      -> [코드테이블 1: S3 감지 -> Glue 적재]
    #      -> [코드테이블 2: S3 감지 -> Glue 적재]
    #         -> END
    # -------------------------------------------------------------------
    start >> group_list >> end
