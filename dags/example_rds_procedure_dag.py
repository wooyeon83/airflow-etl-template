# /*********************************************************************************************
#  Program Name          :  AFEC_B001_DATA_TRANSFORM_D01_DAG
#  Program Description   :  데이터레이크 검증 → RDS 프로시저 호출 (Transform 템플릿)
# *********************************************************************************************
#  Parameters (Airflow Variable)
#  1. base_ymd                  :  기준일자 (YYYYMMDD)
#  2. ext_db_nm                 :  Athena 외부연계 Glue DB
#  3. sec_manager_core_rds      :  RDS Secrets Manager ID
# *********************************************************************************************
#  Target Tables         :  datamart_db.dm_summary_daily
#                           datamart_db.dm_segment_summary
# *********************************************************************************************
#  [Pattern] Transform
#  업스트림 DAG (EXT_SFTP_TO_S3) 완료 후,
#    1) Athena 조회로 적재된 행 수를 검증 (0건이면 실패)
#    2) 검증 통과 시 RDS(PostgreSQL) 프로시저를 호출하여 요약 테이블 적재
#  실패 시 SLA Miss 발생하지 않도록 재시도를 설정한다.
# *********************************************************************************************/

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from dependencies.hooks.rds import RDSHook
from dependencies.operators.rds_procedure import RDSProcedureOperator
from dependencies.operators.row_count_validator import AthenaRowCountValidator

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

# 업스트림 DAG (Extract 단계가 먼저 끝나야 하는 경우)
UPSTREAM_DAG_ID = "AFEC_A001_EXT_SFTP_TO_S3_DAG"

# 검증 대상: Athena 테이블명과 파티션 식
# partition 값은 Jinja 템플릿으로 런타임에 평가됨
VALIDATION_TARGETS = [
    {"table": "source_table_a", "partition": "partn_ymd={{ var.value.base_ymd }}"},
    {"table": "source_table_b", "partition": "partn_ymd={{ var.value.base_ymd }}"},
]

# 프로시저 호출 정의
# parameter 는 Jinja 템플릿으로 런타임에 평가됨
PROCEDURES = [
    {
        "task_id": "SUMMARY_DAILY",
        "schema": "datamart_db",
        "procedure": "sp_summary_daily",
        "parameter": "{{ var.value.base_ymd }}",
    },
    {
        "task_id": "SEGMENT_SUMMARY",
        "schema": "datamart_db",
        "procedure": "sp_segment_summary",
        "parameter": "{{ var.value.base_ymd }}",
    },
]

# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------
with DAG(
    dag_id="AFEC_B001_DATA_TRANSFORM_D01_DAG",
    description="Athena 건수 검증 후 RDS 프로시저 호출 (Transform 템플릿)",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="0 5 * * *",  # 매일 05:00 KST (업스트림 03:00 이후)
    catchup=False,
    tags=["transform", "rds", "athena", "template"],
    is_paused_upon_creation=True,
) as dag:

    start = EmptyOperator(task_id="START")
    end = EmptyOperator(task_id="END")

    # 업스트림 DAG 완료 대기 (필요 시)
    wait_upstream = ExternalTaskSensor(
        task_id="S.WAIT_UPSTREAM_EXTRACT",
        external_dag_id=UPSTREAM_DAG_ID,
        external_task_id=None,  # 전체 DAG 완료 대기
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 2,  # 2시간
        check_existence=True,
    )

    # 1) Athena 기반 건수 검증 (병렬)
    # database 는 Jinja 템플릿으로 런타임에 읽어 파싱 시점 고정 방지
    with TaskGroup(group_id="TG_VALIDATION") as tg_validation:
        for item in VALIDATION_TARGETS:
            AthenaRowCountValidator(
                task_id=f"T.VALIDATE_{item['table'].upper()}",
                table=item["table"],
                partition_key=item["partition"],
                database="{{ var.value.ext_db_nm }}",
            )

    # 2) RDS 프로시저 호출 (Hook 재사용 - 공유 커넥션)
    shared_rds_hook = RDSHook(secret_manager_id=Variable.get("sec_manager_core_rds"))

    with TaskGroup(group_id="TG_PROCEDURES") as tg_procedures:
        prev = None
        for proc in PROCEDURES:
            task = RDSProcedureOperator(
                task_id=f"T.{proc['task_id']}",
                schema=proc["schema"],
                procedure_name=proc["procedure"],
                parameter=proc["parameter"],
                hook=shared_rds_hook,
            )
            if prev is not None:
                prev >> task
            prev = task

    # -------------------------------------------------------------------
    # 흐름:
    #   START -> 업스트림 DAG 대기 -> Athena 건수검증(병렬)
    #                            -> RDS 프로시저(순차) -> END
    # -------------------------------------------------------------------
    start >> wait_upstream >> tg_validation >> tg_procedures >> end
