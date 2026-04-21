"""EMR Serverless 작업 실행 & 상태 폴링 유틸."""
import time
from datetime import datetime

import boto3
import pytz


def _emr_client(region_name: str = "ap-northeast-2"):
    return boto3.client("emr-serverless", region_name=region_name)


def check_job_status(
    job_run_id: str,
    application_id: str,
    max_pokes: int = 60,
    poke_interval: int = 60,
) -> bool:
    """EMR Serverless 잡 상태를 주기적으로 조회한다.

    성공 시 True, 실패/취소/Timeout 시 예외를 raise 한다.
    """
    client = _emr_client()
    for attempt in range(1, max_pokes + 1):
        job_runs = client.list_job_runs(applicationId=application_id).get("jobRuns", [])
        job = next((j for j in job_runs if j["id"] == job_run_id), None)
        if job is not None:
            state = job["state"]
            print(f"[{attempt}/{max_pokes}] EMR job_run={job_run_id} state={state}")
            if state == "SUCCESS":
                return True
            if state in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"EMR Serverless job failed: {state}")
        time.sleep(poke_interval)

    raise TimeoutError(f"EMR Serverless job did not finish in {max_pokes * poke_interval}s")


def start_emr_serverless_task(
    script_name: str,
    exec_date: str,
    application_id: str,
    iam_role_emr: str,
    s3_bucket_nm_emr: str,
    entry_point_args: list[str] | None = None,
    extra_py_files: list[str] | None = None,
    spark_submit_parameters: str | None = None,
) -> bool:
    """
    EMR Serverless 에 Spark 잡을 시작하고 상태를 폴링한다.

    :param script_name: S3 상의 ``jobs/script/{script_name}.py`` 를 실행
    :param exec_date: 스크립트에 전달할 기준일자 (``sys.argv[1]``)
    :param application_id: EMR Serverless Application ID
    :param iam_role_emr: 실행 IAM Role ARN
    :param s3_bucket_nm_emr: 스크립트/로그가 있는 S3 버킷
    :param entry_point_args: ``sys.argv[2:]`` 로 전달할 추가 인자
    :param extra_py_files: ``--py-files`` 로 붙일 S3 경로 리스트
    :param spark_submit_parameters: 커스텀 spark-submit 파라미터 문자열
    """
    now_kst = datetime.now(pytz.timezone("Asia/Seoul"))
    client = _emr_client()

    args = [exec_date] + (entry_point_args or [])

    default_spark_params = (
        "--conf spark.dynamicAllocation.enabled=true "
        "--conf spark.driver.cores=4 "
        "--conf spark.driver.memory=16g "
        "--conf spark.executor.cores=4 "
        "--conf spark.executor.memory=16g "
        "--conf spark.emr-serverless.memoryOverheadFactor=0.1 "
        "--conf spark.dynamicAllocation.minExecutors=3 "
        "--conf spark.dynamicAllocation.maxExecutors=10 "
        "--conf spark.emr-serverless.executor.disk.type=shuffle_optimized "
    )
    spark_params = spark_submit_parameters or default_spark_params
    if extra_py_files:
        spark_params = f"--py-files {','.join(extra_py_files)} " + spark_params

    response = client.start_job_run(
        applicationId=application_id,
        executionRoleArn=iam_role_emr,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": f"s3://{s3_bucket_nm_emr}/jobs/script/{script_name}.py",
                "entryPointArguments": args,
                "sparkSubmitParameters": spark_params,
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": (
                        f"s3://{s3_bucket_nm_emr}/emr-logs/"
                        f"yyyy={now_kst.year}/mm={now_kst.month:02d}/dd={now_kst.day:02d}/"
                    )
                }
            }
        },
        name=f"{script_name}_{exec_date}",
    )

    job_run_id = response["jobRunId"]
    print(f"Started EMR job: {job_run_id}")
    return check_job_status(job_run_id, application_id)
