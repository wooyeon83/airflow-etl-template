"""AWS Athena 쿼리 실행 Hook (동기 대기)."""
import time

import boto3
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable


class AthenaHook(BaseHook):
    """
    Athena 쿼리를 실행하고 결과를 동기적으로 반환하는 경량 Hook.

    :param poke_interval: 상태 폴링 간격(초). 쿼리가 길수록 늘려 워커 부하 절감 (default: 5)
    :param region_name: AWS 리전 (default: ``ap-northeast-2``)

    주의: 자체 타임아웃이 없으므로 장시간 쿼리는 워커를 점유한다.
    Operator 레벨에서 ``execution_timeout`` 을 설정해 상한을 두는 것을 권장한다.
    """

    TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}

    def __init__(self, poke_interval: int = 5, region_name: str = "ap-northeast-2"):
        super().__init__()
        self.poke_interval = poke_interval
        self.region_name = region_name
        self.result_bucket = Variable.get("s3_bucket_nm_athena_result")
        self._client = None

    def get_conn(self):
        if self._client is None:
            self._client = boto3.client("athena", region_name=self.region_name)
        return self._client

    def execute_athena_query(self, query: str, database: str | None = None) -> list[dict]:
        """쿼리를 실행하고 결과 Rows 를 반환한다."""
        athena = self.get_conn()
        kwargs = {
            "QueryString": query,
            "ResultConfiguration": {"OutputLocation": f"s3://{self.result_bucket}/"},
        }
        if database:
            kwargs["QueryExecutionContext"] = {"Database": database}

        response = athena.start_query_execution(**kwargs)
        execution_id = response["QueryExecutionId"]

        while True:
            status = athena.get_query_execution(QueryExecutionId=execution_id)
            state = status["QueryExecution"]["Status"]["State"]
            if state in self.TERMINAL_STATES:
                if state != "SUCCEEDED":
                    reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
                    raise AirflowException(f"Athena query {state}: {reason}")
                break
            time.sleep(self.poke_interval)

        result = athena.get_query_results(QueryExecutionId=execution_id)
        return result["ResultSet"]["Rows"]

    def __del__(self):
        if self._client is not None:
            try:
                self._client.close()
            except Exception:  # noqa: BLE001
                pass
            self._client = None
