"""Athena 조회 건수 기반 검증 Operator."""
from typing import Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Variable

from dependencies.hooks.athena import AthenaHook


class AthenaRowCountValidator(BaseOperator):
    """
    파티션 조건으로 테이블 행 수를 조회하고 기대값과 비교한다.

    :param table: 검증 대상 테이블명
    :param partition_key: ``partn_ymd=20260420/partn_gubn=A`` 형태의 파티션 식
    :param expected_count: 기대 행 수. 음수이면 ``> 0`` 체크만 수행 (default: -1)
    :param database: Athena DB (미지정 시 ``ext_db_nm`` Variable 사용)
    :param hook: 재사용할 AthenaHook (None 이면 자동 생성)
    """

    template_fields: Sequence[str] = ("table", "database", "partition_key", "expected_count")
    ui_color = "#b66de3"

    def __init__(
        self,
        table: str,
        partition_key: str,
        expected_count: int = -1,
        database: str | None = None,
        hook: AthenaHook | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table = table
        self.partition_key = partition_key
        self.expected_count = expected_count
        self.database = database
        self._hook = hook

    @property
    def hook(self) -> AthenaHook:
        if self._hook is None:
            self._hook = AthenaHook()
        return self._hook

    def _where_clause(self) -> str:
        tokens = [token for token in self.partition_key.split("/") if token]
        conditions = []
        for token in tokens:
            key, value = token.split("=", 1)
            conditions.append(f"{key} = '{value}'")
        return "WHERE " + " AND ".join(conditions) if conditions else ""

    def execute(self, context) -> int:
        database = self.database or Variable.get("ext_db_nm")
        query = f"SELECT count(1) AS cnt FROM {database}.{self.table} {self._where_clause()};"
        self.log.info("Validation query: %s", query)

        rows = self.hook.execute_athena_query(query, database=database)
        if len(rows) != 2:
            raise AirflowException(f"예상치 못한 검증 결과 행 수: {rows}")

        actual = int(rows[1]["Data"][0]["VarCharValue"])
        self.log.info("Row count: %s (expected=%s)", actual, self.expected_count)

        if self.expected_count < 0:
            if actual <= 0:
                raise AirflowException(f"행 수가 0입니다: {self.table}")
        elif actual != self.expected_count:
            raise AirflowException(
                f"검증 실패: expected={self.expected_count}, actual={actual} ({self.table})"
            )
        return actual
