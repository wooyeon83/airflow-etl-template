"""RDS(PostgreSQL) 저장 프로시저 호출 Operator."""
from typing import Any, Sequence

from airflow.models import BaseOperator

from dependencies.hooks.rds import RDSHook


class RDSProcedureOperator(BaseOperator):
    """
    ``CALL schema.procedure(...)`` 를 실행하는 단순 Operator.

    :param schema: 대상 스키마명
    :param procedure_name: 프로시저명
    :param secret: Secrets Manager ID (hook 를 직접 넣지 않은 경우 사용)
    :param parameter: 프로시저 인자.
        - ``tuple`` / ``list``: 그대로 인자로 확장
        - ``int``: 단일 정수 인자
        - ``str``: **콤마로 분해** → ``(a,b)`` 형태의 복수 인자에만 사용.
          단일 값은 ``int`` 또는 ``(value,)`` 튜플 권장
        - ``dict``: values 순서로 확장
    :param hook: 재사용할 RDSHook (None 이면 secret 으로 자동 생성)
    """

    template_fields: Sequence[str] = ("schema", "procedure_name", "parameter")
    ui_color = "#ffffe0"

    def __init__(
        self,
        schema: str,
        procedure_name: str,
        secret: str | None = None,
        parameter: Any = None,
        hook: RDSHook | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.schema = schema
        self.procedure_name = procedure_name
        self.secret = secret
        self.parameter = parameter
        self._hook = hook

    @property
    def hook(self) -> RDSHook:
        if self._hook is None:
            if self.secret is None:
                raise ValueError("`hook` 또는 `secret` 중 하나는 반드시 전달해야 합니다")
            self._hook = RDSHook(secret_manager_id=self.secret)
        return self._hook

    def _build_sql(self) -> tuple[str, tuple | None]:
        if self.parameter is None:
            return f"CALL {self.schema}.{self.procedure_name}();", None

        if isinstance(self.parameter, tuple):
            params = self.parameter
        elif isinstance(self.parameter, int):
            params = (self.parameter,)
        elif isinstance(self.parameter, str):
            params = tuple(self.parameter.split(","))
        elif isinstance(self.parameter, list):
            params = tuple(self.parameter)
        elif isinstance(self.parameter, dict):
            params = tuple(self.parameter.values())
        else:
            raise TypeError("parameter 는 tuple | list | dict | int | str 중 하나여야 합니다")

        placeholders = ",".join(["%s"] * len(params))
        return f"CALL {self.schema}.{self.procedure_name}({placeholders});", params

    def execute(self, context) -> None:
        sql, params = self._build_sql()
        self.log.info("Executing: %s | params=%s", sql, params)
        self.hook.execute(sql, params)
