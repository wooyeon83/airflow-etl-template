"""AWS Secrets Manager 기반 RDS(PostgreSQL) 연결 Hook."""
import json

import boto3
import psycopg2
from airflow.hooks.base import BaseHook


class RDSHook(BaseHook):
    """
    Secrets Manager 에 저장된 자격증명으로 RDS PostgreSQL 연결을 생성한다.

    Secret JSON 은 아래 키를 포함해야 한다:
        host, port, dbname, username, password

    주의: ``__init__`` 에서 즉시 연결을 생성하므로 모듈 스코프에서 인스턴스화하면
    DAG 파싱(스케줄러 heartbeat)마다 Secrets Manager 호출 + DB 연결이 발생한다.
    """

    def __init__(self, secret_manager_id: str, region_name: str = "ap-northeast-2"):
        super().__init__()
        self.secret_manager_id = secret_manager_id
        self.region_name = region_name
        self.conn = self._connect()
        self.conn.autocommit = True

    def _get_secret(self) -> dict:
        client = boto3.client("secretsmanager", region_name=self.region_name)
        response = client.get_secret_value(SecretId=self.secret_manager_id)
        return json.loads(response["SecretString"])

    def _connect(self):
        secret = self._get_secret()
        conn = psycopg2.connect(
            host=secret["host"],
            port=secret["port"],
            dbname=secret["dbname"],
            user=secret["username"],
            password=secret["password"],
        )
        self.log.info("RDS connection established (db=%s)", secret["dbname"])
        return conn

    def execute(self, sql: str, params: tuple | None = None) -> None:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
        # autocommit=True 이므로 명시적 commit 불필요

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __del__(self):
        try:
            self.close()
        except Exception:  # noqa: BLE001
            pass
