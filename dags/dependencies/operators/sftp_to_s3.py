"""SFTP 서버의 파일을 S3 로 전송하는 Operator."""
import os
import tempfile
from typing import Sequence

import boto3
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from dependencies.hooks.sftp import SFTPHook
from dependencies.utils.date import get_base_ym, get_base_ymd


class SFTPToS3Operator(BaseOperator):
    """
    SFTP 의 `{sftp_root}/{table_name}/{partition}/` 경로의 모든 CSV(.gz) 를 S3 로 업로드한다.

    :param table_name: SFTP 폴더명 겸 S3 prefix 로 사용
    :param loading_cycle: ``daily`` | ``monthly``
    :param s3_bucket: 업로드 대상 S3 버킷
    :param sftp_root: SFTP 루트 디렉터리 (default: ``/upload``)
    :param s3_prefix: S3 상위 prefix (default: 빈 문자열)
    :param sftp_variable_key: SFTP 인증정보를 담은 Variable key
    :param hook: 재사용할 SFTPHook (None 이면 자동 생성)
    """

    template_fields: Sequence[str] = ("table_name", "s3_bucket", "s3_prefix")
    ui_color = "#256e1b"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        table_name: str,
        loading_cycle: str,
        s3_bucket: str,
        sftp_root: str = "/upload",
        s3_prefix: str = "",
        sftp_variable_key: str = "ext_sftp_authorization",
        hook: SFTPHook | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.loading_cycle = loading_cycle
        self.s3_bucket = s3_bucket
        self.sftp_root = sftp_root.rstrip("/")
        self.s3_prefix = s3_prefix.strip("/")
        self.sftp_variable_key = sftp_variable_key
        self._hook = hook

    @property
    def hook(self) -> SFTPHook:
        if self._hook is None:
            self._hook = SFTPHook(sftp_conn_id=self.sftp_variable_key)
        return self._hook

    def _partition_sftp(self) -> str:
        if self.loading_cycle == "daily":
            return get_base_ymd()
        if self.loading_cycle == "monthly":
            return get_base_ym()
        raise ValueError(f"지원하지 않는 loading_cycle: {self.loading_cycle} (daily | monthly 만 허용)")

    def _partition_s3(self) -> str:
        if self.loading_cycle == "daily":
            return f"partn_ymd={get_base_ymd()}"
        if self.loading_cycle == "monthly":
            return f"partn_ym={get_base_ym()}"
        raise ValueError(f"지원하지 않는 loading_cycle: {self.loading_cycle} (daily | monthly 만 허용)")

    def execute(self, context) -> list[str]:
        sftp = self.hook.get_conn()
        s3 = boto3.client("s3")

        sftp_dir = f"{self.sftp_root}/{self.table_name}/{self._partition_sftp()}"
        try:
            sftp.chdir(sftp_dir)
        except FileNotFoundError as exc:
            raise AirflowException(f"SFTP 경로가 존재하지 않습니다: {sftp_dir}") from exc

        files = [f for f in sftp.listdir() if f.endswith((".csv", ".csv.gz"))]
        if not files:
            raise AirflowException(f"대상 파일이 없습니다: {sftp_dir}")

        uploaded_keys: list[str] = []
        # 동시 실행 태스크 간 /tmp 파일명 충돌 방지를 위해 임시 디렉터리 사용
        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                for filename in files:
                    local_path = os.path.join(tmp_dir, filename)
                    s3_key = "/".join(
                        part
                        for part in (self.s3_prefix, self.table_name, self._partition_s3(), filename)
                        if part
                    )
                    sftp.get(filename, local_path)
                    if not os.path.exists(local_path):
                        raise AirflowException(f"{filename} 다운로드 실패")
                    s3.upload_file(local_path, self.s3_bucket, s3_key)
                    uploaded_keys.append(s3_key)
                    self.log.info("Uploaded: %s -> s3://%s/%s", filename, self.s3_bucket, s3_key)
            finally:
                s3.close()

        return uploaded_keys
