"""Airflow Variable 에 저장된 자격증명을 이용한 SFTP Hook."""
import paramiko
from airflow.hooks.base import BaseHook
from airflow.models import Variable


class SFTPHook(BaseHook):
    """
    Variable(JSON) 기반 SFTP 연결 Hook.

    Variable 예시 (key=`ext_sftp_authorization`)::

        {
            "sftp_host": "sftp.example.com",
            "sftp_port": 22,
            "sftp_username": "user",
            "sftp_password": "****"
        }
    """

    def __init__(self, sftp_conn_id: str):
        super().__init__()
        self.sftp_conn_info = Variable.get(sftp_conn_id, deserialize_json=True)
        self._transport: paramiko.Transport | None = None
        self._sftp: paramiko.SFTPClient | None = None

    def get_conn(self) -> paramiko.SFTPClient:
        if self._sftp is None:
            self._transport = paramiko.Transport(
                (self.sftp_conn_info["sftp_host"], int(self.sftp_conn_info["sftp_port"]))
            )
            self._transport.connect(
                username=self.sftp_conn_info["sftp_username"],
                password=self.sftp_conn_info["sftp_password"],
            )
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            self.log.info("SFTP connection established: %s", self.sftp_conn_info["sftp_host"])
        return self._sftp

    def close(self) -> None:
        if self._sftp is not None:
            self._sftp.close()
            self._sftp = None
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        self.log.info("SFTP connection closed")

    def __del__(self):
        try:
            self.close()
        except Exception:  # noqa: BLE001
            pass
