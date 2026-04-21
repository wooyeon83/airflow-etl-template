"""기준일자 Variable 조회 유틸."""
from datetime import datetime

from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from pytz import timezone

_KST = timezone("Asia/Seoul")


def _fallback_variable(primary_key: str, secondary_key: str, expected_len: int) -> str | None:
    """primary → secondary 순으로 Variable 조회 (길이 검증 포함)."""
    for key in (primary_key, secondary_key):
        try:
            value = Variable.get(key)
        except KeyError:
            continue
        if value and len(value) == expected_len:
            return value
    return None


def get_base_ymd(to_tuple: bool = False):
    """기준일자(YYYYMMDD). 없으면 어제 날짜."""
    value = _fallback_variable("base_ymd", "base_ymd_online", 8)
    if value is None:
        value = (datetime.now(tz=_KST) - relativedelta(days=1)).strftime("%Y%m%d")
    if to_tuple:
        return value[:4], value[4:6], value[6:]
    return value


def get_base_ym(to_tuple: bool = False):
    """기준년월(YYYYMM). 없으면 전월."""
    value = _fallback_variable("base_ym", "base_ym_online", 6)
    if value is None:
        value = (datetime.now(tz=_KST) - relativedelta(months=1)).strftime("%Y%m")
    if to_tuple:
        return value[:4], value[4:]
    return value


def get_base_qtr() -> tuple[int, int]:
    """기준분기: (year, quarter). Variable 미존재 시 현재 시점의 직전 분기."""
    try:
        raw = Variable.get("base_qtr")
        year, quarter = raw.split(",")
        return int(year), int(quarter)
    except KeyError:
        pass

    now = datetime.now(tz=_KST)
    if now.month <= 3:
        return now.year - 1, 4
    if now.month <= 6:
        return now.year, 1
    if now.month <= 9:
        return now.year, 2
    return now.year, 3
