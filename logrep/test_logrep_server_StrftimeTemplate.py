from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from logrep.logrep_server import StrftimeTemplate


def _calc_actual_expected(tzstr: str, offset: tuple[int, int] | timedelta):
    if isinstance(offset, timedelta):
        _tzinfo = timezone(offset)
    else:
        _tzinfo = timezone(timedelta(hours=offset[0], minutes=offset[1]))
    expected = f"/log/log-{datetime.now().astimezone(_tzinfo).strftime('%Y-%m-%d_%H,%M,%S')}.log"
    actual = StrftimeTemplate('/log/log-<%Y-%m-%d_%H,%M,%S>.log').substitute({'timezone': tzstr})
    return actual, expected


def test_logrep_server_StrftimeTemplate__plusHH_MM():
    actual, expected = _calc_actual_expected('+02:00', (2, 00))
    assert actual == expected


def test_logrep_server_StrftimeTemplate__minusHH_MM():
    actual, expected = _calc_actual_expected('-03:30', (-3, -30))
    assert actual == expected


def test_logrep_server_StrftimeTemplate__UTCplusHH_MM():
    actual, expected = _calc_actual_expected('UTC+12:45', (12, 45))
    assert actual == expected


def test_logrep_server_StrftimeTemplate__UTCminusHH_MM():
    actual, expected = _calc_actual_expected('UTC-11:00', (-11, -00))
    assert actual == expected


def test_logrep_server_StrftimeTemplate__Australia_Sydney():
    offset = ZoneInfo('Australia/Sydney').utcoffset(datetime.now())
    actual, expected = _calc_actual_expected('Australia/Sydney', offset)
    assert actual == expected


def test_logrep_server_StrftimeTemplate__America_Toronto():
    offset = ZoneInfo('America/Toronto').utcoffset(datetime.now())
    actual, expected = _calc_actual_expected('America/Toronto', offset)
    assert actual == expected
