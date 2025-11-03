import re
from textwrap import dedent

import pytest

from logrep.logrep_server import get_matching_lines, RX_ESCAPE_FOLLOWED_BY_SPECIAL, is_probably_complex_pattern, MatchType


@pytest.fixture
def context(tmp_path):
    file_path = tmp_path / 'file_path.log'
    file_path.write_text(dedent('''\
    line 1 one uno
    line 2 two dos
    line 3 three tres
    line 4 four cuatro
    line 5.five cinco
    line 6 six seis
    line 7 seven siete
    line 8 eight ocho
    line 9 nine nueve
    line 10 ten diez
    line 11 eleven once
    line 12 twelve doce
    line 13 thirteen trece
    line 14 fourteen catorce
    line 15.fifteen quince
    line 16 sixteen dieciséis
    line 17 seventeen diecisiete
    line 18 eighteen dieciocho
    line 19 nineteen diecinueve
    line 20 twenty veinte
    '''))
    d = dict(file_path=file_path)
    yield d
    file_path.unlink()


pytestmark = pytest.mark.asyncio  # [pip install pytest-asyncio] mark all tests in the module as async


# @pytest.mark.asyncio  # an alternative way to mark individual tests as async
async def test_get_matching_lines__pattern_str_plain(context):
    file_path = context['file_path']
    pattern = 'four'
    expected = [
        (4, MatchType.pattern, 'line 4 four cuatro'),
        (14, MatchType.pattern, 'line 14 fourteen catorce'),
    ]
    actual = await get_matching_lines(file_path, None, 0, pattern, 0, None)
    assert actual == expected


async def test_get_matching_lines__pattern_rx_plain(context):
    file_path = context['file_path']
    pattern = re.compile('1?4')
    expected = [
        (4, MatchType.pattern, 'line 4 four cuatro'),
        (14, MatchType.pattern, 'line 14 fourteen catorce'),
    ]
    actual = await get_matching_lines(file_path, None, 0, pattern, 0, None)
    assert actual == expected


async def test_get_matching_lines__pattern_str_escaped(context):
    file_path = context['file_path']
    pattern = RX_ESCAPE_FOLLOWED_BY_SPECIAL.sub('', '5\.')
    expected = [
        (5, MatchType.pattern, 'line 5.five cinco'),
        (15, MatchType.pattern, 'line 15.fifteen quince'),
    ]
    actual = await get_matching_lines(file_path, None, 0, pattern, 0, None)
    assert actual == expected


async def test_get_matching_lines__pattern_rx_escaped(context):
    file_path = context['file_path']
    pattern = '1?5\.'
    assert is_probably_complex_pattern(pattern) is True
    pattern = re.compile(pattern)
    expected = [
        (5, MatchType.pattern, 'line 5.five cinco'),
        (15, MatchType.pattern, 'line 15.fifteen quince'),
    ]
    actual = await get_matching_lines(file_path, None, 0, pattern, 0, None)
    assert actual == expected


async def test_get_matching_lines__discard_after_str_plain(context):
    file_path = context['file_path']
    discard_after = '2'
    expected = [
        (2, MatchType.discard_after, 'line 2 two dos'),
    ]
    actual = await get_matching_lines(file_path, None, 0, None, 0, discard_after)
    assert actual == expected


async def test_get_matching_lines__discard_before_str_plain(context):
    file_path = context['file_path']
    discard_before = '19'
    expected = [
        (19, MatchType.discard_before, 'line 19 nineteen diecinueve'),
    ]
    actual = await get_matching_lines(file_path, discard_before, 0, None, 0, None)
    assert actual == expected


async def test_get_matching_lines__pattern__discard_after(context):
    file_path = context['file_path']
    pattern = '1'
    discard_after = '11'
    expected = [
        (1, MatchType.pattern, 'line 1 one uno'),
        (10, MatchType.pattern, 'line 10 ten diez'),
        (11, MatchType.discard_after, 'line 11 eleven once'),
    ]
    actual = await get_matching_lines(file_path, None, 0, pattern, 0, discard_after)
    assert actual == expected


async def test_get_matching_lines__pattern__discard_before(context):
    file_path = context['file_path']
    discard_before = '11'
    pattern = '2'
    expected = [
        (11, MatchType.discard_before, 'line 11 eleven once'),
        (12, MatchType.pattern, 'line 12 twelve doce'),
        (20, MatchType.pattern, 'line 20 twenty veinte'),
    ]
    actual = await get_matching_lines(file_path, discard_before, 0, pattern, 0, None)
    assert actual == expected


async def test_get_matching_lines__before_context__pattern__after_context__discard_after(context):
    file_path = context['file_path']
    before_context = 1
    pattern = '1'
    after_context = 1
    discard_after = '11'
    expected = [
        (1, MatchType.pattern, 'line 1 one uno'),
        (2, MatchType.after_context, 'line 2 two dos'),
        (9, MatchType.before_context, 'line 9 nine nueve'),
        (10, MatchType.pattern, 'line 10 ten diez'),
        (11, MatchType.discard_after, 'line 11 eleven once'),
    ]
    actual = await get_matching_lines(file_path, None, before_context, pattern, after_context, discard_after)
    assert actual == expected


async def test_get_matching_lines__discard_before__before_context__pattern__after_context(context):
    file_path = context['file_path']
    discard_before = '11'
    before_context = 1
    pattern = '2'
    after_context = 1
    expected = [
        (11, MatchType.discard_before, 'line 11 eleven once'),
        (11, MatchType.before_context, 'line 11 eleven once'),
        (12, MatchType.pattern, 'line 12 twelve doce'),
        (13, MatchType.after_context, 'line 13 thirteen trece'),
        (19, MatchType.before_context, 'line 19 nineteen diecinueve'),
        (20, MatchType.pattern, 'line 20 twenty veinte'),
    ]
    actual = await get_matching_lines(file_path, discard_before, before_context, pattern, after_context, None)
    assert actual == expected


async def test_get_matching_lines__pattern__after_context__discard_before__same_as_pattern(context):
    file_path = context['file_path']
    discard_before = '11'
    pattern = '11'
    after_context = 1
    expected = [
        (11, MatchType.discard_before, 'line 11 eleven once'),
        (11, MatchType.pattern, 'line 11 eleven once'),
        (12, MatchType.after_context, 'line 12 twelve doce'),
    ]
    actual = await get_matching_lines(file_path, discard_before, 0, pattern, after_context, None)
    assert actual == expected


async def test_get_matching_lines__discard_before__matches_multiple_lines__expected_last_match(context):
    file_path = context['file_path']
    discard_before = 'four'
    before_context = 0
    pattern = None
    after_context = 0
    discard_after = None
    expected = [
        (14, MatchType.discard_before, 'line 14 fourteen catorce'),
    ]
    actual = await get_matching_lines(file_path, discard_before, before_context, pattern, after_context, discard_after)
    assert actual == expected
