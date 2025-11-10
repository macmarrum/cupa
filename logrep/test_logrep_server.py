import re
import tarfile
from textwrap import dedent

import pytest

from logrep.logrep_server import gen_matching_lines, RX_ESCAPE_FOLLOWED_BY_SPECIAL, is_probably_complex_pattern, RecordType


@pytest.fixture
def context(tmp_path):
    file_path = tmp_path / 'file_path.log'
    text = dedent('''\
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
    ''')
    file_path.write_text(text)
    file_path2 = tmp_path / 'file_path2.log'
    with file_path2.open('wt') as f:
        for line in text.splitlines(keepends=True):
            f.write('file_path2 ')
            f.write(line)
    file_path_tar_gz = tmp_path / 'file_path.tar.gz'
    with tarfile.open(file_path_tar_gz, 'w:gz') as tf:
        tf.add(file_path, arcname=file_path.name)
        tf.add(file_path2, arcname=file_path2.name)
    d = dict(
        file_path=file_path,
        file_path2=file_path2,
        file_path_tar_gz=file_path_tar_gz
    )
    yield d
    for path in d.values():
        path.unlink()


pytestmark = pytest.mark.asyncio  # [pip install pytest-asyncio] mark all tests in the module as async


# @pytest.mark.asyncio  # an alternative way to mark individual tests as async
async def test_gen_matching_lines__pattern_str_plain(context):
    file_path = context['file_path']
    discard_before = None
    before_context = 0
    pattern = 'four'
    except_pattern = None
    after_context = 0
    discard_after = None
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (4, RecordType.pattern, 'line 4 four cuatro'),
        (14, RecordType.pattern, 'line 14 fourteen catorce'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__pattern_rx_plain(context):
    file_path = context['file_path']
    discard_before = None
    before_context = 0
    pattern = re.compile('1?4')
    except_pattern = None
    after_context = 0
    discard_after = None
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (4, RecordType.pattern, 'line 4 four cuatro'),
        (14, RecordType.pattern, 'line 14 fourteen catorce'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__pattern_str_escaped(context):
    file_path = context['file_path']
    discard_before = None
    before_context = 0
    pattern = RX_ESCAPE_FOLLOWED_BY_SPECIAL.sub('', '5\.')
    except_pattern = None
    after_context = 0
    discard_after = None
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (5, RecordType.pattern, 'line 5.five cinco'),
        (15, RecordType.pattern, 'line 15.fifteen quince'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__pattern_rx_escaped(context):
    file_path = context['file_path']
    discard_before = None
    before_context = 0
    pattern = '1?5\.'
    assert is_probably_complex_pattern(pattern) is True
    pattern = re.compile(pattern)
    except_pattern = None
    after_context = 0
    discard_after = None
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (5, RecordType.pattern, 'line 5.five cinco'),
        (15, RecordType.pattern, 'line 15.fifteen quince'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__discard_after_str_plain(context):
    file_path = context['file_path']
    discard_before = None
    before_context = 0
    pattern = None
    except_pattern = None
    after_context = 0
    discard_after = '2'
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (2, RecordType.discard_after, 'line 2 two dos'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__discard_before_str_plain(context):
    file_path = context['file_path']
    discard_before = '19'
    before_context = 0
    pattern = None
    except_pattern = None
    after_context = 0
    discard_after = None
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (19, RecordType.discard_before, 'line 19 nineteen diecinueve'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__pattern__discard_after(context):
    file_path = context['file_path']
    discard_before = None
    before_context = 0
    pattern = '1'
    except_pattern = None
    after_context = 0
    discard_after = '11'
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (1, RecordType.pattern, 'line 1 one uno'),
        (10, RecordType.pattern, 'line 10 ten diez'),
        (11, RecordType.pattern, 'line 11 eleven once'),
        (11, RecordType.discard_after, 'line 11 eleven once'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__pattern__discard_before(context):
    file_path = context['file_path']
    discard_before = '11'
    before_context = 0
    pattern = '2'
    except_pattern = None
    after_context = 0
    discard_after = None
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (11, RecordType.discard_before, 'line 11 eleven once'),
        (12, RecordType.pattern, 'line 12 twelve doce'),
        (20, RecordType.pattern, 'line 20 twenty veinte'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__before_context__pattern__after_context__discard_after(context):
    file_path = context['file_path']
    discard_before = None
    before_context = 1
    pattern = '1'
    except_pattern = None
    after_context = 1
    discard_after = '11'
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (1, RecordType.pattern, 'line 1 one uno'),
        (2, RecordType.after_context, 'line 2 two dos'),
        (9, RecordType.before_context, 'line 9 nine nueve'),
        (10, RecordType.pattern, 'line 10 ten diez'),
        (11, RecordType.pattern, 'line 11 eleven once'),
        (11, RecordType.discard_after, 'line 11 eleven once'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__discard_before__pattern__discard_after(context):
    file_path = context['file_path']
    discard_before = 'four'
    before_context = 0
    pattern = '17'
    except_pattern = None
    after_context = 0
    discard_after = '19'
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (14, RecordType.discard_before, 'line 14 fourteen catorce'),
        (17, RecordType.pattern, 'line 17 seventeen diecisiete'),
        (19, RecordType.discard_after, 'line 19 nineteen diecinueve')
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__discard_before__before_context__pattern__after_context(context):
    file_path = context['file_path']
    discard_before = '11'
    before_context = 1
    pattern = '2'
    except_pattern = None
    after_context = 1
    discard_after = None
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (11, RecordType.discard_before, 'line 11 eleven once'),
        (11, RecordType.before_context, 'line 11 eleven once'),
        (12, RecordType.pattern, 'line 12 twelve doce'),
        (13, RecordType.after_context, 'line 13 thirteen trece'),
        (19, RecordType.before_context, 'line 19 nineteen diecinueve'),
        (20, RecordType.pattern, 'line 20 twenty veinte'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__pattern__after_context__discard_before__same_as_pattern(context):
    file_path = context['file_path']
    discard_before = '11'
    before_context = 0
    pattern = '11'
    except_pattern = None
    after_context = 1
    discard_after = None
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (11, RecordType.discard_before, 'line 11 eleven once'),
        (11, RecordType.pattern, 'line 11 eleven once'),
        (12, RecordType.after_context, 'line 12 twelve doce'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__discard_before__matches_multiple_lines__expected_last_match(context):
    file_path = context['file_path']
    discard_before = 'four'
    before_context = 0
    pattern = None
    except_pattern = None
    after_context = 0
    discard_after = None
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (14, RecordType.discard_before, 'line 14 fourteen catorce'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__tar_gz__discard_before__matches_multiple_lines__expected_last_match(context):
    file_path2 = context['file_path2']
    file_path_tar_gz = context['file_path_tar_gz']
    discard_before = 'four'
    before_context = 0
    pattern = None
    except_pattern = None
    after_context = 0
    discard_after = None
    expected = [
        (0, RecordType.file_path, f"{file_path_tar_gz}#{file_path2.name}"),
        (20 + 14, RecordType.discard_before, 'file_path2 line 14 fourteen catorce'),
    ]
    actual = [e async for e in gen_matching_lines(file_path_tar_gz, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__not_foundable_discard_before__pattern(context):
    file_path = context['file_path']
    discard_before = 'this line does not exist in the file'
    before_context = 0
    pattern = 'four'
    except_pattern = None
    after_context = 0
    discard_after = None
    expected = [
        (0, RecordType.file_path, f"{file_path}"),
        (4, RecordType.pattern, 'line 4 four cuatro'),
        (14, RecordType.pattern, 'line 14 fourteen catorce'),
    ]
    actual = [e async for e in gen_matching_lines(file_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after)]
    assert actual == expected


async def test_gen_matching_lines__tar_gz__files_with_matches__multiple(context):
    file_path = context['file_path']
    file_path2 = context['file_path2']
    file_path_tar_gz = context['file_path_tar_gz']
    discard_before = None
    before_context = 0
    pattern = 'four'
    except_pattern = None
    after_context = 0
    discard_after = None
    files_with_matches = True
    expected = [
        (0, RecordType.file_path, f"{file_path_tar_gz}#{file_path.name}"),
        (0, RecordType.file_path, f"{file_path_tar_gz}#{file_path2.name}"),
    ]
    actual = [e async for e in gen_matching_lines(file_path_tar_gz, discard_before, before_context, pattern, except_pattern, after_context, discard_after, files_with_matches)]
    assert actual == expected


async def test_gen_matching_lines__tar_gz__files_with_matches__single(context):
    file_path2 = context['file_path2']
    file_path_tar_gz = context['file_path_tar_gz']
    discard_before = None
    before_context = 0
    pattern = 'file_path2'
    except_pattern = None
    after_context = 0
    discard_after = None
    files_with_matches = True
    expected = [
        (0, RecordType.file_path, f"{file_path_tar_gz}#{file_path2.name}"),
    ]
    actual = [e async for e in gen_matching_lines(file_path_tar_gz, discard_before, before_context, pattern, except_pattern, after_context, discard_after, files_with_matches)]
    assert actual == expected
