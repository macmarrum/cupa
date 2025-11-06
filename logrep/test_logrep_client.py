import re

from logrep.logrep_client import gen_segments_with_is_match


def test_gen_segments_with_is_match__multiple_matching_segments():
    line = '2025-11-06 15:52 INFO gen_segments_with_is_match test 123 lorem ipsum'
    rx = re.compile(r'(2025)|(INFO)|(123)')
    expected = [
        (True, '2025'),
        (False, '-11-06 15:52 '),
        (True, 'INFO'),
        (False, ' gen_segments_with_is_match test '),
        (True, '123'),
        (False, ' lorem ipsum'),
    ]
    actual = list(gen_segments_with_is_match(line, rx))
    assert actual == expected


def test_gen_segments_with_is_match__no_matching_segments():
    line = '2025-11-06 15:52 INFO gen_segments_with_is_match test 123 lorem ipsum'
    rx = re.compile('no such text')
    expected = [
        (False, '2025-11-06 15:52 INFO gen_segments_with_is_match test 123 lorem ipsum'),
    ]
    actual = list(gen_segments_with_is_match(line, rx))
    assert actual == expected


def test_gen_segments_with_is_match__ungrouped_match():
    line = '2025-11-06 15:52 INFO gen_segments_with_is_match test 123 lorem ipsum'
    rx = re.compile('INFO')
    expected = [
        (False, '2025-11-06 15:52 '),
        (True, 'INFO'),
        (False, ' gen_segments_with_is_match test 123 lorem ipsum'),
    ]
    actual = list(gen_segments_with_is_match(line, rx))
    assert actual == expected
