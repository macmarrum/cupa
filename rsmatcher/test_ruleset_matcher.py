#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
from textwrap import dedent

import pytest

from rsmatcher.ruleset_matcher import Item, RuleSetMatcher


@pytest.fixture
def ruleset_matcher():
    csv_config = dedent('''\
#prio | combo | color | size | key
20 | m2/p9 | red | 2 | p9
20 | m2/p9 | blue | 2 | p9
10 | m2/p9 | ALL | ALL | m2
10 | m2/p9 | ALL | 25 | m2
10 | m1/p8 | ALL | ALL | m1
10 | m10 | ALL | ALL | m10
''')
    yield RuleSetMatcher.from_csv(csv_config, delimiter=' | ')


def _calc_actual(expected, ruleset_matcher):
    actual = {}
    for line in expected:
        item = Item(*line.split(' | '))
        actual[line] = 1 if ruleset_matcher.matches(item) else 0
    return actual


def test_matches__red_2(ruleset_matcher):
    expected = {
        'red | 2 | p9': 1,
        'red | 2 | m2': 0,
    }
    assert _calc_actual(expected, ruleset_matcher) == expected


def test_matches__red_3(ruleset_matcher):
    expected = {
        'red | 31 | p9': 0,
        'red | 31 | m2': 1,
    }
    assert _calc_actual(expected, ruleset_matcher) == expected
