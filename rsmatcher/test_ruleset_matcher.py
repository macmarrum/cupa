#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
from textwrap import dedent

import pytest

from rsmatcher.ruleset_matcher import RulesParser, Item, RuleSetMatcher


@pytest.fixture
def rule_set():
    tsv = dedent('''\
#combo	prio	color	size	key
m2/p9	20	red	2	p9
m2/p9	20	blue	2	p9
m2/p9	10	ALL	ALL	m2
m2/p9	10	ALL	25	m2
m1/p8	10	ALL	ALL	m1
m10	10	ALL	ALL	m10
''')
    yield RulesParser.from_csv(tsv, delimiter='\t')


def _calc_actual(expected, rule_set):
    actual = {}
    for line in expected:
        item = Item(*line.split())
        actual[line] = 1 if RuleSetMatcher(rule_set).matches(item) else 0
    return actual


def test_matches__red_2(rule_set):
    expected = {
        'red	2	p9': 1,
        'red	2	m2': 0,
    }
    assert _calc_actual(expected, rule_set) == expected


def test_matches__red_3(rule_set):
    expected = {
        'red	31	p9': 0,
        'red	31	m2': 1,
    }
    assert _calc_actual(expected, rule_set) == expected
