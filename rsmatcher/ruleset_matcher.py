#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import csv
from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar


@dataclass
class Rule:
    combo: list[str]
    prio: int
    color: str
    size: str
    key: str
    ALL: ClassVar[str] = 'ALL'
    COMBO_SEP: ClassVar[str] = '/'


@dataclass
class Item:
    color: str
    size: str
    key: str


class RuleSetMatcher:

    def __init__(self, rule_set: Sequence[Rule]):
        self._rule_set = rule_set

    def matches(self, item: Item):
        for _ in self._gen_rules_matching_combo_color_size_with_highest_prio_and_matching_key(item):
            return True
        return False

    def _gen_rules_matching_combo_color_size_with_highest_prio_and_matching_key(self, item: Item):
        for rule in self._gen_rules_matching_combo_color_size_with_highest_prio(item):
            if rule.key == item.key:
                yield rule

    def _gen_rules_matching_combo_color_size_with_highest_prio(self, item: Item):
        matching_rules, max_prio = self._get_rules_matching_combo_color_size__max_prio(item)
        for rule in matching_rules:
            if rule.prio == max_prio:
                yield rule

    def _get_rules_matching_combo_color_size__max_prio(self, item: Item):
        matching_rules = []
        max_prio = None
        for rule in self._rule_set:
            if (item.key in rule.combo
                    and (rule.color == item.color or rule.color == Rule.ALL)
                    and (rule.size == item.size or rule.size == Rule.ALL)
            ):
                matching_rules.append(rule)
                max_prio = max(max_prio, rule.prio) if max_prio else rule.prio
        return matching_rules, max_prio


class RulesParser:

    @staticmethod
    def from_csv(text: str, delimiter: str = ','):
        reader = csv.reader((ln for ln in text.splitlines() if ln and not ln.startswith('#')), delimiter=delimiter)
        rules = []
        for row in reader:
            combo, priority, color, size, key = row
            rules.append(Rule(combo=combo.split(Rule.COMBO_SEP), prio=int(priority), color=color, size=size, key=key))
        return rules
