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


@dataclass
class Item:
    color: str
    size: str
    key: str


class RuleSetMatcher:

    def __init__(self, rule_set: Sequence[Rule]):
        self._rule_set = rule_set

    def matches(self, item: Item):
        return bool(self._get_count_of_matching_rules(item))

    def _get_count_of_matching_rules(self, item: Item) -> int:
        return len(self._get_rules_matching_also_type(item))

    def _get_rules_matching_also_type(self, item: Item):
        return [r for r in self._get_rules_matching_combo_color_size_with_highest_priority(item) if r.key == item.key]

    def _get_rules_matching_combo_color_size_with_highest_priority(self, item: Item):
        matching_rules = self._get_rules_matching_combo_color_size(item)
        if matching_rules:
            max_priority = max(c.prio for c in matching_rules)
            return [r for r in matching_rules if r.prio == max_priority]
        return []

    def _get_rules_matching_combo_color_size(self, item: Item):
        matching_rules = []
        for rule in self._rule_set:
            if (item.key in rule.combo
                    and (rule.color == item.color or rule.color == Rule.ALL)
                    and (rule.size == item.size or rule.size == Rule.ALL)
            ):
                matching_rules.append(rule)
        return matching_rules


class RulesParser:

    @staticmethod
    def from_csv(text: str, delimiter: str = ','):
        reader = csv.reader((ln for ln in text.splitlines() if ln and not ln.startswith('#')), delimiter=delimiter)
        rules = []
        for row in reader:
            combo, priority, color, size, key = row
            rules.append(Rule(combo=combo.split('/'), prio=int(priority), color=color, size=size, key=key))
        return rules
