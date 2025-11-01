#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
from collections.abc import Sequence
from dataclasses import dataclass, fields as dc_fields
from typing import ClassVar


@dataclass
class Rule:
    prio: int
    combo: list[str]
    color: str
    size: str
    key: str
    ALL: ClassVar[str] = 'ALL'
    COMBO_SEP: ClassVar[str] = '/'
    _init_fields: ClassVar[list] = None

    @classmethod
    def from_csv(cls, line: str, delimiter: str = ',', fields: list[str] = None):
        if fields:
            if not cls._init_fields:
                cls._init_fields = [f.name for f in dc_fields(cls) if f.init]
            if fields != cls._init_fields:
                raise ValueError(f"fields does not match Rule fields: {fields} != {cls._init_fields}")
            kwargs = dict(zip(fields, line.split(delimiter)))
            kwargs['prio'] = int(kwargs['prio'])
            kwargs['combo'] = kwargs['combo'].split(Rule.COMBO_SEP)
            return Rule(**kwargs)
        else:
            prio, combo, color, size, key = line.split(delimiter)
            return Rule(int(prio), combo.split(Rule.COMBO_SEP), color, size, key)


@dataclass
class Item:
    color: str
    size: str
    key: str


class RuleSetMatcher:

    def __init__(self, rule_seq: Sequence[Rule]):
        self._rule_seq = rule_seq

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
        for rule in self._rule_seq:
            if (item.key in rule.combo
                    and (item.color == rule.color or rule.color == Rule.ALL)
                    and (item.size == rule.size or rule.size == Rule.ALL)
            ):
                matching_rules.append(rule)
                max_prio = max(max_prio, rule.prio) if max_prio else rule.prio
        return matching_rules, max_prio

    @staticmethod
    def from_csv(text: str, delimiter: str = ',', fields=None, fields_from_header=False, comment_indicator='#'):
        lines_gen = (ln for ln in text.splitlines() if ln)
        if fields_from_header:
            header = next(lines_gen).lstrip(comment_indicator)
            fields = header.split(delimiter)
        rules = []
        for line in lines_gen:
            if line.startswith(comment_indicator):
                continue
            rules.append(Rule.from_csv(line, delimiter, fields))
        return RuleSetMatcher(rules)
