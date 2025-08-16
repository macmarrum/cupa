#!/usr/bin/python3
# Copyright (C) 2018-2020, 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
from dataclasses import dataclass
from enum import Enum


class MergeStatus(Enum):
    """Alternative: collections.namedtuple('MergeStatus', ['TOPLEFT', 'BELOW', 'TORIGHT', 'OUTSIDE'])(1, 2, 3, 0)"""
    TOPLEFT = 1
    BELOW = 2
    TORIGHT = 3
    OUTSIDE = 0


@dataclass
class Bounds:
    """Alternative: collections.namedtuple('Bounds', ['beg', 'end'])"""
    beg: int
    end: int


@dataclass
class Coord:
    """Alternative: collections.namedtuple('Coord', ['col', 'row'])"""
    col: int
    row: int


class Flavor(Enum):
    TW5 = 'TW5'
    JIRA = 'JIRA'


MERGED = {MergeStatus.BELOW: '~', MergeStatus.TORIGHT: '<'}
HORIZONTAL = {"left": ['{} '], "center": [' {} '], "right": [' {}'], "_default_": ['{}']}
VERTICAL = {"top": ['^'], "bottom": [','], "_default_": []}
VERTICAL_TR_MAP = {'^': '&#94;', ',': '&#44;'}


class XlsToTableConverter:
    def __init__(self, flavor: Flavor = None):
        self._range_bounds_of_merged_cells = None
        self._worksheet = None
        self._flavor = flavor or Flavor.TW5

    @property
    def is_tw5(self):
        return self._flavor == Flavor.TW5

    @property
    def is_jira(self):
        return self._flavor == Flavor.JIRA

    @property
    def range_bounds_of_merged_cells(self):
        if self._range_bounds_of_merged_cells is None:
            self._range_bounds_of_merged_cells = []
            for rng in self._worksheet.merged_cells.ranges:
                beg = Coord(rng.bounds[0], rng.bounds[1])
                end = Coord(rng.bounds[2], rng.bounds[3])
                self._range_bounds_of_merged_cells.append(Bounds(beg, end))
        return self._range_bounds_of_merged_cells

    def _get_tw5formated_value(self, cell):
        status = self._get_cell_merged_status(cell)
        text = MERGED.get(status)
        if text is not None:
            return text
        else:
            template = VERTICAL.get(cell.alignment.vertical, VERTICAL["_default_"]).copy()
            template += HORIZONTAL.get(cell.alignment.horizontal, HORIZONTAL["_default_"])
            template_str = ''.join(template)
            value = self._get_value(cell, self._replace_tw5_merge_steering_chars)
            value_br = value.replace('\n', '<br/>')
            text = template_str.format(value_br)
            return text

    @classmethod
    def _get_value(cls, cell, format_func=None):
        if cell.value is None:
            val = ''
        else:
            val = str(cell.value).strip(' ')
            if val != '' and format_func is not None:
                val = cls._replace_tw5_merge_steering_chars(val)
        return val

    @staticmethod
    def _replace_tw5_merge_steering_chars(value):
        for (tr_from, tr_to) in VERTICAL_TR_MAP.items():
            if value[0] == tr_from:
                return tr_to + value[1:]
        return value

    def _get_cell_merged_status(self, cell):
        c = Coord(cell.column, cell.row)
        for b in self.range_bounds_of_merged_cells:
            if c.col == b.beg.col and c.row == b.beg.row:
                return MergeStatus.TOPLEFT
            if b.beg.col <= c.col <= b.end.col and b.beg.row <= c.row <= b.end.row:
                if c.row > b.beg.row:
                    return MergeStatus.BELOW
                if c.col > b.beg.col:
                    return MergeStatus.TORIGHT
        return MergeStatus.OUTSIDE

    def convert(self, worksheet):
        self._range_bounds_of_merged_cells = None
        self._worksheet = worksheet
        tb = []
        for row_idx, row in enumerate(self._worksheet):
            ln = ['']
            for idx, cell in enumerate(row):
                if self.is_tw5:
                    val = self._get_tw5formated_value(cell)
                    # TW5 table mustn't have repeating ~ next to each other under < (merging)
                    if not ((tb[-1][idx] == '<' if len(tb) > 0 else False) and ln[-1] == '~' and val == '~'):
                        ln.append(val)
                elif self.is_jira:
                    val = self._get_value(cell)  # no formatting or merging for JIRA
                    ln.append(val)
            ln.append('')  # so that there's a bar added at the end by .join(ln)
            if row_idx == 0 and self.is_jira:
                bar = '||'
            else:
                bar = '|'
            tb.append(bar.join(ln))
        if self.is_tw5:
            tb[0] += 'h'
        return '\r\n'.join(tb)
