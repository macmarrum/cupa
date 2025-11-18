# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import re
import sys
from os import PathLike
from typing import TextIO


class Tee:
    _RX_ANSI_SGR_SEQ = re.compile(r'\x1b\[(\d+)(;\d+)*m')

    def __init__(self, file: PathLike | str = None, out: TextIO = None, mode: str = 'w', encoding: str = 'utf-8', errors: str = 'strict', strip_ansi: bool = True):
        self._file = file
        self._out = out or sys.stdout
        self._mode = mode
        self._encoding = encoding
        self._errors = errors
        self._strip_ansi = strip_ansi
        self._fileobj = None

    def open(self):
        if self._file:
            self._fileobj = open(self._file, self._mode, encoding=self._encoding, errors=self._errors)

    def close(self):
        if self._fileobj:
            self._fileobj.close()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def write(self, s: str):
        if self._fileobj:
            self._fileobj.write(self._RX_ANSI_SGR_SEQ.sub('', s) if self._strip_ansi else s)
        self._out.write(s)

    def flush(self):
        if self._fileobj:
            self._fileobj.flush()
        self._out.flush()
