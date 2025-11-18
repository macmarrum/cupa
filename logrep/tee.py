# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import re
import sys
from os import PathLike
from typing import TextIO


class Tee:
    RX_ANSI_CODE = re.compile(r'\x1b\[(\d+)(;\d+)*m')

    def __init__(self, file: PathLike | str, out: TextIO = None, mode: str = 'a', encoding: str = 'utf-8', errors: str = 'strict'):
        self._file = file
        self._out = out or sys.stdout
        self._mode = mode
        self._encoding = encoding
        self._errors = errors
        self._fileobj = None

    def open(self):
        self._fileobj = open(self._file, self._mode, encoding=self._encoding, errors=self._errors)

    def close(self):
        self._fileobj.close()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def write(self, s: str):
        self._fileobj.write(self.RX_ANSI_CODE.sub('', s))
        self._out.write(s)

    def flush(self):
        self._fileobj.flush()
        self._out.flush()
