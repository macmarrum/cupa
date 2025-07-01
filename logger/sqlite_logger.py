# python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import sqlite3
from collections import defaultdict
from datetime import datetime
from os import PathLike
from pathlib import Path
from textwrap import dedent

LEVEL_TO_NAME = defaultdict(str)
LEVEL_TO_NAME |= {
    logging.DEBUG: 'DEBUG',
    logging.INFO: 'INFO',
    logging.WARN: 'WARN',
    logging.ERROR: 'ERROR',
    logging.CRITICAL: 'CRITICAL',
}


class Logger:
    def __init__(self, sqlite_path: Path | PathLike | str):
        self._db = sqlite3.connect(sqlite_path)
        self.warning = self.warn

    def create_table_log(self):
        self._db.execute(dedent('''\
            CREATE TABLE IF NOT EXISTS log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            localtime TEXT NOT NULL DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
            level INTEGER NOT NULL,
            message TEXT NOT NULL
            )'''))

    def log(self, level: int, message: str):
        level_name = LEVEL_TO_NAME[level]
        localtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        print(f"{localtime} {level_name} {message}")
        params = (localtime, level, message)
        self._db.execute(f"INSERT INTO log (localtime, level, message) VALUES (?, ?, ?)", params)
        self._db.commit()

    def debug(self, message):
        self.log(logging.DEBUG, message)

    def info(self, message):
        self.log(logging.INFO, message)

    def warn(self, message):
        self.log(logging.WARN, message)

    def error(self, message):
        self.log(logging.ERROR, message)

    def critical(self, message):
        self.log(logging.CRITICAL, message)
