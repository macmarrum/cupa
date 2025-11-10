# python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import re
import sqlite3
import sys
from datetime import datetime
from os import PathLike
from textwrap import dedent


class Logger:
    _CREATE_STMT = dedent('''\
            CREATE TABLE IF NOT EXISTS log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            localtime TEXT NOT NULL DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
            level TEXT NOT NULL,
            message TEXT NOT NULL
            ) STRICT''')

    def __init__(self, database: PathLike | str | sqlite3.Connection, *, file=None, flatten_file_msg=True, datefmt: str = None, print_create=False):
        self._db = database if isinstance(database, sqlite3.Connection) else sqlite3.connect(database)
        self._file = file or sys.stderr
        self._datefmt = datefmt or '%Y-%m-%d %H:%M:%S.%f'
        self._flatten_file_msg = flatten_file_msg
        self._RX_FLAT = re.compile(r'\n\s*')
        self._migrate_column_level_to_level_name_if_required()
        self._create_table_log_if_not_exists(print_create=print_create)
        self.warn = self.warning

    def _create_table_log_if_not_exists(self, print_create=False):
        print_create and print(self._RX_FLAT.sub(' ', self._CREATE_STMT) if self._flatten_file_msg else self._CREATE_STMT, file=self._file)
        self._db.execute(self._CREATE_STMT)

    def _migrate_column_level_to_level_name_if_required(self):
        for _ in self._db.execute("SELECT 1 FROM pragma_table_info('log') WHERE name = 'level'"):
            self._db.execute('ALTER TABLE log RENAME TO log_old')
            self._create_table_log_if_not_exists()
            cur = self._db.cursor()
            for row in self._db.execute('SELECT localtime, level, message FROM log_old ORDER BY id'):
                cur.execute('INSERT INTO log (localtime, level, message) VALUES (?, ?, ?)', (row[0], logging.getLevelName(row[1]), row[2]))
            cur.close()
            self._db.execute('DROP TABLE log_old')
            self._db.commit()

    def log(self, level: int, message: str):
        level_name = logging.getLevelName(level)
        localtime = datetime.now().strftime(self._datefmt)
        file_message = self._RX_FLAT.sub(' ', message) if self._flatten_file_msg else message
        print(f"{localtime} {level_name} {file_message}", file=self._file)
        params = (localtime, level_name, message)
        self._db.execute('INSERT INTO log (localtime, level, message) VALUES (?, ?, ?)', params)
        self._db.commit()

    def debug(self, message):
        self.log(logging.DEBUG, message)

    def info(self, message):
        self.log(logging.INFO, message)

    def warning(self, message):
        self.log(logging.WARNING, message)

    def error(self, message):
        self.log(logging.ERROR, message)

    def critical(self, message):
        self.log(logging.CRITICAL, message)
