import io
import sqlite3
from time import strftime

from logger.sqlite_logger import Logger


def test_sqlite_logger__no_flatten__print_create__datefmt():
    string_io = io.StringIO()
    datefmt = '%Y-%m-%d'
    log = Logger(':memory:', file=string_io, flatten_file_msg=False, print_create=True, datefmt=datefmt)
    message = 'test message'
    log.info(message)
    localtime = strftime(datefmt)
    assert string_io.getvalue() == f"{Logger._CREATE_STMT}\n{localtime} INFO {message}\n"
    assert log._db.execute('SELECT * FROM log').fetchall() == [(1, localtime, 'INFO', message)]


def test_sqlite_logger__migrate_column_level_to_level_name_if_required():
    db = sqlite3.connect(':memory:')
    db.execute(Logger._CREATE_STMT.replace('level TEXT', 'level INTEGER'))
    db.execute('''INSERT INTO log (localtime, level, message) VALUES 
        ('2025-11-10', 10, 'test\nmessage 1'),
        ('2025-11-10', 20, 'test\r\nmessage 2')
    ''')
    log = Logger(db)
    assert log._db.execute('SELECT * FROM log').fetchall() == [
        (1, '2025-11-10', 'DEBUG', 'test\nmessage 1'),
        (2, '2025-11-10', 'INFO', 'test\r\nmessage 2'),
    ]
