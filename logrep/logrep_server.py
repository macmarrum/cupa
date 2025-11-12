#!/usr/bin/env python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import asyncio
import bz2
import collections
import contextlib
import getpass
import gzip
import io
import json
import logging.handlers
import lzma
import os
import queue
import re
import socket
import sys
import threading
import tomllib
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, tzinfo, timezone, timedelta
from ipaddress import IPv4Address
from pathlib import Path
from string import Template
from typing import ClassVar, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from zstd_asgi import ZstdMiddleware

if sys.version_info >= (3, 14):
    from compression import zstd
    import tarfile
    import zipfile
else:
    from backports import zstd
    from backports.zstd import tarfile
    from backports.zstd import zipfile

me = Path(__file__)
UTF8 = 'UTF-8'
APPLICATION_X_NDJSON = 'application/x-ndjson'
SEARCH = 'search'
config_path = me.with_suffix('.toml')

formatter = logging.Formatter('{asctime} {levelname} {name} [{funcName}] {message}', style='{')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
log_queue = queue.SimpleQueue()
queue_handler = logging.handlers.QueueHandler(log_queue)
queue_listener = logging.handlers.QueueListener(log_queue, console_handler, respect_handler_level=False)

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(me.stem)
log.addHandler(queue_handler)
log.setLevel(logging.DEBUG)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """https://github.com/fastapi/fastapi/blob/master/docs/en/docs/advanced/events.md"""
    log.info("Starting log_queue listener")
    queue_listener.start()
    yield
    log.info("Stopping log_queue listener")
    queue_listener.stop()


MINIMUM_SIZE = 1000
app = FastAPI(lifespan=lifespan)
app.add_middleware(ZstdMiddleware, minimum_size=MINIMUM_SIZE)


@dataclass
class Settings:
    profile: str
    file_path: str = '/__not_set__'
    discard_before: str | None = None
    before_context: int = 0
    pattern: str = ''
    except_pattern: str = ''
    after_context: int = 0
    discard_after: str | None = None
    host: str = '0.0.0.0'
    port: int = 8000
    uuid: str = str(uuid.uuid4())
    ssl_keyfile: str | None = None
    ssl_keyfile_password: str | None = None
    ssl_certificate: str | None = None
    # timezone can be any of zoneinfo.available_timezones()
    # or an offset from UTC, e.g. -03:30, UTC-03:30, +02:00, UTC+02:00
    timezone: str | None = None
    ASK: ClassVar[str] = 'ASK'  # ask for ssl_keyfile_password


@dataclass
class SearchArgs:
    settings: Settings
    discard_before: str | re.Pattern | None
    before_context: int
    pattern: str | re.Pattern | None
    except_pattern: str | re.Pattern | None
    after_context: int
    discard_after: str | re.Pattern | None
    files_with_matches: bool = False

    @classmethod
    def from_settings_and_args_with_validation(cls, settings: Settings, discard_before: str | None, before_context: int | None, pattern: str | None, except_pattern: str | None, after_context: int | None, discard_after: str | None, files_with_matches: bool):
        """For creation of SearchArgs with validation before streaming starts"""
        _before_context = before_context if before_context is not None else settings.before_context
        if _before_context < 0:
            raise HTTPException(status_code=400, detail='before_context must be non-negative')
        _after_context = after_context if after_context is not None else settings.after_context
        if _after_context < 0:
            raise HTTPException(status_code=400, detail='after_context must be non-negative')
        _discard_before = discard_before or settings.discard_before
        if _discard_before and is_probably_complex_pattern(_discard_before):
            try:
                _discard_before = re.compile(_discard_before)
            except re.error as e:
                raise HTTPException(status_code=400, detail=f"discard_before pattern {e}: {_discard_before!r}")
        _pattern = pattern or settings.pattern
        if _pattern and is_probably_complex_pattern(_pattern):
            try:
                _pattern = re.compile(_pattern)
            except re.error as e:
                raise HTTPException(status_code=400, detail=f"pattern {e}: {_pattern!r}")
        _except_pattern = except_pattern or settings.except_pattern
        if _except_pattern and is_probably_complex_pattern(_except_pattern):
            try:
                _except_pattern = re.compile(_except_pattern)
            except re.error as e:
                raise HTTPException(status_code=400, detail=f"except_pattern {e}: {_except_pattern!r}")
        _discard_after = discard_after or settings.discard_after
        if _discard_after and is_probably_complex_pattern(_discard_after):
            try:
                _discard_after = re.compile(_discard_after)
            except re.error as e:
                raise HTTPException(status_code=400, detail=f"discard_after pattern {e}: {_discard_after!r}")
        if not _discard_before and not _pattern and not _discard_after:
            raise HTTPException(status_code=400, detail='discard_before or pattern or discard_after must be specified')
        return cls(
            settings=settings,
            discard_before=_discard_before,
            before_context=_before_context,
            pattern=_pattern,
            except_pattern=_except_pattern,
            after_context=_after_context,
            discard_after=_discard_after,
            files_with_matches=files_with_matches,
        )


TOP_LEVEL = '#top-level'
ProfileToSettings = dict[str, Settings]


def make_profile_to_settings_from_toml_path(toml_file: Path) -> ProfileToSettings:
    toml_str = toml_file.read_text(encoding=UTF8)
    return make_profile_to_settings_from_toml_text(toml_str)


def make_profile_to_settings_from_toml_text(toml_str) -> ProfileToSettings:
    profile_to_settings: ProfileToSettings = {}
    toml_dict = tomllib.loads(toml_str)
    common_kwargs_for_settings = {}
    profile_to_dict = {TOP_LEVEL: {}}
    for key, value in toml_dict.items():
        if isinstance(value, dict):  # gather profiles, i.e. "name": {dict, aka hash table}
            if not key.startswith('#'):  # skip profiles starting with hash (#)
                profile_to_dict[key] = value
        else:  # gather top-level settings (common for each profile)
            key = key.replace('-', '_')
            common_kwargs_for_settings[key] = value
    for profile, dct in profile_to_dict.items():
        kwargs_for_settings = common_kwargs_for_settings.copy()
        kwargs_for_settings['profile'] = profile
        for key, value in dct.items():
            kwargs_for_settings[key] = value
        profile_to_settings[profile] = Settings(**kwargs_for_settings)
    return profile_to_settings


class ConfigLoader:
    def __init__(self, config_path: Path):
        self._config_path = config_path
        self._config_cache = None
        self._config_mtime = None
        self._config_size = None

    @property
    def fresh_profile_to_settings(self):
        return self._get_fresh_profile_to_settings()

    def _get_fresh_profile_to_settings(self):
        try:
            sr = self._config_path.stat()
        except OSError:
            raise HTTPException(status_code=500, detail='Error accessing config file')
        if self._config_cache is None or self._config_mtime != sr.st_mtime or self._config_size != sr.st_size:
            self._config_cache = make_profile_to_settings_from_toml_path(self._config_path)
            self._config_mtime = sr.st_mtime
            self._config_size = sr.st_size
        return self._config_cache

    async def get_fresh_profile_to_settings(self):
        return await asyncio.to_thread(self._get_fresh_profile_to_settings)


config_loader = ConfigLoader(config_path)
top_level_settings = config_loader.fresh_profile_to_settings[TOP_LEVEL]


async def get_settings(profile: str):
    profile_to_settings = await config_loader.get_fresh_profile_to_settings()
    if profile:
        settings = profile_to_settings.get(profile)
        if not settings:
            raise HTTPException(status_code=404, detail=f"profile not found: {profile!r}")
    else:
        settings = profile_to_settings[TOP_LEVEL]
    return settings


class SearchRequest(BaseModel):
    profile: str | None = None
    discard_before: str | None = None
    before_context: str | None = None
    pattern: str | None = None
    except_pattern: str | None = None
    after_context: int | None = None
    discard_after: str | None = None
    files_with_matches: bool | None = None


@app.get(f"/{top_level_settings.uuid}/{SEARCH}")
async def search_logs_get(profile: str | None = None, discard_before: str | None = None, before_context: int | None = None, pattern: str | None = None, except_pattern: str | None = None, after_context: int | None = None, discard_after: str | None = None, files_with_matches: bool | None = None):
    try:
        settings = await get_settings(profile)
        search_args = SearchArgs.from_settings_and_args_with_validation(settings, discard_before, before_context, pattern, except_pattern, after_context, discard_after, files_with_matches)
        return StreamingResponse(search_logs(search_args), media_type=APPLICATION_X_NDJSON)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Search error: {str(e)}")
        log.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.post(f"/{top_level_settings.uuid}/{SEARCH}")
async def search_logs_post(sr: SearchRequest):
    try:
        settings = await get_settings(sr.profile)
        search_args = SearchArgs.from_settings_and_args_with_validation(settings, sr.discard_before, sr.before_context, sr.pattern, sr.except_pattern, sr.after_context, sr.discard_after, sr.files_with_matches)
        return StreamingResponse(search_logs(search_args), media_type=APPLICATION_X_NDJSON)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Search error: {str(e)}")
        log.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


class StrftimeTemplate(Template):
    """
    Substitutes the current time for the format string specified within angle brackets,
    with an optional timedelta specification (weeks, days, hours, minutes, seconds),
    e.g., `Yesterday was <%m/%d %Y|days=-1>` becomes `Yesterday was 10/16 2025`.
    The optional mapping passed to `substitute` can specify a tzinfo instance,
    e.g., `substitute({'timezone': ZoneInfo('Europe/Warsaw')})`
    """
    flags = re.VERBOSE  # to override the default re.IGNORECASE
    delimiter = '<'
    idpattern = '[^>]+>'

    def substitute(self, mapping=None, **kwargs):
        class StrftimeResolver:
            SEP = '|'

            def __getitem__(self, key):
                _timezone = mapping.get('timezone') if mapping else None
                _tzinfo = self.parse_timezone(_timezone)
                _now = ((mapping.get('now') if mapping else None) or datetime.now()).astimezone(_tzinfo)
                spec = key[:-1]
                weeks, days, hours, minutes, seconds = 0, 0, 0, 0, 0
                if self.SEP in spec:
                    spec, td_spec = spec.split(self.SEP)
                    for elem in td_spec.split(','):
                        match elem.split('='):
                            case ['weeks', weeks]:
                                weeks = int(weeks)
                            case ['days', days]:
                                days = int(days)
                            case ['hours', hours]:
                                hours = int(hours)
                            case ['minutes', minutes]:
                                minutes = int(minutes)
                            case ['seconds', seconds]:
                                seconds = int(seconds)
                            case _:
                                raise ValueError(f"unexpected timedelta spec: {td_spec}")
                td = timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
                return (_now + td).strftime(spec)

            @staticmethod
            def parse_timezone(tz: str | None) -> tzinfo | None:
                if not tz:
                    return None
                if ':' in tz:
                    hours, minutes = tz.removeprefix('UTC').split(':')
                    try:
                        hours = int(hours)
                        minutes = int(minutes) * (-1 if hours < 0 else 1)
                        offset = timedelta(hours=hours, minutes=minutes)
                        _tzinfo = timezone(offset, name=tz)
                    except ValueError as e:
                        log.warning(f"parse_timezone {e}: {tz}")
                        _tzinfo = None
                else:
                    try:
                        _tzinfo = ZoneInfo(tz)
                    except ZoneInfoNotFoundError as e:
                        log.warning(f"parse_timezone {e}: {tz}")
                        _tzinfo = None
                return _tzinfo

        return super().substitute(StrftimeResolver())


# Note: special chars could be either escaped or bracketed [] to make them literal
# Bracketing is not accounted for here, hence "probably"
RX_PROBABLY_COMPLEX_PATTERN = re.compile(r'(?<!\\)[()\[{.*+?^$|]|\\[AbdDsSwWzZ]')
RX_ESCAPE_FOLLOWED_BY_SPECIAL = re.compile(r'\\(?=[()\[{.*+?^$|])')


def is_probably_complex_pattern(pattern: str):
    return RX_PROBABLY_COMPLEX_PATTERN.search(pattern) is not None


JSON_SEPARATORS = (',', ':')


async def search_logs(search_args: SearchArgs):
    """Common search logic for both GET and POST endpoints; streams matching lines as NDJSON"""
    log.info(f"{search_args=}")
    file_path = Path(StrftimeTemplate(search_args.settings.file_path).substitute({'timezone': search_args.settings.timezone})).absolute()
    list_of_lists = []
    total_line_size_in_list_of_lists = 0
    minimum_size_batch_count = 0
    async for item in gen_matching_lines(file_path, search_args):
        list_of_lists.append(item)
        total_line_size_in_list_of_lists += len(item[2])
        if total_line_size_in_list_of_lists >= MINIMUM_SIZE:
            yield json.dumps(list_of_lists, separators=JSON_SEPARATORS) + '\n'
            list_of_lists.clear()
            total_line_size_in_list_of_lists = 0
            minimum_size_batch_count += 1
    if list_of_lists:
        yield json.dumps(list_of_lists, separators=JSON_SEPARATORS) + '\n'
    log.debug(f"[/] minimum_size_batch_count: {minimum_size_batch_count}")


class FileReader:
    """Reads text files. Handles the standard compression and archive formats. In the case of an archive, fires on_file_open for each member but concats each member's content for all other operations"""

    def __init__(self, file_path: Path, encoding: str = UTF8, errors: str = 'strict', on_file_open: Callable[['FileReader'], None] = lambda _: None):
        self._file_path = file_path
        self._encoding = encoding
        self._errors = errors
        self._on_file_open = on_file_open
        self._file = None
        self._file_iterator = None
        self._outer_file = None

    def __enter__(self):
        name = self._file_path.name
        if name.endswith('.tar.gz') or name.endswith('.tgz'):
            self._open_tar('r:gz')
        elif name.endswith('.tar.bz2') or name.endswith('.tbz'):
            self._open_tar('r:bz2')
        elif name.endswith('.tar.xz') or name.endswith('.txz'):
            self._open_tar('r:xz')
        elif name.endswith('.tar.zst') or name.endswith('.tzst'):
            self._open_tar('r:zstd')
        elif name.endswith('.tar'):
            self._open_tar('r:')
        elif name.endswith('.zip'):
            self._open_zip()
        elif name.endswith('.gz'):
            self._open_compressed(gzip)
        elif name.endswith('.bz2'):
            self._open_compressed(bz2)
        elif name.endswith('.xz'):
            self._open_compressed(lzma)
        elif name.endswith('.zst'):
            self._open_compressed(zstd)
        else:
            self._open_file()
        return self

    def _open_tar(self, mode: str):
        self._outer_file = tarfile.open(self._file_path, mode, encoding=self._encoding, errors=self._errors)

        def _iter_file():
            for tarinfo in self._outer_file.getmembers():
                if not tarinfo.isfile():
                    continue
                if self._file:
                    self._file.close()
                if binary_file := self._outer_file.extractfile(tarinfo):
                    self._file = io.TextIOWrapper(binary_file, encoding=self._encoding, errors=self._errors)
                    self._on_file_open(self)
                    yield from self._file

        self._file_iterator = _iter_file()

    def _open_zip(self):
        self._outer_file = zipfile.ZipFile(self._file_path, 'r')

        def _iter_file():
            for zipinfo in self._outer_file.infolist():
                if zipinfo.is_dir():
                    continue
                if self._file:
                    self._file.close()
                if binary_file := self._outer_file.open(zipinfo):
                    self._file = io.TextIOWrapper(binary_file, encoding=self._encoding, errors=self._errors)
                    self._on_file_open(self)
                    yield from self._file

        self._file_iterator = _iter_file()

    def _open_compressed(self, compressor):
        self._file_iterator = self._file = compressor.open(self._file_path, 'rt', encoding=self._encoding, errors=self._errors)
        self._on_file_open(self)

    def _open_file(self):
        self._file_iterator = self._file = open(self._file_path, 'rt', encoding=self._encoding, errors=self._errors)
        self._on_file_open(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        with contextlib.suppress(Exception):
            if self._file:
                self._file.close()
        with contextlib.suppress(Exception):
            if self._outer_file:
                self._outer_file.close()
        return False  # don't suppress exceptions cauth within with

    def __iter__(self):
        return self._file_iterator

    def inner_seek(self, offset, whence=os.SEEK_SET, /):
        """Forwards to ``_file.seek``"""
        self._file.seek(offset, whence)

    def rewind(self):
        """Rewinds to the beginning: reopens the file for ZipFile or TarFile; calls ``_file.seek(0)`` for non-archive files"""
        if self._outer_file:
            self.__exit__(None, None, None)
            self.__enter__()
        else:
            self._file.seek(0)

    @property
    def name(self):
        if self._outer_file:
            return f"{self._file_path}#{self._file.name}"
        else:
            return f"{self._file_path}"


class RecordType:
    file_path = 'l'
    discard_before = 'D'
    before_context = 'B'
    pattern = 'p'
    after_context = 'A'
    discard_after = 'd'


RX_DISCARD_BEFORE_LINE_NUM = re.compile(r'^discard_before_line_num=(\d+)$')
RX_DISCARD_AFTER_LINE_NUM = re.compile(r'^discard_after_line_num=(\d+)$')


class FileNamePrependQueue(queue.Queue):
    """Ensures ``file_name`` is put before other items, but only if they are put,
    to avoid reporting ``file_name`` for empty searches,
    or if ``FLUSH_FILE_NAME`` - for ``files_with_matches``"""
    FLUSH_FILE_NAME = object()

    def __init__(self, maxsize: int = 0):
        super().__init__(maxsize)
        self.file_name = None

    def put(self, item, block=True, timeout=None):
        if self.file_name and item is not None:
            super().put((0, RecordType.file_path, self.file_name), block, timeout)
            self.file_name = None
        if item is not self.FLUSH_FILE_NAME:
            super().put(item, block, timeout)


async def gen_matching_lines(file_path: Path, a: SearchArgs):
    pattern_rx = a.pattern if isinstance(a.pattern, re.Pattern) else None
    pattern_str = a.pattern if isinstance(a.pattern, str) else None
    except_pattern_rx = a.except_pattern if isinstance(a.except_pattern, re.Pattern) else None
    except_pattern_str = a.except_pattern if isinstance(a.except_pattern, str) else None
    discard_before_rx = a.discard_before if isinstance(a.discard_before, re.Pattern) else None
    discard_before_str = a.discard_before if isinstance(a.discard_before, str) else None
    discard_after_rx = a.discard_after if isinstance(a.discard_after, re.Pattern) else None
    discard_after_str = a.discard_after if isinstance(a.discard_after, str) else None
    log.debug(f"({file_path.name!r}, "
              f"discard_before={discard_before_rx.pattern if discard_before_rx else discard_before_str!r} [{'re' if discard_before_rx else 'str'}], "
              f"{a.before_context=}, pattern={pattern_rx.pattern if pattern_rx else pattern_str!r} [{'re' if pattern_rx else 'str'}], "
              f"except_pattern={except_pattern_rx.pattern if except_pattern_rx else except_pattern_str!r} [{'re' if except_pattern_rx else 'str'}], {a.after_context=}, "
              f"discard_after={discard_after_rx.pattern if discard_after_rx else discard_after_str!r} [{'re' if discard_after_rx else 'str'}], "
              f"{a.files_with_matches=})")
    before_deque = collections.deque(maxlen=a.before_context) if a.before_context else None

    def _gen_matching_lines(que):
        discard_before_line_num = 0
        if discard_before_str and (m := RX_DISCARD_BEFORE_LINE_NUM.match(discard_before_str)):
            with contextlib.suppress(ValueError):
                discard_before_line_num = int(m.group(1))
        discard_after_line_num = 0
        if discard_after_str and (m := RX_DISCARD_AFTER_LINE_NUM.match(discard_after_str)):
            with contextlib.suppress(ValueError):
                discard_after_line_num = int(m.group(1))

        def on_file_open(file_reader):
            """Called for each file; in an archive, for each member"""
            name = file_reader.name
            que.file_name = name
            log.debug(f"{name=}")

        try:
            ## sort paths by name (case-insensitive), but capitals first if e.g. A.txt and a.txt
            for path in sorted(file_path.parent.glob(file_path.name), key=lambda p: (p.name.lower(), p.name)):
                with FileReader(path, errors='backslashreplace', on_file_open=on_file_open) as file:
                    if discard_before_line_num == 0 and discard_before_rx or discard_before_str:
                        line_num = 0
                        for line_ in file:
                            line = line_.rstrip('\r\n')
                            line_num += 1
                            if (discard_before_rx and discard_before_rx.search(line)) or (discard_before_str and discard_before_str in line):
                                discard_before_line_num = line_num
                        file.rewind()
                    log.debug(f"{discard_before_line_num=}")
                    line_num = 0
                    lines_after = 0
                    match_found_so_can_process_after_context = False
                    for line_ in file:
                        line = line_.rstrip('\r\n')
                        line_num += 1
                        if discard_before_line_num > 0:
                            if line_num < discard_before_line_num:
                                continue
                            elif line_num == discard_before_line_num:
                                que.put((line_num, RecordType.discard_before, line))
                        if (((pattern_rx and pattern_rx.search(line)) or (pattern_str and pattern_str in line))
                                and not ((except_pattern_rx and except_pattern_rx.search(line)) or (except_pattern_str and except_pattern_str in line))):
                            if a.files_with_matches:
                                que.put(FileNamePrependQueue.FLUSH_FILE_NAME)
                                file.inner_seek(0, os.SEEK_END)
                            else:
                                while before_deque:
                                    que.put(before_deque.popleft())
                                que.put((line_num, RecordType.pattern, line))
                                lines_after = 0
                                match_found_so_can_process_after_context = True
                        else:
                            if before_deque is not None:
                                before_deque.append((line_num, RecordType.before_context, line))
                            if match_found_so_can_process_after_context:
                                if lines_after < a.after_context:
                                    que.put((line_num, RecordType.after_context, line))
                                    lines_after += 1
                                else:
                                    match_found_so_can_process_after_context = False
                        if (discard_after_line_num and line_num >= discard_after_line_num) or (discard_after_rx and discard_after_rx.search(line)) or (discard_after_str and discard_after_str in line):
                            que.put((line_num, RecordType.discard_after, line))
                            break
        except:
            traceback.print_exc()
            raise
        finally:
            que.put(None)

    que = FileNamePrependQueue()
    thread = threading.Thread(target=_gen_matching_lines, args=(que,))
    thread.start()
    try:
        while item := await asyncio.to_thread(que.get):
            yield item
    finally:
        thread.join()


def main(host=None, port=None, uuid_str=None, ssl_keyfile=None, ssl_keyfile_password=None, ssl_certificate=None):
    import uvicorn

    host = host or top_level_settings.host
    port = port or top_level_settings.port
    if ssl_keyfile := ssl_keyfile or top_level_settings.ssl_keyfile:
        ssl_keyfile = (p if (p := Path(ssl_keyfile)).is_absolute() else me.parent / p).as_posix()
    if (ssl_keyfile_password := ssl_keyfile_password or top_level_settings.ssl_keyfile_password) == Settings.ASK:
        ssl_keyfile_password = getpass.getpass(prompt='SSL keyfile password: ')
    if ssl_certificate := ssl_certificate or top_level_settings.ssl_certificate:
        ssl_certificate = (p if (p := Path(ssl_certificate)).is_absolute() else me.parent / p).as_posix()
    hostname = host if IPv4Address(host).is_loopback else socket.gethostname()
    uuid_str = uuid_str or top_level_settings.uuid
    url = f"http{'s' if ssl_keyfile and ssl_certificate else ''}://{hostname}:{port}/{uuid_str}"
    log.info(f"Starting logrep server: {url}/{SEARCH}")
    log.info(f"{ssl_keyfile=}, {ssl_certificate=}")
    log_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'to_queue': {'()': lambda: queue_handler},
        },
        'loggers': {
            'uvicorn': {'handlers': ['to_queue']},
        },
    }
    uvicorn.run(app, host=host, port=port, ssl_keyfile=ssl_keyfile, ssl_keyfile_password=ssl_keyfile_password, ssl_certfile=ssl_certificate, log_config=log_config)


if __name__ == "__main__":
    main()
