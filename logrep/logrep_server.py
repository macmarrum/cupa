#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import asyncio
import bz2
import collections
import getpass
import json
import logging.handlers
import queue
import re
import socket
import threading
import tomllib
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, tzinfo, timezone, timedelta
from ipaddress import IPv4Address
from pathlib import Path
from string import Template
from typing import ClassVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from zstd_asgi import ZstdMiddleware

me = Path(__file__)
UTF8 = 'UTF-8'
APPLICATION_X_NDJSON = 'application/x-ndjson'
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


def fastapi_lifespan(app: FastAPI):
    log.info("Starting log_queue listener")
    queue_listener.start()
    yield
    log.info("Stopping log_queue listener")
    queue_listener.stop()


MINIMUM_SIZE = 1000
app = FastAPI(lifespan=fastapi_lifespan)
app.add_middleware(ZstdMiddleware, minimum_size=MINIMUM_SIZE)


@dataclass
class Settings:
    profile: str
    log_path: str = '/__not_set__'
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


## How to generate private key and self-signed certificate (365 days)
## Add ",IP:$(hostname -i)" to subjectAltName if you want to access your server via IP address; requires `hostname` from GNU inetutils
# openssl genpkey -algorithm ED25519 -out private.key
# openssl req -new -x509 -key private.key -out certificate.crt -days 365 -subj "/CN=$(hostname)" -addext "subjectAltName=DNS:$(hostname),DNS:localhost,IP:127.0.0.1"

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


class SearchRequest(BaseModel):
    profile: str | None = None
    discard_before: str | None = None
    before_context: str | None = None
    pattern: str | None = None
    except_pattern: str | None = None
    after_context: int | None = None
    discard_after: str | None = None


class SearchResponse(BaseModel):
    log_path: str
    matches: list[tuple[int, str, str]]


@app.get(f"/{top_level_settings.uuid}/search")
async def search_logs_get(profile: str | None = None, discard_before: str | None = None, before_context: int | None = None, pattern: str | None = None, except_pattern: str | None = None, after_context: int | None = None, discard_after: str | None = None):
    try:
        return StreamingResponse(search_logs(profile, discard_before, before_context, pattern, except_pattern, after_context, discard_after), media_type=APPLICATION_X_NDJSON)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Search error: {str(e)}")
        log.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.post(f"/{top_level_settings.uuid}/search")
async def search_logs_post(sr: SearchRequest):
    try:
        return StreamingResponse(search_logs(sr.profile, sr.discard_before, sr.before_context, sr.pattern, sr.except_pattern, sr.after_context, sr.discard_after), media_type=APPLICATION_X_NDJSON)
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


async def search_logs(profile: str | None = None, discard_before: str | None = None, before_context: int | None = None, pattern: str | None = None, except_pattern: str | None = None, after_context: int | None = None, discard_after: str | None = None):
    """Common search logic for both GET and POST endpoints; streams matching lines as NDJSON."""
    log.info(f"({profile=}, {discard_before=}, {before_context=}, {pattern=}, {except_pattern=}, {after_context=}, {discard_after=})")
    profile_to_settings = await config_loader.get_fresh_profile_to_settings()
    if profile:
        settings = profile_to_settings.get(profile)
        if not settings:
            raise HTTPException(status_code=404, detail=f"profile not found: {profile!r}")
    else:
        settings = profile_to_settings[TOP_LEVEL]
    log_path = Path(StrftimeTemplate(settings.log_path).substitute({'timezone': settings.timezone})).absolute()
    if discard_before := discard_before or settings.discard_before:
        if is_probably_complex_pattern(discard_before):
            try:
                discard_before = re.compile(discard_before)
            except re.error as e:
                raise HTTPException(status_code=400, detail=f"pattern {e}: {discard_before!r}")
        else:
            discard_before = RX_ESCAPE_FOLLOWED_BY_SPECIAL.sub('', discard_before)
    before_context = before_context if before_context is not None else settings.before_context
    if before_context < 0:
        raise HTTPException(status_code=400, detail='before_context must be non-negative')
    after_context = after_context if after_context is not None else settings.after_context
    if after_context < 0:
        raise HTTPException(status_code=400, detail='after_context must be non-negative')
    if pattern := pattern or settings.pattern:
        if is_probably_complex_pattern(pattern):
            try:
                pattern = re.compile(pattern)
            except re.error as e:
                raise HTTPException(status_code=400, detail=f"pattern {e}: {pattern!r}")
        else:
            pattern = RX_ESCAPE_FOLLOWED_BY_SPECIAL.sub('', pattern)
    if except_pattern := except_pattern or settings.except_pattern:
        if is_probably_complex_pattern(except_pattern):
            try:
                except_pattern = re.compile(except_pattern)
            except re.error as e:
                raise HTTPException(status_code=400, detail=f"except_pattern {e}: {except_pattern!r}")
        else:
            except_pattern = RX_ESCAPE_FOLLOWED_BY_SPECIAL.sub('', except_pattern)
    if discard_after := discard_after or settings.discard_after:
        if is_probably_complex_pattern(discard_after):
            try:
                discard_after = re.compile(discard_after)
            except re.error as e:
                raise HTTPException(status_code=400, detail=f"pattern {e}: {discard_after!r}")
        else:
            discard_after = RX_ESCAPE_FOLLOWED_BY_SPECIAL.sub('', discard_after)
    if not discard_before and not pattern and not discard_after:
        raise HTTPException(status_code=400, detail='discard_before or pattern or discard_after must be specified')
    list_of_lists = []
    total_line_size_in_list_of_lists = 0
    minimum_size_batch_count = 0
    async for item in gen_matching_lines(log_path, discard_before, before_context, pattern, except_pattern, after_context, discard_after):
        list_of_lists.append(item)
        total_line_size_in_list_of_lists += len(item[2])
        if total_line_size_in_list_of_lists >= MINIMUM_SIZE:
            yield json.dumps(list_of_lists) + '\n'
            list_of_lists.clear()
            total_line_size_in_list_of_lists = 0
            minimum_size_batch_count += 1
    if list_of_lists:
        yield json.dumps(list_of_lists) + '\n'
    log.debug(f"🗹 minimum_size_batch_count: {minimum_size_batch_count}")


class FileReader:

    def __init__(self, file_path: Path, encoding: str = UTF8, errors: str = 'strict'):
        self._file_path = file_path
        self._encoding = encoding
        self._errors = errors
        self._file = None

    def __enter__(self):
        match self._file_path.suffix:
            case '.bz2':
                self._file = bz2.open(self._file_path, 'rt', encoding=self._encoding, errors=self._errors)
            case _:
                self._file = open(self._file_path, 'r', encoding=self._encoding, errors=self._errors)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file.close()
        return False  # don't suppress exceptions cauth within with

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._file)

    def seek(self, offset, whence=0):
        self._file.seek(offset, whence)


class RecordType:
    log_path = 'l'
    discard_before = 'D'
    before_context = 'B'
    pattern = 'p'
    after_context = 'A'
    discard_after = 'd'


async def gen_matching_lines(file_path: Path, discard_before: str | re.Pattern | None, before_context: int, pattern: str | re.Pattern | None, except_pattern: str | re.Pattern | None, after_context: int, discard_after: str | re.Pattern | None):
    pattern_rx = pattern if isinstance(pattern, re.Pattern) else None
    pattern_str = pattern if isinstance(pattern, str) else None
    except_pattern_rx = except_pattern if isinstance(except_pattern, re.Pattern) else None
    except_pattern_str = except_pattern if isinstance(except_pattern, str) else None
    discard_before_rx = discard_before if isinstance(discard_before, re.Pattern) else None
    discard_before_str = discard_before if isinstance(discard_before, str) else None
    discard_after_rx = discard_after if isinstance(discard_after, re.Pattern) else None
    discard_after_str = discard_after if isinstance(discard_after, str) else None
    log.debug(f"({file_path.name!r}, "
              f"discard_before={discard_before_rx.pattern if discard_before_rx else discard_before_str!r} [{'re' if discard_before_rx else 'str'}], "
              f"{before_context=}, pattern={pattern_rx.pattern if pattern_rx else pattern_str!r} [{'re' if pattern_rx else 'str'}], "
              f"except_pattern={except_pattern_rx.pattern if except_pattern_rx else except_pattern_str!r} [{'re' if except_pattern_rx else 'str'}], {after_context=}, "
              f"discard_after={discard_after_rx.pattern if discard_after_rx else discard_after_str!r} [{'re' if discard_after_rx else 'str'}])")
    before_deque = collections.deque(maxlen=before_context) if before_context else None

    def _gen_matching_lines(que):
        try:
            # sort paths by name (case-insensitive), but capitals first if e.g. A.txt and a.txt
            for path in sorted(file_path.parent.glob(file_path.name), key=lambda p: (p.name.lower(), p.name)):
                log.debug(f"{path.as_posix()!r}")
                que.put((0, RecordType.log_path, path.as_posix()))
                with FileReader(path, errors='backslashreplace') as file:
                    discard_before_line_num = 0
                    if discard_before_rx or discard_before_str:
                        line_num = 0
                        for line_ in file:
                            line = line_.rstrip('\r\n')
                            line_num += 1
                            if (discard_before_rx and discard_before_rx.search(line)) or (discard_before_str and discard_before_str in line):
                                discard_before_line_num = line_num
                        file.seek(0)
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
                        if (discard_after_rx and discard_after_rx.search(line)) or (discard_after_str and discard_after_str in line):
                            que.put((line_num, RecordType.discard_after, line))
                            break
                        if (((pattern_rx and pattern_rx.search(line)) or (pattern_str and pattern_str in line))
                                and not ((except_pattern_rx and except_pattern_rx.search(line)) or (except_pattern_str and except_pattern_str in line))):
                            while before_deque:
                                que.put(before_deque.popleft())
                            que.put((line_num, RecordType.pattern, line))
                            lines_after = 0
                            match_found_so_can_process_after_context = True
                        else:
                            if before_deque is not None:
                                before_deque.append((line_num, RecordType.before_context, line))
                            if match_found_so_can_process_after_context:
                                if lines_after < after_context:
                                    que.put((line_num, RecordType.after_context, line))
                                    lines_after += 1
                                else:
                                    match_found_so_can_process_after_context = False
        except:
            traceback.print_exc()
            raise
        finally:
            que.put(None)

    que = queue.Queue()
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
    log.info(f"Starting logrep server: {url}")
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
