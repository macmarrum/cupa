#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import asyncio
import logging.handlers
import queue
import re
import socket
import tomllib
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, tzinfo, timezone, timedelta
from ipaddress import IPv4Address
from pathlib import Path
from string import Template
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from brotli_asgi import BrotliMiddleware
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from zstd_asgi import ZstdMiddleware

me = Path(__file__)

formatter = logging.Formatter('{asctime} {levelname} {name} [{funcName}] {message}', style='{')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
log_queue = queue.SimpleQueue()
queue_handler = logging.handlers.QueueHandler(log_queue)
queue_listener = logging.handlers.QueueListener(log_queue, console_handler, respect_handler_level=False)

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(me.stem)
log.addHandler(queue_handler)


def fastapi_lifespan(app: FastAPI):
    log.info("Starting log_queue listener")
    queue_listener.start()
    yield
    log.info("Stopping log_queue listener")
    queue_listener.stop()


app = FastAPI(lifespan=fastapi_lifespan)
app.add_middleware(ZstdMiddleware, minimum_size=500)
app.add_middleware(BrotliMiddleware, minimum_size=500)


@dataclass
class Settings:
    profile: str
    log_path: str = '/__not_set__'
    discard_before: str | None = None
    pattern: str = ''
    after_context: int = 0
    discard_after: str | None = None
    host: str = '0.0.0.0'
    port: int = 8000
    uuid: str = str(uuid.uuid4())
    # timezone can be any of zoneinfo.available_timezones()
    # or an offset from UTC, e.g. -03:30, UTC-03:30, +02:00, UTC+02:00
    timezone: str | None = None


TOP_LEVEL = '#top-level'
ProfileToSettings = dict[str, Settings]


def make_profile_to_settings_from_toml_path(toml_file: Path) -> ProfileToSettings:
    toml_str = toml_file.read_text(encoding='UTF-8')
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


def load_config():
    config_path = Path(__file__).with_suffix('.toml')
    if not config_path.exists():
        raise HTTPException(status_code=500, detail='Configuration file not found')
    return make_profile_to_settings_from_toml_path(config_path)


profile_to_settings = load_config()
top_level_settings = profile_to_settings[TOP_LEVEL]


class SearchRequest(BaseModel):
    profile: str | None = None
    discard_before: str | None = None
    pattern: str | None = None
    after_context: int | None = None
    discard_after: str | None = None


class SearchResponse(BaseModel):
    matches: list[tuple[int, str, str]]


@app.get(f"/{top_level_settings.uuid}/search")
async def search_logs_get(profile: str | None = None, pattern: str | None = None, after_context: int | None = None, discard_before: str | None = None, discard_after: str | None = None):
    try:
        return await search_logs(profile, discard_before, pattern, after_context, discard_after)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Search error: {str(e)}")
        log.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.post(f"/{top_level_settings.uuid}/search")
async def search_logs_post(sr: SearchRequest):
    try:
        return await search_logs(sr.profile, sr.discard_before, sr.pattern, sr.after_context, sr.discard_after)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Search error: {str(e)}")
        log.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


class StrftimeTemplate(Template):
    """
    Substitutes the current time for the format string specified within angle brackets,
    e.g., `Today is <%m/%d %Y>` becomes `Today is 10/16 2025`.
    The optional mapping passed to `substitute` can specify a tzinfo instance,
    e.g., `substitute({'timezone': ZoneInfo('Europe/Warsaw')})`
    """
    flags = re.VERBOSE  # to override the default re.IGNORECASE
    delimiter = '<'
    idpattern = '[^>]+>'

    def substitute(self, mapping=None, **kwargs):
        class StrftimeResolver:
            def __getitem__(self, key):
                _timezone = mapping.get('timezone') if mapping else None
                _tzinfo = self.parse_timezone(_timezone)
                return datetime.now(_tzinfo).strftime(key[:-1])

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


async def search_logs(profile: str | None = None, discard_before: str | None = None, pattern: str | None = None, after_context: int | None = None, discard_after: str | None = None) -> SearchResponse:
    """Common search logic for both GET and POST endpoints."""
    log.info(f"(profile={profile!r}, pattern={pattern!r}, after_context={after_context!r})")
    if profile:
        settings = profile_to_settings.get(profile)
        if not settings:
            raise HTTPException(status_code=404, detail=f"profile not found: {profile!r}")
    else:
        settings = top_level_settings
    log_path = Path(StrftimeTemplate(settings.log_path).substitute({'timezone': settings.timezone}))
    if not log_path.exists():
        raise HTTPException(status_code=404, detail=f"log_path doesn't exist: {log_path.__str__()!r}")
    if discard_before := discard_before or settings.discard_before:
        if is_probably_complex_pattern(discard_before):
            try:
                discard_before = re.compile(discard_before)
            except re.error as e:
                raise HTTPException(status_code=400, detail=f"pattern {e}: {discard_before!r}")
        else:
            discard_before = RX_ESCAPE_FOLLOWED_BY_SPECIAL.sub('', discard_before)
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
    matches = await get_matching_lines(log_path, discard_before, pattern, after_context, discard_after)
    log.debug(f"Found {len(matches)} matches in {log_path.__str__()!r}")
    return SearchResponse(matches=matches)


class MatchType:
    discard_before = 'D'
    pattern = 'p'
    after_context = 'A'
    discard_after = 'd'


async def get_matching_lines(file_path: Path, discard_before: str | re.Pattern | None, pattern: str | re.Pattern | None, after_context: int, discard_after: str | re.Pattern | None):
    """
    Searches through a file and return matching lines with specified number of lines after each match.
    :returns: List of tuples containing (line_number, match_found, line_content)
    """
    pattern_rx = pattern if isinstance(pattern, re.Pattern) else None
    pattern_str = pattern if isinstance(pattern, str) else None
    discard_before_rx = discard_before if isinstance(discard_before, re.Pattern) else None
    discard_before_str = discard_before if isinstance(discard_before, str) else None
    discard_after_rx = discard_after if isinstance(discard_after, re.Pattern) else None
    discard_after_str = discard_after if isinstance(discard_after, str) else None
    log.info(f"({file_path.name!r}, "
             f"discard_before={discard_before_rx.pattern if discard_before_rx else discard_before_str!r} [{'rgx' if discard_before_rx else 'str'}], "
             f"pattern={pattern_rx.pattern if pattern_rx else pattern_str!r} [{'rgx' if pattern_rx else 'str'}], {after_context=}, "
             f"discard_after={discard_after_rx.pattern if discard_after_rx else discard_after_str!r} [{'rgx' if discard_after_rx else 'str'}])")

    def _get_matching_lines(discard_before_already_found=False):
        matches = []
        line_num = 0
        lines_after = 0
        last_match_line = -1
        with open(file_path, 'r') as file:
            for line_ in file:
                line = line_.rstrip('\r\n')
                line_num += 1
                if (discard_after_rx and discard_after_rx.search(line)) or (discard_after_str and discard_after_str in line):
                    matches.append((line_num, MatchType.discard_after, line))
                    break
                if (discard_before_rx and discard_before_rx.search(line)) or (discard_before_str and discard_before_str in line):
                    discard_before_already_found = True
                    matches.clear()
                    matches.append((line_num, MatchType.discard_before, line))
                if discard_before_already_found or (not discard_before_rx and not discard_before_str):
                    if (pattern_rx and pattern_rx.search(line)) or (pattern_str and pattern_str in line):
                        matches.append((line_num, MatchType.pattern, line))
                        lines_after = 0
                        last_match_line = line_num
                    elif lines_after < after_context and last_match_line != -1:
                        matches.append((line_num, MatchType.after_context, line))
                        lines_after += 1
        if (discard_before_rx or discard_before_str) and not discard_before_already_found and (pattern_rx or pattern_str):
            return _get_matching_lines(discard_before_already_found=True)
        return matches

    return await asyncio.to_thread(_get_matching_lines)


def main(host=None, port=None):
    import uvicorn

    host = host or top_level_settings.host
    port = port or top_level_settings.port
    hostname = host if IPv4Address(host).is_loopback else socket.gethostname()
    url = f"http://{hostname}:{port}/{top_level_settings.uuid}"
    log.info(f"Starting logrep server: {url}")
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
    uvicorn.run(app, host=host, port=port, log_config=log_config)


if __name__ == "__main__":
    main()
