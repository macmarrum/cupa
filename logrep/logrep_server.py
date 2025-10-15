#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import asyncio
import logging
import re
import socket
import tomllib
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from string import Template

from brotli_asgi import BrotliMiddleware
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from zstd_asgi import ZstdMiddleware

logging.basicConfig(format='{asctime} {levelname} {funcName}: {msg}', style='{', level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
app.add_middleware(ZstdMiddleware, minimum_size=500)
app.add_middleware(BrotliMiddleware, minimum_size=500)


@dataclass
class Settings:
    profile: str
    log_path: str = '/__not_set__'
    after_context: int = 0
    host: str = '0.0.0.0'
    port: int = 8000
    uuid: str = str(uuid.uuid4())


TOP_LEVEL = 'top-level'
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


async def get_lines_between_matches(file_path: Path, pattern: re.Pattern, after_context: int, pattern_str: str = None):
    """
    Search through a file and return matching lines with specified number of lines after each match.

    :param file_path: Path to the file to search
    :param pattern: Compiled regular expression pattern
    :param after_context: Number of lines to show after each match
    :param pattern_str: Simple string pattern to search for
    :returns: List of tuples containing (line_number, line_content)
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    def file_reader():
        matches = []
        line_num = 0
        lines_after = 0
        last_match_line = -1

        with open(file_path, 'r') as file:
            for line in file:
                line_num += 1

                if (pattern and pattern.search(line)) or pattern_str in line:
                    matches.append((line_num, line.rstrip()))
                    lines_after = 0
                    last_match_line = line_num
                elif lines_after < after_context and last_match_line != -1:
                    matches.append((line_num, line.rstrip()))
                    lines_after += 1

        return matches

    return await asyncio.to_thread(file_reader)


class SearchRequest(BaseModel):
    pattern: str
    after_context: int | None = None
    profile: str | None = None


class SearchResponse(BaseModel):
    matches: list[tuple[int, str]]


# Note: special chars could be either escaped or bracketed [] to make them literal
# Bracketing is not accounter for here, hence "possibly"
RX_POSSIBLY_COMPLEX_PATTERN = re.compile(r'(?<!\\)[()\[\]{}.*+?^$|]')


def is_possibly_complex_pattern(pattern: str):
    return RX_POSSIBLY_COMPLEX_PATTERN.search(pattern) is not None


class TimeTemplate(Template):
    """Substitutes time spec at the current time, specified within <...>, e.g. <%m/%d %Y> => 10/15 2025"""
    flags = re.VERBOSE  # to override the default re.IGNORECASE
    delimiter = '<'
    idpattern = '[^>]+>'

    def substitute(self, mapping=None, **kwargs):
        if mapping is None:
            mapping = {}

        class TimeWrapper:
            def __getitem__(self, key):
                if key.endswith('>'):
                    fmt = key[:-1]
                    return datetime.now().strftime(fmt)
                return mapping.get(key, '')

        return super().substitute(TimeWrapper())


async def search_logs(pattern_str: str, after_context: int | None = None, profile: str | None = None) -> SearchResponse:
    """Common search logic for both GET and POST endpoints."""
    logger.info(f"profile={profile!r}, pattern={pattern_str!r}, after_context={after_context!r}")
    if pattern_str == '':
        raise HTTPException(status_code=400, detail='pattern must not be empty')

    settings = profile_to_settings.get(profile or TOP_LEVEL)
    if not settings:
        raise HTTPException(status_code=404, detail=f"Profile not found: {profile!r}")

    log_path = Path(TimeTemplate(settings.log_path).substitute())
    if not log_path.exists():
        raise HTTPException(status_code=404, detail=f"Log file not found: {log_path.__str__()!r}")

    after_context = after_context if after_context is not None else settings.after_context

    if after_context < 0:
        raise HTTPException(status_code=400, detail='after_context must be non-negative')

    if is_possibly_complex_pattern(pattern_str):
        try:
            pattern = re.compile(pattern_str)
        except re.error as e:
            raise HTTPException(status_code=400, detail=f"Invalid regex pattern: {str(e)}")
    else:
        pattern = None

    matches = await get_lines_between_matches(log_path, pattern, after_context, pattern_str)
    logger.info(f"Found {len(matches)} matches in {log_path.__str__()!r}")
    return SearchResponse(matches=matches)


@app.get(f"/{top_level_settings.uuid}/search")
async def search_logs_get(pattern: str, after_context: int | None = None, profile: str | None = None):
    try:
        return await search_logs(pattern, after_context, profile)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Search error: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.post(f"/{top_level_settings.uuid}/search")
async def search_logs_post(search_request: SearchRequest):
    try:
        return await search_logs(search_request.pattern, search_request.after_context, search_request.profile)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Search error: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


def main(host=None, port=None):
    import uvicorn

    host = host or top_level_settings.host
    port = port or top_level_settings.port
    hostname = socket.gethostname()
    url = f"http://{hostname}:{port}/{top_level_settings.uuid}"
    logger.info(f"Starting Log Grep Server: {url}")
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
