#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import socket
import uuid

import aiofiles
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import re
import tomllib
from typing import List, Tuple
from pathlib import Path

app = FastAPI()

uuid4_str = uuid.uuid4()

DEFAULT_AFTER_CONTEXT = 5


async def get_lines_between_matches(file_path: Path, pattern: re.Pattern, after_context: int) -> List[Tuple[int, str]]:
    """
    Search through a file and return matching lines with specified number of lines after each match.

    :param file_path: Path to the file to search
    :param pattern: Compiled regular expression pattern
    :param after_context: Number of lines to show after each match
    :returns: List of tuples containing (line_number, line_content)
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    matches = []
    lines_after = 0
    last_match_line = -1

    async with aiofiles.open(file_path, 'r') as file:
        line_num = 0
        async for line in file:
            line_num += 1

            if pattern.search(line):
                matches.append((line_num, line.rstrip()))
                lines_after = 0
                last_match_line = line_num
            elif lines_after < after_context and last_match_line != -1:
                matches.append((line_num, line.rstrip()))
                lines_after += 1

    return matches


def load_config():
    config_path = Path("logrep_server.toml")
    if not config_path.exists():
        raise HTTPException(status_code=500, detail="Configuration file not found")
    with open(config_path, "rb") as f:
        return tomllib.load(f)


class SearchRequest(BaseModel):
    pattern: str
    after_context: int | None = None


class SearchResponse(BaseModel):
    matches: List[tuple[int, str]]


async def _search_logs_common(pattern_str: str, after_context: int | None = None) -> SearchResponse:
    """Common search logic for both GET and POST endpoints."""
    if pattern_str == '':
        raise HTTPException(status_code=400, detail="pattern must not be empty")

    config = load_config()
    log_path = Path(config["log_file"])

    if not log_path.exists():
        raise HTTPException(status_code=404, detail="Log file not found")

    config_after_context = config.get("after_context", DEFAULT_AFTER_CONTEXT)
    final_after_context = after_context if after_context is not None else config_after_context

    if final_after_context < 0:
        raise HTTPException(status_code=400, detail="after_context must be non-negative")

    try:
        pattern = re.compile(pattern_str)
    except re.error as e:
        raise HTTPException(status_code=400, detail=f"Invalid regex pattern: {str(e)}")

    matches = await get_lines_between_matches(log_path, pattern, final_after_context)
    return SearchResponse(matches=matches)


@app.get(f"/{uuid4_str}/search")
async def search_logs_get(pattern: str, after_context: int | None = None):
    try:
        return await _search_logs_common(pattern, after_context)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post(f"/{uuid4_str}/search")
async def search_logs_post(search_request: SearchRequest):
    try:
        return await _search_logs_common(search_request.pattern, search_request.after_context)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def main(hostname=None, port=None):
    import uvicorn

    hostname = hostname or socket.gethostname()
    port = port or 8080
    url = f"http://{hostname}:{port}/{uuid4_str}/search?pattern="
    print(f"Starting Log Grep Server: {url}")
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
