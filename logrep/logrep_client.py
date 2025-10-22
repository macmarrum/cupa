#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import argparse
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

import requests
from colorama import init, Fore, Style

me = Path(__file__)

# Note: special chars could be either escaped or bracketed [] to make them literal
# Bracketing is not accounted for here, hence "probably"
RX_PROBABLY_COMPLEX_PATTERN = re.compile(r'(?<!\\)[()\[{.*+?^$|]|\\[AbdDsSwWzZ]')
RX_ESCAPE_FOLLOWED_BY_SPECIAL = re.compile(r'\\(?=[()\[{.*+?^$|])')


def is_probably_complex_pattern(pattern: str):
    return RX_PROBABLY_COMPLEX_PATTERN.search(pattern) is not None


@dataclass
class Settings:
    url: str | None = None
    pattern: str | None = None
    section: str | None = None  # table in client toml
    profile: str | None = None  # table in server toml
    after_context: int | None = None
    color: str = 'never'
    line_number: bool = False
    verbose: bool = False


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
            key = key.replace('-', '_')
            common_kwargs_for_settings[key] = value
    for section, dct in profile_to_dict.items():
        kwargs_for_settings = common_kwargs_for_settings.copy()
        kwargs_for_settings['section'] = section
        for key, value in dct.items():
            kwargs_for_settings[key] = value
        profile_to_settings[section] = Settings(**kwargs_for_settings)
    return profile_to_settings


def load_config():
    try:
        return make_profile_to_settings_from_toml_path(me.with_suffix('.toml'))
    except FileNotFoundError:
        return {TOP_LEVEL: {}}


def grep(argv=None):
    parser = argparse.ArgumentParser()
    pattern_gr = parser.add_mutually_exclusive_group()
    pattern_gr.add_argument('pattern_positional', nargs='?')
    pattern_gr.add_argument('-p', '--pattern', )
    parser.add_argument('-S', '--section')
    parser.add_argument('--url')
    parser.add_argument('-A', '--after-context', default=None, type=int)
    parser.add_argument('-P', '--profile')
    parser.add_argument('-n', '--line-number', action='store_true')
    parser.add_argument('--color', choices=['auto', 'always', 'never'], nargs='?')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args(argv)
    profile_to_settings = load_config()
    try:
        settings = profile_to_settings[args.section or TOP_LEVEL]
    except KeyError:
        print(f"section {args.section!r} not found in config file - available sections: {[k for k in profile_to_settings if k != TOP_LEVEL]}")
        sys.exit(1)
    line_number = args.line_number or settings.line_number
    verbose = args.verbose or settings.verbose
    color = args.color or settings.color
    profile = args.profile or settings.profile
    _profile = f"profile={quote(profile)}" if profile else None
    pattern = args.pattern_positional or args.pattern or settings.pattern
    _pattern = f"pattern={quote(pattern)}" if pattern else None
    after_context = args.after_context or settings.after_context
    _after_context = f"after_context={after_context}" if after_context else None
    base_url = (args.url or settings.url).rstrip('/')
    url = f"{base_url}/search?{'&'.join(e for e in [_profile, _pattern, _after_context] if e)}"
    use_color = color == 'always' or (color == 'auto' and sys.stdout.isatty())
    verbose and print(f"{Fore.CYAN}{url}{Style.RESET_ALL}" if use_color else url)
    resp = requests_get_or_exit(url)
    verbose and print(f"{Fore.YELLOW}{resp.headers}{Style.RESET_ALL}" if use_color else resp.headers)
    try:
        d = resp.json()
    except requests.exceptions.JSONDecodeError:
        print(resp.text, file=sys.stderr)
        print(sys.exc_info(), file=sys.stderr)
        return
    if matches := d.get('matches'):
        max_num = matches[-1][0]
        size = len(str(max_num))
        prev_num = 0
        pattern_rx = None
        if use_color:
            init()  # colorama
            if is_probably_complex_pattern(pattern):
                pattern_rx = re.compile(pattern)
            else:
                pattern = RX_ESCAPE_FOLLOWED_BY_SPECIAL.sub('', pattern)
        for num, match_found, line in matches:
            sep = ':' if match_found else '-'
            if prev_num and prev_num + 1 != num:
                if use_color:
                    print(f"{Fore.GREEN}--{Fore.RESET}")
                else:
                    print('--')
            if use_color:
                colored_num_sep = f"{Fore.GREEN}{num:{size}d}{sep}{Style.RESET_ALL}" if line_number else ''
                _line_ = make_colored_line(line, pattern, pattern_rx) if match_found else line
                print(f"{colored_num_sep}{_line_}")
            else:
                num_sep = f"{num:{size}d}{sep}" if line_number else ''
                print(f"{num_sep}{line}")
            prev_num = num
    else:
        print(d.get('details'), file=sys.stderr)


HEADERS = {'Accept-Encoding': 'zstd, br, gzip'}


def requests_get_or_exit(url: str) -> requests.Response:
    try:
        resp = requests.get(url, headers=HEADERS)
    except requests.ConnectionError as e:
        et = type(e)
        print(f"{et.__module__}.{et.__qualname__}: {e}", file=sys.stderr)
        sys.exit(1)
    if resp.status_code != 200:
        print(resp.status_code, resp.reason, file=sys.stderr)
        print(resp.text, file=sys.stderr)
        sys.exit(1)
    return resp


def make_colored_line(line: str, pattern: str | None, pattern_rx: re.Pattern | None) -> str:
    if pattern_rx:
        m = pattern_rx.search(line)
        if m.groups():
            colored_line = ''
            for is_match, text in decompose_into_groups(line, m):
                colored_line += f"{Style.BRIGHT}{Fore.RED}{text}{Style.RESET_ALL}" if is_match else text
        else:
            colored_line = pattern_rx.sub(lambda m: f"{Style.BRIGHT}{Fore.RED}{m[0]}{Style.RESET_ALL}", line)
    else:
        colored_line = line.replace(pattern, f"{Style.BRIGHT}{Fore.RED}{pattern}{Style.RESET_ALL}")
    return colored_line


def decompose_into_groups(line: str, m: re.Match) -> list[tuple[bool, str]]:
    """Decomposes a line based on a regex match into a list of (is_match, text)"""
    if not m:
        return [(False, line)]
    result_list: list[tuple[bool, str]] = []
    # 1. Handle the prefix (unmatched text before the full match)
    prefix_end = m.start(0)
    if prefix_end > 0:
        result_list.append((False, line[0:prefix_end]))
    # 2. Iterate through all captured groups and the text between them
    current_pos = m.start(0)
    for g in range(1, len(m.groups()) + 1):
        group_start = m.start(g)
        group_end = m.end(g)
        group_text = m.group(g)
        # A. Handle the UNMATCHED text between the last position and the current group's start
        if group_start > current_pos:
            unmatched_text = line[current_pos:group_start]
            result_list.append((False, unmatched_text))
        # B. Handle the CAPTURED GROUP text (is_match = True)
        if group_text is not None:
            result_list.append((True, group_text))
        # Update the current position to the end of the current group
        current_pos = group_end
    # 3. Handle the suffix (unmatched text after the full match)
    suffix_start = m.end(0)
    if suffix_start < len(line):
        result_list.append((False, line[suffix_start:len(line)]))
    return result_list


if __name__ == '__main__':
    grep()
