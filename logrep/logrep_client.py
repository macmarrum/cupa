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
        key = key.replace('-', '_')
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
    try:
        return make_profile_to_settings_from_toml_path(me.with_suffix('.toml'))
    except FileNotFoundError:
        return {TOP_LEVEL: {}}


def grep(argv=None):
    parser = argparse.ArgumentParser()
    pattern_gr = parser.add_mutually_exclusive_group()
    pattern_gr.add_argument('pattern_positional', nargs='?')
    pattern_gr.add_argument('-P', '--pattern', )
    parser.add_argument('-s', '--section')
    parser.add_argument('--url')
    parser.add_argument('-A', '--after-context', default=None, type=int)
    parser.add_argument('-p', '--profile')
    parser.add_argument('-n', '--line-number', action='store_true')
    parser.add_argument('--color', choices=['auto', 'always', 'never'], nargs='?')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args(argv)
    profile_to_settings = load_config()
    settings = profile_to_settings[args.section or TOP_LEVEL]
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
    verbose and print(url)
    resp = requests.get(url, headers={'Accept-Encoding': 'zstd, br, gzip, deflate'})
    if resp.status_code != 200:
        print(resp.status_code, resp.reason, file=sys.stderr)
        print(resp.text, file=sys.stderr)
        return
    verbose and print(resp.headers)
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
        use_color = color == 'always' or (color == 'auto' and sys.stdout.isatty())
        if use_color:
            init()  # colorama
        pattern_rx = re.compile(pattern) if use_color and is_probably_complex_pattern(pattern) else None
        for num, match_found, line in matches:
            sep = ':' if match_found else '-'
            if prev_num and prev_num + 1 != num:
                if use_color:
                    print(f"{Fore.GREEN}--{Fore.RESET}")
                else:
                    print('--')
            if use_color:
                _line_ = make_colored_line(line, pattern, pattern_rx) if match_found else line
                colored_num_sep = f"{Fore.GREEN}{num:{size}d}{sep}{Style.RESET_ALL}" if line_number else ''
                print(f"{colored_num_sep}{_line_}")
            else:
                num_sep = f"{num:{size}d}{sep}" if line_number else ''
                print(f"{num_sep}{line}")
            prev_num = num
    else:
        print(d.get('details'))


def make_colored_line(line: str, pattern: str | None, pattern_rx: re.Pattern | None) -> str:
    if pattern_rx:
        m = pattern_rx.search(line)
        if m.groups():
            span_to_text = decompose_line(line, m)
            colored_line = ''
            for span, (is_match, text) in span_to_text.items():
                colored_line += f"{Style.BRIGHT}{Fore.RED}{text}{Style.RESET_ALL}" if is_match else text
        else:
            colored_line = pattern_rx.sub(lambda m: f"{Style.BRIGHT}{Fore.RED}{m[0]}{Style.RESET_ALL}", line)
    else:
        colored_line = line.replace(pattern, f"{Style.BRIGHT}{Fore.RED}{pattern}{Style.RESET_ALL}")
    return colored_line


def decompose_line(line: str, m: re.Match) -> dict[tuple[int, int], tuple[bool, str]]:
    """Decomposes a line based on a regex match into a dictionary: span => (is_match, text)"""
    if not m:
        return {(0, len(line)): (False, line)}
    span_to_text: dict[tuple[int, int], tuple[bool, str]] = {}
    # 1. Handle the prefix (unmatched text before the full match)
    prefix_span = (0, m.start(0))
    if prefix_span[1] > prefix_span[0]:
        span_to_text[prefix_span] = (False, line[prefix_span[0]:prefix_span[1]])
    # Get a list of all captured group spans (index, start, end)
    # Filter out spans where start == -1 (group didn't match, though unlikely here)
    group_spans: list[tuple[int, int, int]] = sorted([(i, m.start(i), m.end(i)) for i in range(1, len(m.groups()) + 1) if m.start(i) != -1], key=lambda x: x[1])
    # Initialize the current position within the full match
    current_pos = m.start(0)
    # 2. Iterate through all captured groups
    for i, start, end in group_spans:
        # A. Handle the UNMATCHED text between the last processed span (or start of match) and the current group
        if start > current_pos:
            unmatched_span = (current_pos, start)
            span_to_text[unmatched_span] = (False, line[unmatched_span[0]:unmatched_span[1]])
        # B. Handle the CAPTURED GROUP text
        matched_span = (start, end)
        span_to_text[matched_span] = (True, line[matched_span[0]:matched_span[1]])
        # Update the current position
        current_pos = end
    # 3. Handle the suffix (unmatched text after the full match)
    suffix_span = (m.end(0), len(line))
    if suffix_span[1] > suffix_span[0]:
        span_to_text[suffix_span] = (False, line[suffix_span[0]:suffix_span[1]])
    return dict(sorted(span_to_text.items()))


if __name__ == '__main__':
    grep()
