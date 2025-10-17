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
# Bracketing is not accounter for here, hence "probably"
RX_PROBABLY_COMPLEX_PATTERN = re.compile(r'(?<!\\)[()\[\]{}.*+?^$|]')


def is_probably_complex_pattern(pattern: str):
    return RX_PROBABLY_COMPLEX_PATTERN.search(pattern) is not None


@dataclass
class Settings:
    url: str | None = None
    profile: str | None = None
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
    parser.add_argument('--url')
    parser.add_argument('-A', '--after-context', default=None, type=int)
    parser.add_argument('-p', '--profile')
    parser.add_argument('-n', '--line-number', action='store_true')
    parser.add_argument('--color', choices=['auto', 'always', 'never'], nargs='?')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args(argv)
    profile_to_settings = load_config()
    if args.profile:
        settings = profile_to_settings.get(args.profile, profile_to_settings[TOP_LEVEL])
    else:
        settings = profile_to_settings[TOP_LEVEL]
    line_number = args.line_number or settings.line_number
    verbose = args.verbose or settings.verbose
    color = args.color or settings.color
    _profile = f"&profile={quote(args.profile)}" if args.profile else ''
    pattern_str = args.pattern_positional or args.pattern
    after_context = args.after_context or settings.after_context
    _after_context = f"&after_context={after_context}" if after_context else ''
    base_url = (args.url or settings.url).rstrip('/')
    url = f"{base_url}/search?pattern={quote(pattern_str)}{_after_context}{_profile}"
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
        if use_color and is_probably_complex_pattern(pattern_str):
            init()  # colorama
            pattern = re.compile(pattern_str)
        else:
            pattern = None
        for num, match_found, line in matches:
            sep = ':' if match_found else '-'
            if prev_num and prev_num + 1 != num:
                if use_color:
                    print(f"{Fore.GREEN}--{Fore.RESET}")
                else:
                    print('--')
            if use_color:
                if pattern:
                    colored_line = pattern.sub(lambda m: f"{Style.BRIGHT}{Fore.RED}{m[0]}{Style.RESET_ALL}", line)
                else:
                    colored_line = line.replace(pattern_str, f"{Style.BRIGHT}{Fore.RED}{pattern_str}{Style.RESET_ALL}")
                colored_num_sep = f"{Fore.GREEN}{num:{size}d}{sep}{Style.RESET_ALL}" if line_number else ''
                print(f"{colored_num_sep}{colored_line}")
            else:
                num_sep = f"{num:{size}d}{sep}" if line_number else ''
                print(f"{num_sep}{line}")
            prev_num = num
    else:
        print(d.get('details'))


if __name__ == '__main__':
    grep()
