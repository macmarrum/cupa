#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import argparse
import importlib
import json
import re
import sys
import tomllib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Generator
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
    section: str | None = None  # table in client toml
    profile: str | None = None  # table in server toml
    verify: str | bool | None = None
    url: str | None = None
    discard_before: str | None = None
    context: int | None = None
    before_context: int | None = None
    pattern: str | None = None
    except_pattern: str | None = None
    after_context: int | None = None
    discard_after: str | None = None
    line_number: bool = False
    color: str | None = None
    verbose: bool = False
    header_template: str | None = None
    footer_template: str | None = None
    template_processor: str | None = None


TOP_LEVEL = '#top-level'
ProfileToSettings = dict[str, Settings]


def resolve_callable(callable_string: str) -> Callable:
    """
    Resolve a callable from a string like 'module.submodule:function_name'.

    Examples:
        - 'html:escape'
        - 'xml.sax.saxutils:escape'
        - 'mymodule.processors:custom_escape'
    """
    if callable_string is None:
        return str
    if isinstance(callable_string, Callable):
        return callable_string
    if ':' not in callable_string:
        raise ValueError(f"Callable string must be in format 'module:function', got: {callable_string}")
    module_name, func_name = callable_string.rsplit(':', 1)
    module = importlib.import_module(module_name)
    try:
        return getattr(module, func_name)
    except AttributeError:
        raise AttributeError(f"Module '{module_name}' has no attribute '{func_name}'")


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
        return {TOP_LEVEL: Settings()}


class HeaderTemplate:
    NAME = 'logrep'

    def __init__(self, template: str):
        self._template = template

    def format(self, line_number: bool = None, color: str = None, discard_before: str = None, discard_after: str = None, before_context: int = None, after_context: int = None, pattern: str = None, except_pattern: str = None, log_path: Path = None, datefmt: str = '%Y-%m-%d %H:%M:%SZ',
               tz: timezone = timezone.utc, processor: Callable = str):
        dct = {
            'asctime': processor(datetime.now(tz).strftime(datefmt)),
            'command': processor(' '.join(e for e in [
                self.NAME,
                '-n' if line_number is not None else '',
                # f"--color={color}" if color else '',
                f"--discard-before={discard_before!r}" if discard_before else '',
                f"--discard-after={discard_after!r}" if discard_after else '',
                f"-B {before_context}" if before_context else '',
                f"-A {after_context}" if after_context else '',
                f"-e {pattern!r}",
                f"--except-pattern={except_pattern!r}" if except_pattern else '',
                f"{log_path.name!r}",
            ] if e))
        }
        return self._template.format_map(dct)


class RecordType:
    log_path = 'L'
    discard_before = 'D'
    before_context = 'B'
    pattern = 'p'
    after_context = 'A'
    discard_after = 'd'


def grep(argv=None):
    # print(f"grep {argv}", file=sys.stderr)
    parser = argparse.ArgumentParser()
    parser.add_argument('-S', '--section')
    parser.add_argument('-P', '--profile')
    parser.add_argument('--url')
    parser.add_argument('--verify')
    parser.add_argument('-D', '--discard-before')
    parser.add_argument('-C', '--context', default=None, type=int)
    parser.add_argument('-B', '--before-context', default=None, type=int)
    pattern_gr = parser.add_mutually_exclusive_group()
    pattern_gr.add_argument('pattern_positional', nargs='?')
    pattern_gr.add_argument('-e', '--pattern')
    parser.add_argument('-E', '--except-pattern')
    parser.add_argument('-A', '--after-context', default=None, type=int)
    parser.add_argument('-d', '--discard-after')
    parser.add_argument('-n', '--line-number', action='store_true')
    parser.add_argument('--color', choices=['auto', 'always', 'never'], nargs='?')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('-N', '--no-compression', action='store_true')
    args = parser.parse_args(argv)
    profile_to_settings = load_config()
    try:
        settings = profile_to_settings[args.section or TOP_LEVEL]
    except KeyError:
        print(f"section {args.section!r} not found in config file - available sections: {[k for k in profile_to_settings if k != TOP_LEVEL]}")
        sys.exit(1)
    profile = args.profile or settings.profile
    _profile = f"profile={quote(profile)}" if profile else None
    base_url = (args.url or settings.url).rstrip('/')
    discard_before = args.discard_before or settings.discard_before
    _discard_before = f"discard_before={quote(discard_before)}" if discard_before else None
    context = args.context or settings.context
    before_context = args.before_context or settings.before_context or context
    _before_context = f"before_context={before_context}" if before_context else None
    pattern = args.pattern_positional or args.pattern or settings.pattern
    _pattern = f"pattern={quote(pattern)}" if pattern else None
    except_pattern = args.except_pattern or settings.except_pattern
    _except_pattern = f"except_pattern={quote(except_pattern)}" if except_pattern else None
    after_context = args.after_context or settings.after_context or context
    _after_context = f"after_context={after_context}" if after_context else None
    discard_after = args.discard_after or settings.discard_after
    _discard_after = f"discard_after={quote(discard_after)}" if discard_after else None
    color = args.color or settings.color
    line_number = args.line_number or settings.line_number
    verbose = args.verbose or settings.verbose
    url = f"{base_url}/search?{'&'.join(e for e in [_profile, _before_context, _pattern, _except_pattern, _after_context, _discard_before, _discard_after] if e)}"
    use_color = color == 'always' or (color == 'auto' and sys.stdout.isatty())
    verbose and print(f"{Fore.CYAN}{url}{Style.RESET_ALL}" if use_color else url, file=sys.stderr)
    template_processor = resolve_callable(settings.template_processor)
    if isinstance(verify := args.verify or settings.verify, str):
        verify = (p if (p := Path(verify)).is_absolute() else me.parent / p).as_posix()
    headers = {'Accept-Encoding': 'identity' if args.no_compression else 'zstd'}
    resp = requests_get_or_exit(url, headers, verify)
    verbose and print(f"{Fore.YELLOW}{resp.headers}{Style.RESET_ALL}" if use_color else resp.headers, file=sys.stderr)
    prev_num = 0
    pattern_str = pattern_rx = None
    if use_color and pattern:
        init()  # colorama
        if is_probably_complex_pattern(pattern):
            pattern_rx = re.compile(pattern)
        else:
            pattern_str = RX_ESCAPE_FOLLOWED_BY_SPECIAL.sub('', pattern)
    header_open = False
    for resp_line in resp.iter_lines():
        if not resp_line:
            continue
        try:
            list_of_lists = json.loads(resp_line)
        except json.JSONDecodeError:
            print(resp_line, file=sys.stderr)
            print(sys.exc_info(), file=sys.stderr)
            continue
        if not isinstance(list_of_lists, list) or len(list_of_lists[0]) != 3:
            print(f"Invalid data format: {resp_line!r}", file=sys.stderr)
            continue
        for line_num, record_type, line in list_of_lists:
            if line_num == 0 and record_type == RecordType.log_path and settings.header_template:
                if header_open:
                    print_footer_if_required(settings, use_color)
                log_path = Path(line)
                msg = HeaderTemplate(settings.header_template).format(line_number=line_number, color=color, discard_before=discard_before, discard_after=discard_after, before_context=before_context, after_context=after_context, pattern=pattern, except_pattern=except_pattern, log_path=log_path, processor=template_processor)
                print(f"{Fore.LIGHTYELLOW_EX}{msg}{Style.RESET_ALL}" if use_color else f"{msg}")
                header_open = True
                prev_num = 0
            else:
                sep = ':' if record_type == RecordType.pattern else '-'
                if prev_num and prev_num + 1 != line_num:
                    if use_color:
                        print(f"{Fore.GREEN}--{Fore.RESET}")
                    else:
                        print('--')
                if use_color:
                    colored_num_sep = f"{Fore.GREEN}{line_num}{sep}{Style.RESET_ALL}" if line_number else ''
                    _line_ = make_colored_line(line, pattern_str, pattern_rx) if record_type == RecordType.pattern else line
                    print(f"{colored_num_sep}{_line_}")
                else:
                    num_sep = f"{line_num}{sep}" if line_number else ''
                    print(f"{num_sep}{line}")
                prev_num = line_num
    print_footer_if_required(settings, use_color)


def print_footer_if_required(settings: Settings, use_color: bool):
    if settings.footer_template:
        msg = settings.footer_template
        print(f"{Fore.LIGHTYELLOW_EX}{msg}{Style.RESET_ALL}" if use_color else f"{msg}")


def requests_get_or_exit(url: str, headers: dict | None = None, verify: str | bool | None = None) -> requests.Response:
    # print(f"GET {url}, headers={HEADERS}, verify={verify!r}", file=sys.stderr)
    try:
        resp = requests.get(url, headers=headers, verify=verify, stream=True)
    except requests.ConnectionError as e:
        et = type(e)
        print(f"{et.__module__}.{et.__qualname__}: {e}", file=sys.stderr)
        sys.exit(1)
    if resp.status_code != 200:
        print(resp.status_code, resp.reason, file=sys.stderr)
        print(resp.text, file=sys.stderr)
        sys.exit(1)
    return resp


def make_colored_line(line: str, pattern_str: str | None, pattern_rx: re.Pattern | None) -> str:
    if pattern_str:
        colored_line = line.replace(pattern_str, f"{Style.BRIGHT}{Fore.RED}{pattern_str}{Style.RESET_ALL}")
    elif pattern_rx:
        colored_line = ''.join(f"{Style.BRIGHT}{Fore.RED}{text}{Style.RESET_ALL}" if is_match else text for is_match, text in gen_segments_with_is_match(line, pattern_rx))
    else:
        colored_line = line
    return colored_line


def gen_segments_with_is_match(line: str, pattern: re.Pattern) -> Generator[tuple[bool, str], None, None]:
    """Decomposes a line based on a regex pattern into segments (is_match, text)"""
    # Find all matches of the pattern in the line
    current_pos = 0
    for match in pattern.finditer(line):
        # Add unmatched text before this match
        if match.start() > current_pos:
            yield False, line[current_pos:match.start()]
        # Add the matched text (find which group actually matched)
        matched_text = match.group(0)
        yield True, matched_text
        current_pos = match.end()
    # Add remaining unmatched text after the last match or if no match at all
    if current_pos < len(line):
        yield False, line[current_pos:]


if __name__ == '__main__':
    grep()
