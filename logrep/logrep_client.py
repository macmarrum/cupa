#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import argparse
import re
import sys
from urllib.parse import quote

import requests
from colorama import init, Fore, Style

# Note: special chars could be either escaped or bracketed [] to make them literal
# Bracketing is not accounter for here, hence "possibly"
RX_POSSIBLY_COMPLEX_PATTERN = re.compile(r'(?<!\\)[()\[\]{}.*+?^$|]')


def is_possibly_complex_pattern(pattern: str):
    return RX_POSSIBLY_COMPLEX_PATTERN.search(pattern) is not None


def grep(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('url')
    pattern_gr = parser.add_mutually_exclusive_group()
    pattern_gr.add_argument('pattern_positional', nargs='?', )
    pattern_gr.add_argument('-P', '--pattern', )
    parser.add_argument('-A', '--after-context', default=-1, type=int, )
    parser.add_argument('-p', '--profile')
    parser.add_argument('-n', '--line-number', action='store_true')
    parser.add_argument('--color', choices=['auto', 'always', 'never'], nargs='?', const='auto')
    args = parser.parse_args(argv)
    _after_context = f"&after_context={args.after_context}" if args.after_context != -1 else ''
    _profile = f"&profile={quote(args.profile)}" if args.profile else ''
    pattern_str = args.pattern_positional or args.pattern
    url = f"{args.url.rstrip('/')}/search?pattern={quote(pattern_str)}{_after_context}{_profile}"
    print(url)
    resp = requests.get(url, headers={'Accept-Encoding': 'zstd, br, gzip, deflate'})
    if resp.status_code != 200:
        print(resp.status_code, resp.reason, file=sys.stderr)
        print(resp.text, file=sys.stderr)
        return
    # print(resp.headers)
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
        use_color = args.color == 'always' or (args.color == 'auto' and sys.stdout.isatty())
        if use_color and is_possibly_complex_pattern(pattern_str):
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
                    colored_line = pattern.sub(lambda m: f"{Fore.RED}{m[0]}{Style.RESET_ALL}", line)
                else:
                    colored_line = line.replace(pattern_str, f"{Fore.RED}{pattern_str}{Style.RESET_ALL}")
                colored_num_sep = f"{Fore.GREEN}{num:{size}d}{sep}{Style.RESET_ALL}" if args.line_number else ''
                print(f"{colored_num_sep}{colored_line}")
            else:
                num_sep = f"{num:{size}d}{sep}" if args.line_number else ''
                print(f"{num_sep}{line}")
            prev_num = num
    else:
        print(d.get('details'))


if __name__ == '__main__':
    grep()
