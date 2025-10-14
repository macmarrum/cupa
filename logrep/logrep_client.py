#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import argparse
import sys

import requests


def grep(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('url')
    parser.add_argument('pattern')
    parser.add_argument('-A', '--after-context', default=-1, type=int, )
    parser.add_argument('-p', '--profile')
    args = parser.parse_args(argv)
    _after_context = f"&after_context={args.after_context}" if args.after_context != -1 else ''
    _profile = f"&profile={args.profile}" if args.profile else ''
    url = f"{args.url.rstrip('/')}/search?pattern={args.pattern}{_after_context}{_profile}"
    print(url)
    resp = requests.get(url, headers={'Accept-Encoding': 'zstd, br, gzip, deflate'})
    if resp.status_code != 200:
        print(resp.status_code, resp.reason, file=sys.stderr)
        print(resp.text, file=sys.stderr)
        return
    print(resp.headers)
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
        for num, line in matches:
            if prev_num and prev_num + 1 != num:
                print('--')
            print(f"{num:{size}d}:{line}")
            prev_num = num
    else:
        print(d.get('details'))


if __name__ == '__main__':
    grep()
