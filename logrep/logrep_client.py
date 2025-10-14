#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import argparse

import requests


def grep(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('url')
    parser.add_argument('pattern')
    parser.add_argument('-A', '--after-context', default=-1, type=int, )
    args = parser.parse_args(argv)
    _after_context = f"&after_context={args.after_context}" if args.after_context != -1 else ''
    url = f"{args.url.rstrip('/')}/search?pattern={args.pattern}{_after_context}"
    resp = requests.get(url, headers={'Accept-Encoding': 'zstd, br, gzip, deflate'})
    resp.raise_for_status()
    print(resp.headers)
    d = resp.json()
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
