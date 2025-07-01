#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import sys
import json
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

me = Path(__file__)
sys.path.insert(0, me.parent.as_posix())
from freeplane_remote_import_json import import_json
from sqlitelogger import Logger

with me.with_suffix('.toml').open('br') as fi:
    conf = tomllib.load(fi)

ACTION_TO_ICON = {
    'add': 'emoji-1F331',  # cross mark
    'remove': 'emoji-274C',  # seedling
}
ITER_EVENTS = '@iter-events'

issue_iter_evs_fetched = False

WORK_DIR = conf['work_dir']
LOG_SQLITE = conf['log']['sqlite_path']

GITLAB_URL = conf['gitlab']['url']
GITLAB_GRAPHQL_ENDPOINT = f"{GITLAB_URL}/api/graphql"
GITLAB_REST_ENDPOINT = f"{GITLAB_URL}/api/v4"
GITLAB_PRIVATE_TOKEN = conf['gitlab']['private_token']
PROJECT_FULL_PATH = conf['gitlab']['project_full_path']

AFTER_ISO = conf['gitlab']['after_iso']
BEFORE_ISO = conf['gitlab']['before_iso']
START_DATE_UTC = datetime.fromisoformat(AFTER_ISO).astimezone(timezone.utc)
END_DATE_UTC = datetime.fromisoformat(BEFORE_ISO).astimezone(timezone.utc)

workdir_path = Path(WORK_DIR)
log = Logger(Path(LOG_SQLITE))

epic_cache_json = workdir_path / 'epic_cache.json'
try:
    with epic_cache_json.open('r') as fi:
        epic_cache = json.load(fi)
except FileNotFoundError:
    epic_cache = {}

epic_to_ancestry = {}


def get_all_issues():
    cursor = None
    query = '''
    query($fullPath: ID!, $updateAfter: Time, $after: String) {
      project(fullPath: $fullPath) {
        issues(first: 100, updateAfter: $updateAfter, after: $after, sort: UPDATED_DESC) {
          pageInfo { hasNextPage endCursor }
          nodes {
            projectId
            id
            iid
            title
            closedAt
            labels(first: 10) { nodes { title } }
            epic {
              id
              iid
              group { fullPath }
            }
          }
        }
      }
    }
    '''
    # iteration { startDate dueDate }
    all_issues = []
    while True:
        variables = {
            'fullPath': PROJECT_FULL_PATH,
            'updatedAfter': AFTER_ISO,
            'after': cursor
        }
        data = run_graphql_query(query, variables)
        if not data:
            break
        issues = data['project']['issues']['nodes']
        all_issues += issues
        page_info = data['project']['issues']['pageInfo']
        if not page_info['hasNextPage']:
            break
        cursor = page_info['endCursor']
    return all_issues


def run_graphql_query(query, variables):
    log.debug(f"query: {query!r}")
    log.debug(f"variables: {variables!r}")
    headers = {'Authorization': f"Bearer {GITLAB_PRIVATE_TOKEN}"}
    response = session.post(
        GITLAB_GRAPHQL_ENDPOINT,
        json={'query': query, 'variables': variables},
        headers=headers
    )
    if response.status_code != 200:
        log.error(f"GraphQL query failed with status {response.status_code}")
        return None
    result = response.json()
    if 'errors' in result:
        log.error(f"GraphQL errors: {result['errors']}")
        log.error(query)
        log.error(variables)
        return None
    return result['data']


def get_freeplane_hierarchy(issues):
    global issue_iter_evs_fetched
    hierarchy = {}
    for issue in issues:
        if (iter_evs := issue.get(ITER_EVENTS)) is None:
            iter_evs = fetch_iteration_events_for_issue(issue['projectId'], issue['iid'])
            issue[ITER_EVENTS] = iter_evs
            issue_iter_evs_fetched = True
        iter_evs_in_range = filter_iteration_events_to_range_and_repackage(iter_evs, START_DATE_UTC, END_DATE_UTC)
        if not iter_evs_in_range:
            continue
        epic_chain = []
        if issue.get('epic'):
            epic_chain = get_epic_ancestry(issue['epic']['group']['fullPath'], issue['epic']['iid'], issue['epic']['id'])
        gid = urlparse(issue['id'])
        _id = int(Path(gid.path).name)
        issue_node = {
            'id': _id,
            'iid': issue['iid'],
            'title': issue['title'],
            'labels': [l['title'] for l in issue['labels']['nodes']],
            'project_id': issue['projectId'],
            'closedAt': issue['closedAt'],
            'iteration_events': iter_evs_in_range,
        }
        insert_into_hierarchy(hierarchy, epic_chain, issue_node)
    return hierarchy


def fetch_iteration_events_for_issue(projectId, issue_iid):
    log.debug(f"fetch_iteration_events_for_issue({projectId}, {issue_iid}")
    url = f"{GITLAB_REST_ENDPOINT}/projects/{projectId}/issues/{issue_iid}/resource_iteration_events"
    headers = {'Authorization': f"Bearer {GITLAB_PRIVATE_TOKEN}"}
    resp = session.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def filter_iteration_events_to_range_and_repackage(iteration_events, start, end):
    filtered_events = []
    for iter_ev in iteration_events:
        itr = iter_ev.get('iteration')
        if itr and is_iteration_in_range(itr, start, end):
            filtered_events.append(dict(
                created_at=iter_ev['created_at'],
                action=iter_ev['action'],
                start_date=itr['start_date'],
                due_date=itr['due_date'],
            ))
    return filtered_events


def is_iteration_in_range(iteration, start, end):
    start_date = datetime.fromisoformat(iteration['start_date']).astimezone(timezone.utc)
    return start <= start_date <= end


def get_epic_ancestry(group_path, epic_iid, epic_id):
    log.debug(f"get_epic_ancestry({group_path}, {epic_iid}, {epic_id})")
    if ancestry := epic_to_ancestry.get(epic_id):
        return ancestry
    ancestry = []
    while True:
        cache_key = epic_id
        if cache_key in epic_cache:
            epic = epic_cache[cache_key]
            # log.info(f"Cache hit: {cache_key}")
        else:
            query = '''
            query($fullPath: ID!, $epicIid: ID!) {
              group(fullPath: $fullPath) {
                epic(iid: $epicIid) {
                  id
                  iid
                  title
                  closedAt
                  labels(first: 20) { nodes { title } }
                  group { id fullPath }
                  parent {
                    id
                    iid
                    group { fullPath }
                  }
                }
              }
            }
            '''
            variables = {'fullPath': group_path, 'epicIid': epic_iid}
            data = run_graphql_query(query, variables)
            epic_data = data.get('group', {}).get('epic')
            if not epic_data:
                log.error(f"Epic not found: {cache_key}")
                break
            gid = urlparse(epic_data['id'])
            id_ = int(Path(gid.path).name)
            gid = urlparse(epic_data['group']['id'])
            group_id_ = int(Path(gid.path).name)
            epic = {
                'id': id_,
                'iid': epic_data['iid'],
                'closedAt': epic_data['closedAt'],
                'title': epic_data['title'],
                'group_id': group_id_,
                'group_path': epic_data['group']['fullPath'],
                'labels': [label['title'] for label in epic_data.get('labels', {}).get('nodes', [])],
                'parent_id': epic_data['parent']['id'] if epic_data['parent'] else None,
                'parent_iid': epic_data['parent']['iid'] if epic_data['parent'] else None,
                'parent_group_path': epic_data['parent']['group']['fullPath'] if epic_data['parent'] and epic_data['parent']['group'] else None
            }
            epic_cache[cache_key] = epic
            # log.info(f"Cached epic: {cache_key}")

        ancestry.insert(0, epic)  # Build from root to leaf
        epic_id = epic['parent_id']
        epic_iid = epic['parent_iid']
        group_path = epic['parent_group_path']
        if not epic_id or not epic_iid or not group_path:
            break
    epic_to_ancestry[epic_id] = ancestry
    return ancestry


def insert_into_hierarchy(hierarchy, ancestry, issue_node):
    current = hierarchy
    for epic in ancestry:
        epic_id = str(epic['id'])
        if epic_id not in current:
            current[epic_id] = {
                '@core': f"&{epic['iid']} {epic['title']}",
                '@attributes': {
                    'group_path': epic['group_path'],
                    'group_id': epic['group_id'],
                    'preStashTags': json.dumps(epic['labels']),
                }
            }
            if closed_at := epic.get('closedAt', ''):
                closed_at_dt = datetime.fromisoformat(closed_at)
                current[epic_id]['@attributes']['closedAt'] = closed_at_dt.astimezone().strftime('%Y-%m-%d %H:%M:%S')
                style_name = '!NextAction.Closed' if closed_at_dt < END_DATE_UTC else '!WaitingFor.Closed'
                current[epic_id]['@style'] = {'name': style_name}
        current = current[epic_id]

    issue_id = str(issue_node['id'])
    iter_evs = issue_node['iteration_events']
    current[issue_id] = {
        '@core': f"#{issue_node['iid']} {issue_node['title']}",
        '@attributes': {
            'project_id': int(issue_node['project_id']),
            'preStashTags': json.dumps(issue_node['labels']),
        },
    }
    if closed_at := issue_node.get('closedAt', ''):
        closed_at_dt = datetime.fromisoformat(closed_at)
        current[issue_id]['@attributes']['closedAt'] = closed_at_dt.astimezone().strftime('%Y-%m-%d %H:%M:%S')
        style_name = '!NextAction.Closed' if closed_at_dt < END_DATE_UTC else '!WaitingFor.Closed'
        current[issue_id]['@style'] = {'name': style_name}
    # iteration events as issue children in Freeplane
    current[issue_id] |= {
        f"{iev['start_date']} - {iev['due_date']}": {
            '@icons': [ACTION_TO_ICON.get(iev['action'])],
            '@attributes': {
                'created_at': iev['created_at'],
                'action': iev['action'],
            }
        } for iev in iter_evs
    }
    # fold children (iteration events)
    current[issue_id]['@props'] = {'folded': True}


def main():
    log.info('Start query')
    issue_cache_json = workdir_path / 'issue_cache.json'
    try:
        with issue_cache_json.open('r') as fi:
            issues = json.load(fi)
    except FileNotFoundError:
        issues = get_all_issues()
        with issue_cache_json.open('w') as fo:
            json.dump(issues, fo, indent=2)
    freeplane_hierarchy = get_freeplane_hierarchy(issues)
    if epic_cache:
        with epic_cache_json.open('w') as fo:
            json.dump(epic_cache, fo, indent=2)
    if issue_iter_evs_fetched:
        with issue_cache_json.open('w') as fo:
            json.dump(issues, fo, indent=2)
    gitlab_export_freeplane_json = workdir_path / 'gitlab-export-freeplane.json'
    log.info(f"Save to {gitlab_export_freeplane_json}")
    with gitlab_export_freeplane_json.open('w') as fo:
        json.dump(freeplane_hierarchy, fo, indent=2)
    result = import_json(gitlab_export_freeplane_json)
    if result:
        log.info(f"Import result: {result}")


if __name__ == '__main__':
    session = requests.session()
    main()
