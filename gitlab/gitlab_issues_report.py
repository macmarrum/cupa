#!/usr/bin/python3
# Copyright (C) 2025  macmarrum (at) outlook (dot) ie
# SPDX-License-Identifier: GPL-3.0-or-later
import os
import sys
import json
import tomllib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

me = Path(__file__)

with me.with_suffix('.toml').open('br') as fi:
    conf = tomllib.load(fi)

for import_path in conf['import_paths']:
    sys.path.insert(0, import_path)
from freeplane_remote_import_json import import_json
from sqlite_logger import Logger

WORK_DIR = conf['work_dir']
LOG_SQLITE = conf['log']['sqlite_path']

GITLAB_URL = conf['gitlab']['url']
GITLAB_GRAPHQL_ENDPOINT = f"{GITLAB_URL}/api/graphql"
GITLAB_REST_ENDPOINT = f"{GITLAB_URL}/api/v4"
GITLAB_PRIVATE_TOKEN = conf['gitlab']['private_token']
PROJECT_FULL_PATH = conf['gitlab']['project_full_path']
GROUP_FULL_PATH = conf['gitlab']['group_full_path']

AFTER_ISO = conf['gitlab']['after_iso']
BEFORE_ISO = conf['gitlab']['before_iso']
START_DATE_UTC = datetime.fromisoformat(AFTER_ISO).astimezone(timezone.utc)
END_DATE_UTC = datetime.fromisoformat(BEFORE_ISO).astimezone(timezone.utc)

if conf.get('requests_ca_bundle'):
    os.environ['REQUESTS_CA_BUNDLE'] = conf['requests_ca_bundle']

workdir_path = Path(WORK_DIR)
log = Logger(Path(LOG_SQLITE))

issue_cache_json = workdir_path / 'issue_cache.json'
epic_cache_json = workdir_path / 'epic_cache.json'
epic_cache = {}
epic_to_ancestry = {}
issue_itr_events_fetched = False

ACTION_TO_ICON = {
    'add': 'emoji-2728',  # sparkles
    'remove': 'emoji-274C',  # cross mark
}
FALLBACK_ACTION_ICON = 'emoji-1F33B'  # sunflower
ISSUE_ICON = 'emoji-2139'
ITER_EVENTS = '@iter-events'


class f:
    ATTRIBUTES = '@attributes'
    CORE = '@core'
    DETAILS = '@details'
    ICONS = '@icons'
    NOTE = '@note'
    PROPS = '@props'
    STYLE = '@style'
    folded = 'folded'
    iteration_events = 'iteration events'
    comments = 'comments'
    detailsContentType = 'detailsContentType'
    noteContentType = 'noteContentType'
    markdown = 'markdown'
    minimized = 'minimized'


class q:
    issues_updated_after = '''
    query($fullPath: ID!, $updatedAfter: Time, $after: String) {
      project(fullPath: $fullPath) {
        issues(first: 100, updatedAfter: $updatedAfter, after: $after, sort: UPDATED_DESC) {
          pageInfo { hasNextPage endCursor }
          nodes {
            projectId
            id
            iid
            title
            closedAt
            labels(first: 10) { nodes { title } }
            assignees { nodes { name } }
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
    epic_with_parent = '''
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
    issues_for_iterations = '''
    query($fullPath: ID!, $iterationId: [ID!], $first: Int = 100) {
      project(fullPath: $fullPath) {
        issues(iterationId: $iterationId, first: $first, sort: CREATED_ASC) {
          nodes {
            projectId
            id
            iid
            title
            closedAt
            description
            iteration { startDate dueDate }
            labels {
              nodes {
                title
              }
            }
            assignees {
              nodes {
                username
                name
              }
            }
            notes {
              nodes {
                id
                body
                author {
                  name
                }
                createdAt
              }
            }
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
    iterations_for_cadences_sorted_by_due_date_desc = '''
    query($fullPath: ID!, $cadenceId: [IterationsCadenceID!], $first: Int = 5) {
      group(fullPath: $fullPath) {
        iterations(iterationCadenceIds: $cadenceId, sort: CADENCE_AND_DUE_DATE_DESC, first: $first) {
          nodes {
            id
            startDate
            dueDate
          }
        }
      }
    }
    '''
    cadences = '''
    query($fullPath: ID!) {
      group(fullPath: $fullPath) {
        iterationCadences {
          nodes {
            id
            title
          }
        }
      }
    }
    '''


class DictLike:
    def __getitem__(self, key):
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(f"{key} not found in {self.__class__.__name__}")


@dataclass(frozen=True)
class NoteRecord(DictLike):
    id: str
    body: str
    author_name: str
    createdAt: str

    @staticmethod
    def of(note_node):
        note_rec = NoteRecord(
            id=note_node['id'],
            body=note_node['body'],
            author_name=note_node['author']['name'],
            createdAt=note_node['createdAt'],
        )
        return note_rec


@dataclass(frozen=True)
class IterationEventRecord(DictLike):
    id: str
    user_name: str
    created_at: str
    action: str
    start_date: str
    due_date: str

    @staticmethod
    def of(itr_event):
        itr_event_rec = IterationEventRecord(
            id=itr_event['id'],
            user_name=itr_event['user']['name'],
            created_at=itr_event['created_at'],
            action=itr_event['action'],
            start_date=itr_event['iteration']['start_date'],
            due_date=itr_event['iteration']['due_date'],
        )
        return itr_event_rec


@dataclass(frozen=True)
class IssueRecord(DictLike):
    id: str
    iid: str
    title: str
    description: str
    labels: list[str]
    project_id: int
    closedAt: str
    assignees: list[str]
    iteration_events: list[IterationEventRecord]
    notes: list[NoteRecord]

    @staticmethod
    def of(issue_node, iteration_event_recs: list[IterationEventRecord], note_recs: list[NoteRecord] = None):
        gid = urlparse(issue_node['id'])
        _id = Path(gid.path).name
        issue_rec = IssueRecord(
            id=_id,
            iid=issue_node['iid'],
            title=issue_node['title'],
            description=issue_node.get('description'),
            labels=[l['title'] for l in issue_node['labels']['nodes']],
            project_id=issue_node['projectId'],
            closedAt=issue_node['closedAt'],
            assignees=[node['name'] for node in issue_node['assignees']['nodes']],
            iteration_events=iteration_event_recs,
            notes=note_recs or [],
        )
        return issue_rec


@dataclass(frozen=True)
class EpicRecord(DictLike):
    gid: str
    iid: str
    closedAt: str
    title: str
    group_id: int
    group_path: str
    labels: list[str]
    parent_gid: str
    parent_iid: str
    parent_group_path: str

    @staticmethod
    def of(epic_node):
        gid = urlparse(epic_node['group']['id'])
        group_id_ = int(Path(gid.path).name)
        epic_rec = EpicRecord(
            gid=epic_node['id'],
            iid=epic_node['iid'],
            closedAt=epic_node['closedAt'],
            title=epic_node['title'],
            group_id=group_id_,
            group_path=epic_node['group']['fullPath'],
            labels=[label['title'] for label in epic_node.get('labels', {}).get('nodes', [])],
            parent_gid=epic_node['parent']['id'] if epic_node['parent'] else None,
            parent_iid=epic_node['parent']['iid'] if epic_node['parent'] else None,
            parent_group_path=epic_node['parent']['group']['fullPath'] if epic_node['parent'] and epic_node['parent']['group'] else None
        )
        return epic_rec


def main():
    global epic_cache
    log.info('Start main')
    try:
        with epic_cache_json.open('r') as fi:
            epic_cache = json.load(fi)
    except FileNotFoundError:
        pass
    # create_fp_report_of_issues_with_ancestry_for_period()
    create_fp_report_of_issues_for_iterations()


def create_fp_report_of_issues_with_ancestry_for_period():
    global issue_itr_events_fetched
    try:
        with issue_cache_json.open('r') as fi:
            issue_nodes = json.load(fi)
    except FileNotFoundError:
        issue_nodes = fetch_issues_updated_after()
        with issue_cache_json.open('w') as fo:
            json.dump(issue_nodes, fo, indent=2)
    freeplane_hierarchy = {}
    for issue_node in issue_nodes:
        if epic := issue_node.get('epic'):
            epic_rec_ancestry = build_epic_rec_ancestry(epic['group']['fullPath'], epic['iid'], epic['id'])
        else:
            epic_rec_ancestry = []
        if (itr_events := issue_node.get(ITER_EVENTS)) is None:
            itr_events = fetch_iteration_events_for_issue(issue_node['projectId'], issue_node['iid'])
            issue_node[ITER_EVENTS] = itr_events
            issue_itr_events_fetched = True
        itr_event_recs_in_range = filter_itr_events_to_range_and_repackage(itr_events, START_DATE_UTC, END_DATE_UTC)
        issue_rec = IssueRecord.of(issue_node, itr_event_recs_in_range)
        insert_into_freeplane_json_dct(freeplane_hierarchy, epic_rec_ancestry, issue_rec)
    if epic_cache:
        with epic_cache_json.open('w') as fo:
            json.dump(epic_cache, fo, indent=2)
    if issue_itr_events_fetched:
        with issue_cache_json.open('w') as fo:
            json.dump(issue_nodes, fo, indent=2)
    gitlab_export_freeplane_json = workdir_path / 'gitlab-export-freeplane.json'
    log.info(f"Save to {gitlab_export_freeplane_json}")
    dump_json_to_disk_and_import_to_freeplane(freeplane_hierarchy, gitlab_export_freeplane_json)


def fetch_issues_updated_after(updated_after: str = None, project_full_path: str = None):
    updated_after = updated_after or AFTER_ISO
    project_full_path = project_full_path or PROJECT_FULL_PATH
    cursor = None
    all_issues = []
    while True:
        variables = {
            'fullPath': project_full_path,
            'updatedAfter': updated_after,
            'after': cursor
        }
        data = run_graphql_query(q.issues_updated_after, variables)
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
    # log.debug(f"query: {query!r} | variables: {variables!r}")
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
        log.error(str(variables))
        return None
    return result['data']


def build_epic_rec_ancestry(group_path, epic_iid, epic_gid):
    log.debug(f"build_epic_rec_ancestry({group_path}, {epic_iid}, {epic_gid})")
    if epic_rec_ancestry := epic_to_ancestry.get(epic_gid):
        return epic_rec_ancestry
    epic_rec_ancestry: list[EpicRecord] = []
    while True:
        cache_key = epic_gid
        if cache_key in epic_cache:
            epic_rec = epic_cache[cache_key]
        else:
            variables = {'fullPath': group_path, 'epicIid': epic_iid}
            data = run_graphql_query(q.epic_with_parent, variables)
            epic_node = data.get('group', {}).get('epic')
            if not epic_node:
                log.error(f"Epic not found: {cache_key}")
                break
            epic_rec = EpicRecord.of(epic_node)
            epic_cache[cache_key] = epic_rec
        epic_rec_ancestry.insert(0, epic_rec)  # Build from root to leaf
        epic_gid = epic_rec['parent_gid']
        epic_iid = epic_rec['parent_iid']
        group_path = epic_rec['parent_group_path']
        if not epic_gid or not epic_iid or not group_path:
            break
    epic_to_ancestry[epic_gid] = epic_rec_ancestry
    return epic_rec_ancestry


def fetch_iteration_events_for_issue(project_id, issue_iid):
    log.debug(f"fetch_iteration_events_for_issue({project_id}, {issue_iid})")
    url = f"{GITLAB_REST_ENDPOINT}/projects/{project_id}/issues/{issue_iid}/resource_iteration_events"
    headers = {'Authorization': f"Bearer {GITLAB_PRIVATE_TOKEN}"}
    resp = session.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def filter_itr_events_to_range_and_repackage(iteration_events, start, end):
    filtered_event_recs: list[IterationEventRecord] = []
    for itr_event in iteration_events:
        itr = itr_event.get('iteration')
        if itr and is_iteration_in_range(itr, start, end):
            iter_event_rec = IterationEventRecord.of(itr_event)
            filtered_event_recs.append(iter_event_rec)
    return filtered_event_recs


def is_iteration_in_range(iteration, start, end):
    start_date = datetime.fromisoformat(iteration['start_date']).astimezone(timezone.utc)
    return start <= start_date <= end


def insert_into_freeplane_json_dct(freeplane_hierarchy, epic_rec_ancestry_chain: list[EpicRecord], issue_rec: IssueRecord):
    current = freeplane_hierarchy
    for epic_rec in epic_rec_ancestry_chain:
        epic_id = epic_rec['gid']
        if epic_id not in current:
            current[epic_id] = {
                f.CORE: f"&{epic_rec['iid']} {epic_rec['title']}",
                f.ATTRIBUTES: {
                    'group_path': epic_rec['group_path'],
                    'group_id': epic_rec['group_id'],
                    'preStashTags': json.dumps(epic_rec['labels']),
                }
            }
            if closed_at := epic_rec['closedAt']:
                closed_at_dt = datetime.fromisoformat(closed_at)
                current[epic_id][f.ATTRIBUTES]['closedAt'] = format_date(closed_at_dt)
                style_name = '!NextAction.Closed' if closed_at_dt < END_DATE_UTC else '!WaitingFor.Closed'
                current[epic_id][f.STYLE] = {'name': style_name}
        current = current[epic_id]
    issue_id = issue_rec['id']
    current[issue_id] = {
        f.CORE: f"#{issue_rec['iid']} {issue_rec['title']}",
        f.DETAILS: issue_rec['description'],
        f.ICONS: [ISSUE_ICON],
        f.ATTRIBUTES: {
            'assignees': json.dumps(issue_rec['assignees']),
            # 'project_id': int(issue_node['project_id']),
            'preStashTags': json.dumps(issue_rec['labels']),
        },
        f.comments: {},
        f.iteration_events: {},
    }
    if closed_at := issue_rec['closedAt']:
        closed_at_dt = datetime.fromisoformat(closed_at)
        current[issue_id][f.ATTRIBUTES]['closedAt'] = format_date(closed_at_dt)
        style_name = '!NextAction.Closed' if closed_at_dt < END_DATE_UTC else '!WaitingFor.Closed'
        current[issue_id][f.STYLE] = {'name': style_name}
    # notes aka comments
    current[issue_id][f.comments] |= {
        f"{nt['id']}": {
            f.CORE: f"{format_date(nt['createdAt'])} | {nt['author_name']}",
            f.DETAILS: nt['body'],
            f.PROPS: {f.detailsContentType: f.markdown, f.minimized: True}
        } for nt in issue_rec['notes']
    }
    # fold children of notes
    current[issue_id][f.comments][f.PROPS] = {f.folded: True}
    # iteration events
    current[issue_id][f.iteration_events] |= {
        f"{iev['id']}": {
            f.CORE: f"{iev['start_date']} - {iev['due_date']}",
            f.ICONS: [ACTION_TO_ICON.get(iev['action'], FALLBACK_ACTION_ICON)],
            f.ATTRIBUTES: {
                'user': iev['user_name'],
                'created_at': format_date(iev['created_at']),
                'action': iev['action'],
            }
        } for iev in issue_rec['iteration_events']
    }
    # fold children of iteration events
    current[issue_id][f.iteration_events][f.PROPS] = {f.folded: True}
    # issue properties
    current[issue_id][f.PROPS] = {f.detailsContentType: f.markdown, f.minimized: True if issue_rec['description'] else False, f.folded: True}


def format_date(date_or_str: datetime | str) -> str:
    try:
        dt = datetime.fromisoformat(date_or_str) if isinstance(date_or_str, str) else date_or_str
        return dt.astimezone().strftime('%Y-%m-%d %H:%M:%S %z')
    except (ValueError, TypeError) as e:
        log.error(f"Date formatting error: {e}")
        return str(date_or_str)


def dump_json_to_disk_and_import_to_freeplane(freeplane_hierarchy, export_json):
    with export_json.open('w') as fo:
        json.dump(freeplane_hierarchy, fo, indent=2)
    result = import_json(export_json)
    if result:
        log.info(f"Import result: {result}")


def create_fp_report_of_issues_for_iterations(iteration_gids: list[str] = None, project_full_path: str = None):
    issue_nodes = fetch_issues_for_iterations(iteration_gids, project_full_path)
    for issue_node in issue_nodes:
        itr_event_recs = fetch_iteration_events_for_issue(issue_node['projectId'], issue_node['iid'])
        issue_node[ITER_EVENTS] = itr_event_recs
    issues_for_iterations_json = workdir_path / 'issues_for_iterations.json'
    with issues_for_iterations_json.open('w') as fo:
        json.dump(issue_nodes, fo, indent=2)
    freeplane_json_dct = {}
    for issue_node in issue_nodes:
        if issue_node.get('epic'):
            epic_gid = issue_node['epic']['id']
            if not (epic_node := epic_cache.get(epic_gid)):
                data = run_graphql_query(q.epic_with_parent, {'fullPath': issue_node['epic']['group']['fullPath'], 'epicIid': issue_node['epic']['iid']})
                epic_node = data.get('group', {}).get('epic')
                epic_cache[epic_gid] = epic_node
            epic_rec_ancestry = [EpicRecord.of(epic_node)]
        else:
            epic_rec_ancestry = []
        itr_event_recs = [IterationEventRecord.of(itr_event) for itr_event in issue_node[ITER_EVENTS]]
        note_recs = [NoteRecord.of(note_node) for note_node in issue_node['notes']['nodes']]
        issue_rec = IssueRecord.of(issue_node, itr_event_recs, note_recs)
        insert_into_freeplane_json_dct(freeplane_json_dct, epic_rec_ancestry, issue_rec)
    gitlab_export_freeplane_json = workdir_path / 'gitlab-export-freeplane.json'
    dump_json_to_disk_and_import_to_freeplane(freeplane_json_dct, gitlab_export_freeplane_json)


def fetch_issues_for_iterations(iteration_gids: list[str] = None, project_full_path: str = None):
    iteration_gids = iteration_gids or [fetch_current_iteration()['id']]
    project_full_path = project_full_path or PROJECT_FULL_PATH
    variables = {'fullPath': project_full_path, 'iterationId': iteration_gids}
    data = run_graphql_query(q.issues_for_iterations, variables)
    issues = data['project']['issues']['nodes']
    return issues


def fetch_current_iteration():
    iterations = fetch_iterations_sorted_by_due_date_desc()
    for iteration in iterations:
        start_date = datetime.fromisoformat(iteration['startDate'])
        due_date = datetime.fromisoformat(iteration['dueDate'])
        if start_date <= datetime.now() <= due_date:
            return iteration
    return None


def fetch_iterations_sorted_by_due_date_desc(cadence_gids: list = None):
    variables = {'fullPath': GROUP_FULL_PATH}
    if cadence_gids:
        variables['cadenceId'] = cadence_gids
    data = run_graphql_query(q.iterations_for_cadences_sorted_by_due_date_desc, variables)
    iterations = data['group']['iterations']['nodes']
    return iterations


def fetch_cadences():
    variables = {'fullPath': GROUP_FULL_PATH}
    data = run_graphql_query(q.cadences, variables)
    cadences = data['group']['iterationCadences']['nodes']
    return cadences


if __name__ == '__main__':
    with requests.session() as session:
        main()
