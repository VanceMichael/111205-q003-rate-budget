"""Offline fixtures and deterministic partner for the export flow.

The fixture in ``fixtures/exports.json`` is synthetic.  Everything here runs
on caller-supplied virtual time; no sleep, HTTP call or wall clock is used.

``ScenarioAPI`` complements ``export_api.ExportAPI``: it is in-memory so a
scenario can express partner-side failures, a ready notification that precedes
file availability, a report cancelled at the partner, and a late file whose
columns are in a different order.
"""

import json
from copy import deepcopy
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


FIXTURE_PATH = Path(__file__).resolve().parent / 'fixtures' / 'exports.json'


def load_fixture(path=None):
    with open(path or FIXTURE_PATH, encoding='utf-8') as handle:
        return json.load(handle)


class ScenarioAPI:
    """Deterministic partner protocol implementation.

    ``ready_notification_at`` allows the "ready" notification to arrive before
    members are downloadable, which is the case the collection must not treat
    as a complete report.  ``expire_member_call`` makes a specific download
    attempt observe an expired address even when its URL still says otherwise,
    which models a partner that invalidates addresses server-side.
    """

    def __init__(self, parts, ready_after=5, member_available_after=None,
                 link_ttl=3, fail_at=None, cancel_at=None,
                 expire_member_call=None, ready_notification_at=None):
        self.parts = deepcopy(parts)
        self.ready_after = ready_after
        self.member_available_after = dict(member_available_after or {})
        self.link_ttl = link_ttl
        self.fail_at = fail_at
        self.cancel_at = cancel_at
        self.expire_member_call = dict(expire_member_call or {})
        self.ready_notification_at = ready_notification_at
        self.jobs = {}
        self.apply_calls = 0
        self.download_calls = 0
        self.link_calls = 0
        self.calls = []

    # -- partner protocol ----------------------------------------------------
    def apply(self, tenant, filters, now, intent_key=None):
        self.apply_calls += 1
        if intent_key is not None:
            for job, record in self.jobs.items():
                if record.get('intent_key') == intent_key:
                    self.calls.append({'at': now, 'call': 'apply', 'job': job, 'idempotent': True})
                    return job
        job = 'E%d' % (len(self.jobs) + 1)
        self.jobs[job] = {
            'tenant': tenant, 'filters': deepcopy(filters), 'created_at': now,
            'intent_key': intent_key, 'cancelled': False,
        }
        self.calls.append({'at': now, 'call': 'apply', 'job': job, 'idempotent': False})
        return job

    def status(self, job, now):
        record = self.jobs[job]
        self.calls.append({'at': now, 'call': 'status', 'job': job})
        if record['cancelled'] or (self.cancel_at is not None and now >= self.cancel_at):
            return {'job': job, 'state': 'cancelled', 'members': []}
        if self.fail_at is not None and now >= self.fail_at:
            return {'job': job, 'state': 'failed', 'members': []}
        ready_at = record['created_at'] + self.ready_after
        if self.ready_notification_at is not None:
            ready_at = max(ready_at, self.ready_notification_at)
        if now < ready_at:
            return {'job': job, 'state': 'generating', 'members': []}
        return {
            'job': job,
            'state': 'ready',
            'members': [{'name': part['name'], 'rows': len(part['rows'])} for part in self.parts],
        }

    def links(self, job, now):
        self.link_calls += 1
        self.calls.append({'at': now, 'call': 'links', 'job': job})
        if self.status(job, now)['state'] != 'ready':
            raise ValueError('export is not ready')
        return [f'local://{job}/{part["name"]}?expires={now + self.link_ttl}' for part in self.parts]

    def download(self, link, now):
        parsed = urlsplit(link)
        job = parsed.netloc
        self.download_calls += 1
        self.calls.append({'at': now, 'call': 'download', 'job': job})
        if job not in self.jobs:
            raise ValueError('unknown export job')
        if now >= int(parse_qs(parsed.query)['expires'][0]):
            raise TimeoutError('download link expired')
        state = self.status(job, now)['state']
        if state == 'cancelled':
            raise ValueError('export cancelled')
        if state != 'ready':
            raise ValueError('export unavailable')
        name = parsed.path.lstrip('/')
        index = next((i for i, part in enumerate(self.parts) if part['name'] == name), None)
        if index is None:
            raise ValueError('member %s is not part of export %s' % (name, job))
        record = self.jobs[job]
        available_at = record['created_at'] + self.ready_after + index
        available_at += self.member_available_after.get(name, 0)
        if now < available_at:
            raise FileNotFoundError('member not downloadable yet')
        expiry_call = self.expire_member_call.get(name)
        if expiry_call is not None and self.download_calls >= expiry_call:
            raise TimeoutError('download link expired')
        part = deepcopy(self.parts[index])
        part['job'] = job
        return part

    def cancel(self, job):
        self.jobs[job]['cancelled'] = True
        self.calls.append({'call': 'cancel', 'job': job})

    def report_failed(self, job):
        self.fail_at = 0

    def report_cancelled(self, job):
        self.jobs[job]['cancelled'] = True


def fixture_scenario(member_available_after=None, **overrides):
    """Build a ScenarioAPI from fixtures/exports.json.

    The fixture itself already declares members whose rows have different
    key orders, so column-order handling is exercised without extra setup.
    """
    fixture = load_fixture()
    options = {
        'ready_after': fixture['ready_at'],
        'link_ttl': fixture['link_ttl'],
        'member_available_after': member_available_after,
    }
    options.update(overrides)
    return fixture, ScenarioAPI(fixture['parts'], **options)
