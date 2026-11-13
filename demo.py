"""Offline demonstration of the asynchronous export collection.

Everything runs on virtual time supplied by this module; no real report is
generated and no wall clock or network is used.  The output is deterministic,
so tests can compare repeated runs and the CLI output with the in-process
result.
"""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import export_flow
import export_sim
import export_store
from export_api import ExportAPI
from fake_api import PartnerAPI
from service import eligible


def legacy():
    fixture = json.loads((Path(__file__).parent / 'fixtures/partner.json').read_text())
    partner = PartnerAPI(fixture['responses'])
    first = partner.request(fixture['credential'], fixture['tenant'], fixture['endpoint'], 0)
    second = partner.request(fixture['credential'], fixture['tenant'], fixture['endpoint'],
                             first['retry_after'])
    assert first['status'] == 429 and second['status'] == 200
    assert eligible(0, 5, 6) and not eligible(0, 7, 6)
    export = json.loads((Path(__file__).parent / 'fixtures/exports.json').read_text())
    with TemporaryDirectory() as directory:
        api = ExportAPI(Path(directory) / 'exports.json', export['parts'])
        job = api.apply(export['intent']['tenant'], export['intent']['filters'], 0)
        generating = api.status(job, 4)
        ready = api.status(job, 6)
        member = api.download(api.links(job, 6)[0], 6)
        assert generating['state'] == 'generating' and ready['state'] == 'ready'
    return {
        'legacy_responses': [first, second],
        'export_before_ready': generating,
        'export_ready': ready,
        'downloaded_member': member,
    }


def _round_events(store, intent_id):
    """Compact the audit log enough to be readable in the CLI."""
    return [
        {'at': event['at'], 'kind': event['kind'], 'detail': event['detail']}
        for event in store.events(intent_id)
        if event['kind'] in ('poll', 'members_declared', 'links_updated', 'member_verified',
                             'member_not_ready', 'link_expired', 'cancel_accepted',
                             'cancel_rejected', 'published', 'deadline_reached',
                             'export_failed', 'job_recovered', 'published_to_downstream')
    ]


def collection():
    """The full collection: poll, restart, first download, link renewal,
    cancelled-by-user and published, on a clean temporary store."""
    return _run_timeline(export_sim.load_fixture())


def _run_timeline(fixture):
    with TemporaryDirectory() as directory:
        path = Path(directory) / 'exports.db'
        api = export_sim.ScenarioAPI(
            fixture['parts'], ready_after=fixture['ready_at'],
            link_ttl=fixture['link_ttl'], member_available_after={'oct-b': 1})
        store = export_store.ExportStore(path)
        engine = export_flow.ExportEngine(api, store)
        intent, _ = engine.submit(
            fixture['intent']['tenant'], fixture['intent']['filters'],
            fixture['intent']['deadline'], 0)

        trace = []
        trace.append(_record(engine, store, intent['id'], 'advance', 0))
        trace.append(_record(engine, store, intent['id'], 'advance', 4))

        # Restart: a fresh engine and store object over the same files.  It
        # must find the already-applied export E1, not apply a new one.
        reopened_store = export_store.ExportStore(path)
        reopened = export_flow.ExportEngine(api, reopened_store)
        result = reopened.recover(intent['id'], 5)
        trace.append({
            'at': 5, 'step': result['step'],
            'state': reopened_store.get_intent(intent['id'])['state'],
            'job_id': reopened_store.get_intent(intent['id'])['job_id'],
            'apply_calls': api.apply_calls,
        })
        engine, store = reopened, reopened_store

        # At t=6 oct-a is downloadable; oct-b is declared and its address is
        # issued, but the file itself is still unavailable.
        old_link = store.links(intent['id'])['oct-b']['url']
        trace.append(_record(engine, store, intent['id'], 'download', 6, member='oct-a'))
        trace.append(_record(engine, store, intent['id'], 'download', 6, member='oct-b'))

        # At t=8 the original oct-b address has expired (issued at 5, ttl 3).
        trace.append(_record(engine, store, intent['id'], 'download', 8,
                             member='oct-b', link_url=old_link))
        trace.append(_record(engine, store, intent['id'], 'publish', 9))

        publication = store.publication_for_intent(intent['id'])
        return {
            'intent': {
                'id': intent['id'],
                'tenant': intent['tenant'],
                'filters': intent['filters'],
                'deadline': intent['deadline'],
                'state': store.get_intent(intent['id'])['state'],
                'partner_job': store.get_intent(intent['id'])['job_id'],
                'decision': store.get_intent(intent['id'])['decision_outcome'],
            },
            'restart': {'apply_calls': api.apply_calls},
            'trace': trace,
            'publication': export_flow.summarize_publication(publication),
            'events': _round_events(store, intent['id']),
        }


def _record(engine, store, intent_id, kind, now, **kwargs):
    if kind == 'advance':
        result = engine.advance(intent_id, now)
        step = result['step']
        detail = export_flow._trace_detail(result)
    elif kind == 'download':
        result = engine.download_member(intent_id, kwargs['member'], now,
                                        link_url=kwargs.get('link_url'))
        step = result['step']
        detail = dict(result.get('member') or {})
        detail['name'] = result.get('name')
    elif kind == 'publish':
        result = engine.publish(intent_id, now)
        step = result['outcome']
        detail = result.get('decision')
    else:
        raise ValueError(kind)
    return {'at': now, 'step': step, 'state': store.get_intent(intent_id)['state'],
            'detail': detail}


def cancellation():
    """Show both sides of the cancel/last-shard interleaving explicitly."""
    fixture = export_sim.load_fixture()
    outcomes = {}
    for label, cancel_at in (('cancel_at_6', 6), ('cancel_at_8', 8)):
        with TemporaryDirectory() as directory:
            api = export_sim.ScenarioAPI(fixture['parts'], ready_after=5, link_ttl=3,
                                         member_available_after={'oct-b': 1})
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            engine = export_flow.ExportEngine(api, store)
            intent, _ = engine.submit('east', fixture['intent']['filters'], 60, 0)
            trace = engine.run_until(intent['id'], [0, 5, 6, 7], cancel_at=cancel_at)
            decision = store.get_intent(intent['id'])
            outcomes[label] = {
                'cancel_at': cancel_at,
                'accepted': any(item.get('accepted') is True for item in trace),
                'final_state': decision['state'],
                'final_outcome': decision['decision_outcome'],
                'reason': decision['decision_reason'],
                'published': store.publication_for_intent(intent['id']) is not None,
            }
    return outcomes


def deadline():
    fixture = export_sim.load_fixture()
    with TemporaryDirectory() as directory:
        api = export_sim.ScenarioAPI(fixture['parts'], ready_after=100, link_ttl=3)
        store = export_store.ExportStore(Path(directory) / 'exports.db')
        engine = export_flow.ExportEngine(api, store)
        intent, _ = engine.submit('south', fixture['intent']['filters'], 10, 0)
        engine.advance(intent['id'], 0)
        result = engine.advance(intent['id'], 10)
        stored = store.get_intent(intent['id'])
        return {
            'step': result['step'],
            'state': stored['state'],
            'decision': stored['decision_outcome'],
            'reason': stored['decision_reason'],
            'publication': store.publication_for_intent(intent['id']),
        }


def run():
    return {
        'legacy': legacy(),
        'collection': _run_timeline(export_sim.load_fixture()),
        'cancellation': cancellation(),
        'deadline': deadline(),
    }


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False, indent=2))
