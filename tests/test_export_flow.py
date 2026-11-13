"""Tests for the collection workflow around the asynchronous export API."""

import json
import tempfile
import unittest
from pathlib import Path

import export_flow
import export_sim
import export_store
from export_api import ExportAPI


ROOT = Path(__file__).resolve().parents[1]


class Harness:
    def __enter__(self):
        self._directory = tempfile.TemporaryDirectory()
        self.directory = Path(self._directory.name)
        self.store = export_store.ExportStore(self.directory / 'exports.db')
        fixture = export_sim.load_fixture()
        self.parts = fixture['parts']
        self.api = export_sim.ScenarioAPI(self.parts, ready_after=fixture['ready_at'],
                                          link_ttl=fixture['link_ttl'])
        self.engine = export_flow.ExportEngine(self.api, self.store)
        self.intent, _ = self.engine.submit(
            'north', fixture['intent']['filters'], fixture['intent']['deadline'], 0)
        return self

    def __exit__(self, *exc):
        self._directory.cleanup()


def scenario(**overrides):
    fixture = export_sim.load_fixture()
    return fixture, export_sim.ScenarioAPI(fixture['parts'], **overrides)


class IdentityTests(unittest.TestCase):
    def test_partner_job_and_deadline_are_not_the_business_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            filters = {'from': '2026-10-01', 'until': '2026-11-01'}
            first, created = store.submit_intent('north', filters, 60, 0)
            self.assertTrue(created)
            again, created_again = store.submit_intent('north', dict(filters), 999, 3)
            self.assertFalse(created_again)
            self.assertEqual(again['id'], first['id'])
            self.assertEqual(again['intent_key'], first['intent_key'])
            different, created_different = store.submit_intent(
                'north', {'from': '2026-10-01', 'until': '2026-11-02'}, 60, 4)
            self.assertTrue(created_different)
            self.assertNotEqual(different['intent_key'], first['intent_key'])

    def test_tenant_is_part_of_the_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            filters = {'from': '2026-10-01', 'until': '2026-11-01'}
            north, _ = store.submit_intent('north', filters, 60, 0)
            south, _ = store.submit_intent('south', filters, 60, 0)
            self.assertNotEqual(north['intent_key'], south['intent_key'])


class RestartTests(unittest.TestCase):
    def test_restart_before_ready_finds_the_existing_export(self):
        with Harness() as harness:
            harness.engine.advance(harness.intent['id'], 0)
            job = harness.store.get_intent(harness.intent['id'])['job_id']
            self.assertEqual(job, 'E1')

            # A new process opens the same store and the same partner state.
            store = export_store.ExportStore(harness.store.path)
            engine = export_flow.ExportEngine(harness.api, store)
            recovered = engine.recover(harness.intent['id'], 4)
            self.assertEqual(recovered['step'], 'generating')
            self.assertEqual(store.get_intent(harness.intent['id'])['job_id'], job)
            self.assertEqual(harness.api.apply_calls, 1)

    def test_crash_between_apply_and_local_write_does_not_duplicate_the_export(self):
        fixture = export_sim.load_fixture()
        with tempfile.TemporaryDirectory() as directory:
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            api = export_sim.ScenarioAPI(fixture['parts'], ready_after=5, link_ttl=3)
            engine = export_flow.ExportEngine(api, store)
            intent, _ = engine.submit('north', fixture['intent']['filters'], 60, 0)

            # Simulate: the partner accepted the apply, then the process died
            # before recording the job id.
            begun = store.begin_apply(intent['id'], 0)
            job = api.apply(begun['tenant'], begun['filters'], 0,
                            intent_key=begun['intent_key'])
            self.assertEqual(job, 'E1')
            self.assertIsNone(store.get_intent(intent['id'])['job_id'])

            reopened = export_store.ExportStore(store.path)
            recovered = export_flow.ExportEngine(api, reopened).recover(intent['id'], 1)
            self.assertIn(recovered['step'], ('generating', 'incomplete', 'published'))
            self.assertEqual(reopened.get_intent(intent['id'])['job_id'], 'E1')
            self.assertEqual(len(api.jobs), 1)
            self.assertEqual(api.apply_calls, 2)  # one lost + one idempotent retry


class DeclarationTests(unittest.TestCase):
    def test_incomplete_members_are_not_published(self):
        with Harness() as harness:
            harness.api.member_available_after = {'oct-b': 1}
            harness.engine.advance(harness.intent['id'], 0)
            first = harness.engine.poll_ready(harness.intent['id'], 5)
            self.assertEqual(first['step'], 'ready')
            self.assertEqual([member['name'] for member in first['members']], ['oct-a', 'oct-b'])
            downloaded = harness.engine.download_member(harness.intent['id'], 'oct-a', 5)
            self.assertEqual(downloaded['step'], 'downloaded')
            # oct-b is declared but not yet downloadable.
            blocked = harness.engine.advance(harness.intent['id'], 6)
            self.assertEqual(blocked['step'], 'incomplete')
            self.assertIsNone(harness.store.publication_for_intent(harness.intent['id']))
            published = harness.engine.advance(harness.intent['id'], 7)
            self.assertEqual(published['step'], 'published')

    def test_row_count_must_match_the_declaration(self):
        parts = export_sim.load_fixture()['parts']
        parts = json.loads(json.dumps(parts))
        parts[1]['rows'] = parts[1]['rows'] + [{'amount': '30.00', 'id': '3'}]
        with tempfile.TemporaryDirectory() as directory:
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            api = export_sim.ScenarioAPI(parts, ready_after=5, link_ttl=10)
            engine = export_flow.ExportEngine(api, store)
            intent, _ = engine.submit('north', {'from': '2026-10-01', 'until': '2026-11-01'}, 60, 0)
            engine.advance(intent['id'], 0)
            declared = engine.poll_ready(intent['id'], 5)
            self.assertEqual(declared['members'][1], {'name': 'oct-b', 'rows': 2})
            # The delivered file is shorter than the declaration.
            api.parts[1]['rows'] = [parts[1]['rows'][0]]
            engine.download_member(intent['id'], 'oct-a', 6)
            result = engine.download_member(intent['id'], 'oct-b', 7)
            self.assertEqual(result['step'], 'invalid')
            self.assertEqual(result['kind'], 'RowMismatch')
            self.assertIsNone(store.publication_for_intent(intent['id']))
            self.assertEqual(store.get_intent(intent['id'])['state'], 'downloading')

    def test_parts_may_use_different_column_orders(self):
        parts = json.loads(json.dumps(export_sim.load_fixture()['parts']))
        parts[0]['rows'][0]['amount'] = '12.00'
        with tempfile.TemporaryDirectory() as directory:
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            api = export_sim.ScenarioAPI(parts, ready_after=5, link_ttl=10,
                                         member_available_after={'oct-b': 1})
            engine = export_flow.ExportEngine(api, store)
            intent, _ = engine.submit('south', {'from': '2026-10-01', 'until': '2026-11-01'}, 60, 0)
            engine.advance(intent['id'], 0)
            engine.advance(intent['id'], 5)
            # oct-b's declared availability is ready + index + lag = 7.
            engine.advance(intent['id'], 7)
            publication = store.publication_for_intent(intent['id'])
            self.assertIsNotNone(publication)
            columns = {member['name']: member['columns']
                       for member in publication['payload']['members']}
            self.assertEqual(columns['oct-a'], ['id', 'amount'])
            self.assertEqual(columns['oct-b'], ['amount', 'id'])
            self.assertEqual(publication['payload']['rows_total'], 2)


class LinkTests(unittest.TestCase):
    def test_expiry_only_refreshes_the_address_of_the_same_export(self):
        parts = json.loads(json.dumps(export_sim.load_fixture()['parts']))
        parts[0]['rows'][0]['amount'] = '12.00'
        with tempfile.TemporaryDirectory() as directory:
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            api = export_sim.ScenarioAPI(parts, ready_after=5, link_ttl=3,
                                         member_available_after={'oct-b': 1})
            engine = export_flow.ExportEngine(api, store)
            intent, _ = engine.submit('west', {'from': '2026-10-01', 'until': '2026-11-01'}, 60, 0)
            engine.advance(intent['id'], 0)
            ready = engine.poll_ready(intent['id'], 5)
            self.assertEqual(ready['step'], 'ready')
            engine.fetch_links(intent['id'], 5, force=True)
            old = store.links(intent['id'])['oct-b']['url']
            self.assertEqual(engine.download_member(intent['id'], 'oct-a', 6)['step'],
                             'downloaded')
            # oct-b's old address (expires at 8) is used at t=8: renewal must
            # stay on the same partner task.
            result = engine.download_member(intent['id'], 'oct-b', 8, link_url=old)
            self.assertEqual(result['step'], 'downloaded')
            job = store.get_intent(intent['id'])['job_id']
            links = store.links(intent['id'])
            self.assertEqual(sorted(links), ['oct-a', 'oct-b'])
            for link in links.values():
                self.assertIn(job, link['url'])
            expiry_events = [event for event in store.events(intent['id'])
                             if event['kind'] == 'link_expired']
            self.assertTrue(expiry_events)
            self.assertGreater(links['oct-b']['issued_at'], 5)
            self.assertEqual(api.apply_calls, 1)

    def test_link_from_another_export_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            fixture = export_sim.load_fixture()
            api = export_sim.ScenarioAPI(fixture['parts'], ready_after=5, link_ttl=3)
            engine = export_flow.ExportEngine(api, store)
            intent, _ = engine.submit('north', fixture['intent']['filters'], 60, 0)
            engine.advance(intent['id'], 0)
            engine.poll_ready(intent['id'], 5)
            foreign = export_sim.ScenarioAPI(fixture['parts'], ready_after=0, link_ttl=30)
            foreign.apply('south', {'from': 'other'}, 0)
            foreign_job = foreign.apply('south', {'from': 'other', 'until': 'x'}, 0)
            foreign_link = foreign.links(foreign_job, 0)[0]
            self.assertNotEqual(foreign_job, store.get_intent(intent['id'])['job_id'])
            with self.assertRaises(export_store.LinkConflict):
                store.record_links(intent['id'], [{
                    'name': 'oct-a', 'url': foreign_link, 'job_id': foreign_job,
                }], 6)
            # A download through a foreign address must not be recorded.
            result = engine.download_member(intent['id'], 'oct-a', 6, link_url=foreign_link)
            self.assertIn(result['step'], ('invalid', 'retry', 'not_ready'))
            self.assertFalse(any(member['downloaded'] for member in store.members(intent['id'])))


class DecisionTests(unittest.TestCase):
    def test_generating_expired_link_failed_cancel_and_deadline_are_distinct(self):
        fixture = export_sim.load_fixture()
        cases = [
            ('generating', dict(ready_after=5), 4, 'generating'),
            ('ready', dict(ready_after=5), 5, 'incomplete'),
            ('failed', dict(ready_after=5, fail_at=6), 6, 'failed'),
            ('cancelled', dict(ready_after=5, cancel_at=6), 6, 'cancelled'),
            ('expired', dict(ready_after=5), 60, 'expired'),
        ]
        for name, overrides, now, expected in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                store = export_store.ExportStore(Path(directory) / 'exports.db')
                api = export_sim.ScenarioAPI(fixture['parts'], link_ttl=3, **overrides)
                engine = export_flow.ExportEngine(api, store)
                intent, _ = engine.submit('north', fixture['intent']['filters'], 60, 0)
                if name == 'expired':
                    intent, _ = engine.submit('north', fixture['intent']['filters'], 50, 0)
                    engine.advance(intent['id'], 0)
                    result = engine.advance(intent['id'], now)
                else:
                    engine.advance(intent['id'], 0)
                    if name != 'generating':
                        engine.poll_ready(intent['id'], 5)
                    result = engine.advance(intent['id'], now)
                self.assertEqual(result['step'], expected)
                state = store.get_intent(intent['id'])['state']
                self.assertIn(state, (expected, 'downloading'))
                decision = store.get_intent(intent['id'])['decision_outcome']
                if expected in ('failed', 'cancelled', 'expired'):
                    self.assertEqual(decision, expected)

    def test_local_deadline_is_recorded_even_without_a_partner_answer(self):
        with tempfile.TemporaryDirectory() as directory:
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            fixture = export_sim.load_fixture()
            api = export_sim.ScenarioAPI(fixture['parts'], ready_after=5, link_ttl=3)
            engine = export_flow.ExportEngine(api, store)
            intent, _ = engine.submit('north', fixture['intent']['filters'], 10, 0)
            engine.advance(intent['id'], 0)
            result = engine.advance(intent['id'], 10)
            self.assertEqual(result['step'], 'expired')
            stored = store.get_intent(intent['id'])
            self.assertEqual(stored['state'], 'expired')
            self.assertEqual(stored['decision_outcome'], 'expired')
            self.assertIn('deadline', stored['decision_reason'])
            self.assertIsNone(store.publication_for_intent(intent['id']))
            kinds = [event['kind'] for event in store.events(intent['id'])]
            self.assertIn('deadline_reached', kinds)

    def test_failed_intent_can_be_retried_as_a_new_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            fixture = export_sim.load_fixture()
            api = export_sim.ScenarioAPI(fixture['parts'], ready_after=5, fail_at=6)
            engine = export_flow.ExportEngine(api, store)
            intent, _ = engine.submit('north', fixture['intent']['filters'], 60, 0)
            engine.advance(intent['id'], 0)
            engine.poll_ready(intent['id'], 5)
            failed = engine.advance(intent['id'], 6)
            self.assertEqual(failed['step'], 'failed')

            api.fail_at = None
            retry, created = engine.retry(intent['id'], 60, 7)
            self.assertTrue(created)
            self.assertEqual(retry['attempt'], 2)
            self.assertNotEqual(retry['intent_key'], intent['intent_key'])
            self.assertEqual(store.get_intent(intent['id'])['state'], 'failed')
            result = engine.advance(retry['id'], 7)
            self.assertEqual(store.get_intent(retry['id'])['job_id'], 'E2')

    def test_late_report_cannot_overwrite_a_newer_intent(self):
        with tempfile.TemporaryDirectory() as directory:
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            fixture = export_sim.load_fixture()
            api = export_sim.ScenarioAPI(fixture['parts'], ready_after=5, link_ttl=3)
            engine = export_flow.ExportEngine(api, store)
            old, _ = engine.submit('north', {'from': '2026-10-01', 'until': '2026-11-01'}, 60, 0)
            engine.advance(old['id'], 0)
            new, _ = engine.submit('north', {'from': '2026-10-01', 'until': '2026-11-02'}, 60, 1)
            engine.advance(new['id'], 1)
            late = engine.advance(old['id'], 7)
            self.assertEqual(late['step'], 'terminal')
            self.assertEqual(store.get_intent(old['id'])['state'], 'superseded')
            self.assertIsNone(store.publication_for_intent(old['id']))
            engine.advance(new['id'], 7)
            self.assertEqual(store.get_intent(new['id'])['state'], 'published')
            active = store.active_publication('north')
            self.assertEqual(active['intent_id'], new['id'])

    def test_declaration_change_between_polls_is_failed_not_published(self):
        with tempfile.TemporaryDirectory() as directory:
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            fixture = export_sim.load_fixture()
            parts = json.loads(json.dumps(fixture['parts']))
            api = export_sim.ScenarioAPI(parts, ready_after=5, link_ttl=10)
            engine = export_flow.ExportEngine(api, store)
            intent, _ = engine.submit('north', fixture['intent']['filters'], 60, 0)
            engine.advance(intent['id'], 0)
            engine.poll_ready(intent['id'], 5)
            api.parts[1]['rows'].append({'id': '9', 'amount': '1.00'})
            result = engine.poll_ready(intent['id'], 6)
            self.assertEqual(result['step'], 'failed')
            self.assertEqual(store.get_intent(intent['id'])['state'], 'failed')
            self.assertIsNone(store.publication_for_intent(intent['id']))


class ExportAPIPartnerTests(unittest.TestCase):
    """The file-backed synthetic partner must work with the same engine."""

    def test_export_api_flow_publishes_and_keeps_provenance(self):
        fixture = json.loads((ROOT / 'fixtures' / 'exports.json').read_text())
        with tempfile.TemporaryDirectory() as directory:
            store = export_store.ExportStore(Path(directory) / 'exports.db')
            api = ExportAPI(Path(directory) / 'partner.json', fixture['parts'], link_ttl=30)
            engine = export_flow.ExportEngine(api, store)
            intent, _ = engine.submit('north', fixture['intent']['filters'], 60, 0)
            for now in (0, 5, 6, 7):
                engine.advance(intent['id'], now)
            publication = store.publication_for_intent(intent['id'])
            self.assertIsNotNone(publication)
            self.assertEqual(publication['payload']['export_job'],
                             store.get_intent(intent['id'])['job_id'])
            self.assertEqual(publication['payload']['rows_total'], 2)


if __name__ == '__main__':
    unittest.main()
