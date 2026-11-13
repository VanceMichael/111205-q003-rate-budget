"""Concurrency tests for the export flow.

Two real processes are started against the same SQLite store to prove that
only one of them can publish an intent, and that a cancellation committed at
the same virtual instant as the last shard has a single deterministic
outcome.
"""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import export_flow
import export_sim
import export_store


ROOT = Path(__file__).resolve().parents[1]

WORKER = '''\
import json, sys
sys.path.insert(0, {root!r})
import export_flow, export_sim, export_store

fixture = export_sim.load_fixture({fixture!r})
store = export_store.ExportStore({store_path!r})
intent = store.list_intents()[0]
scenario = export_sim.ScenarioAPI(
    fixture['parts'], ready_after=fixture['ready_at'], link_ttl=fixture['link_ttl'])
if intent['job_id']:
    scenario.jobs[intent['job_id']] = {{
        'tenant': intent['tenant'], 'filters': intent['filters'], 'created_at': 0,
        'intent_key': intent['intent_key'], 'cancelled': False,
    }}
engine = export_flow.ExportEngine(scenario, store)
result = engine.advance(intent['id'], {now!r})
print(json.dumps({{'step': result['step'],
                   'state': store.get_intent(intent['id'])['state']}}))
'''


class CrossProcessTests(unittest.TestCase):
    def test_two_processes_recover_the_same_intent_and_publish_once(self):
        fixture_path = ROOT / 'fixtures' / 'exports.json'
        with tempfile.TemporaryDirectory() as directory:
            store_path = str(Path(directory) / 'exports.db')
            store = export_store.ExportStore(store_path)
            fixture = export_sim.load_fixture(fixture_path)
            intent, _ = store.submit_intent('north', fixture['intent']['filters'], 60, 0)
            scenario = export_sim.ScenarioAPI(
                fixture['parts'], ready_after=fixture['ready_at'], link_ttl=fixture['link_ttl'])
            engine = export_flow.ExportEngine(scenario, store)
            engine.advance(intent['id'], 0)  # apply persisted by this process
            self.assertEqual(store.get_intent(intent['id'])['job_id'], 'E1')

            script = Path(directory) / 'worker.py'
            script.write_text(WORKER.format(
                root=str(ROOT), fixture=str(fixture_path), store_path=store_path, now=6))
            procs = [
                subprocess.Popen([sys.executable, str(script)],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                for _ in range(2)
            ]
            results = []
            for proc in procs:
                stdout, stderr = proc.communicate(timeout=30)
                self.assertEqual(proc.returncode, 0, stderr)
                results.append(json.loads(stdout))

            publications = store.list_publications()
            self.assertEqual(len(publications), 1)
            self.assertEqual(len({publication['id'] for publication in publications}), 1)
            publish_events = [event for event in store.events(intent['id'])
                              if event['kind'] == 'published_to_downstream']
            self.assertEqual(len(publish_events), 1)
            self.assertEqual(store.get_intent(intent['id'])['state'], 'published')
            self.assertEqual(store.get_intent(intent['id'])['job_id'], 'E1')
            # At least one worker must have performed the publication; the
            # other may observe the terminal decision at any point in its own
            # step, so its reported step is intentionally not asserted.
            self.assertTrue(any(result['step'] == 'published' for result in results))
            self.assertTrue(all(result['state'] in ('published', 'downloading', 'verifying')
                                for result in results))


class CancelInterleavingTests(unittest.TestCase):
    def _fresh(self):
        directory = tempfile.TemporaryDirectory()
        store = export_store.ExportStore(Path(directory.name) / 'exports.db')
        fixture = export_sim.load_fixture()
        api = export_sim.ScenarioAPI(fixture['parts'], ready_after=5, link_ttl=3,
                                     member_available_after={'oct-b': 1})
        engine = export_flow.ExportEngine(api, store)
        intent, _ = engine.submit('north', fixture['intent']['filters'], 60, 0)
        return directory, store, engine, intent

    def test_cancel_before_last_shard_means_the_shard_is_rejected(self):
        directory, store, engine, intent = self._fresh()
        with directory:
            engine.run_until(intent['id'], [0, 5, 6])
            decision = engine.cancel(intent['id'], 6)
            self.assertTrue(decision['accepted'])
            result = engine.download_member(intent['id'], 'oct-b', 6)
            self.assertEqual(result['step'], 'terminal')
            self.assertEqual(store.get_intent(intent['id'])['state'], 'cancelled')
            self.assertIsNone(store.publication_for_intent(intent['id']))
            events = store.events(intent['id'])
            self.assertTrue(any(event['kind'] == 'cancel_accepted' for event in events))
            self.assertFalse(any(event['kind'] == 'published' for event in events))

    def test_publication_committed_first_makes_later_cancel_explainable(self):
        directory, store, engine, intent = self._fresh()
        with directory:
            engine.run_until(intent['id'], [0, 5, 6, 7])
            self.assertEqual(store.get_intent(intent['id'])['state'], 'published')
            decision = engine.cancel(intent['id'], 8)
            self.assertFalse(decision['accepted'])
            self.assertEqual(decision['decision']['outcome'], 'published')
            rejected = [event for event in store.events(intent['id'])
                        if event['kind'] == 'cancel_rejected']
            self.assertTrue(rejected)
            self.assertEqual(rejected[-1]['detail']['publish_event_id'],
                             decision['decision']['event_id'])


if __name__ == '__main__':
    unittest.main()
