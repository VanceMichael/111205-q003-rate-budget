import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from export_api import ExportAPI


class ExportTests(unittest.TestCase):
    def test_renewal_keeps_the_same_report_after_restart(self):
        fixture = json.loads((Path(__file__).resolve().parents[1] / 'fixtures/exports.json').read_text())
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'exports.json'
            api = ExportAPI(path, fixture['parts'])
            job = api.apply('north', fixture['intent']['filters'], 0)
            self.assertEqual(api.status(job, 4)['state'], 'generating')
            links = api.links(job, 5)
            first = api.download(links[0], 5)
            with self.assertRaises(FileNotFoundError):
                api.download(links[1], 5)
            restored = ExportAPI(path, fixture['parts'])
            with self.assertRaises(TimeoutError):
                restored.download(links[0], 8)
            renewed = restored.links(job, 8)
            self.assertEqual(restored.download(renewed[0], 8), first)
            restored.cancel(job)
            self.assertEqual(restored.status(job, 9)['state'], 'cancelled')
