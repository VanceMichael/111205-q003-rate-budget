import unittest
from fake_api import PartnerAPI


class PartnerTests(unittest.TestCase):
    def test_virtual_time_and_response_isolation(self):
        responses = [{'at': 2, 'status': 429}, {'at': 5, 'status': 200, 'records': ['a']}]
        partner = PartnerAPI(responses)
        with self.assertRaises(ValueError):
            partner.request('key', 'north', 'items', 1)
        self.assertEqual(partner.request('key', 'north', 'items', 2)['status'], 429)
        response = partner.request('key', 'south', 'items', 5)
        response['records'].clear()
        self.assertEqual(partner.request('key', 'south', 'items', 5)['records'], ['a'])
        self.assertEqual(len(partner.calls), 3)
        self.assertEqual(partner.calls[1]['tenant'], 'south')
