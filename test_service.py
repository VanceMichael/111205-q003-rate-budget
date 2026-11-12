import unittest
from decimal import Decimal
from service import eligible

class RateBudgetTests(unittest.TestCase):
    def test_deadline(self):
        self.assertTrue(eligible(1, 3, 4))
        self.assertFalse(eligible(1, 5, 4))

    def test_rejects_invalid_schedule(self):
        with self.assertRaises(ValueError):
            eligible(4, 3)


if __name__ == '__main__':
    unittest.main()
