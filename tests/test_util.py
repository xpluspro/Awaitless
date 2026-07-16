import unittest

from awaitless.util import parse_duration


class DurationTest(unittest.TestCase):
    def test_units(self) -> None:
        self.assertEqual(parse_duration("30s"), 30)
        self.assertEqual(parse_duration("2m"), 120)
        self.assertEqual(parse_duration("1.5h"), 5400)

    def test_invalid(self) -> None:
        with self.assertRaises(ValueError):
            parse_duration("soon")
