import unittest

import crunch.vendor as vendor


class InitTest(unittest.TestCase):

    def test_find(self):
        self.assertIsNone(vendor.find("i don't exists"))

        from crunch.vendor import datacrunch
        self.assertEqual(
            datacrunch,
            vendor.find("datacrunch")
        )

    def test_get(self):
        with self.assertRaises(vendor.NoVendorModuleException) as context:
            vendor.get("i don't exists")

        self.assertEqual(
            "i don't exists",
            context.exception.competition_name
        )

        from crunch.vendor import datacrunch
        self.assertEqual(
            datacrunch,
            vendor.get("datacrunch")
        )
