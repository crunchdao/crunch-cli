import graphlib
import unittest

from crunch.runner.installation import plan_installation


class PlanInstallationTest(unittest.TestCase):

    def test_no_build_dependencies(self):
        batches = plan_installation(["pandas", "torch==2.4.0"], {"torchsort": ["torch"]})

        self.assertEqual([], batches)

    def test_dependency_first(self):
        batches = plan_installation(["pandas", "torchsort  # sorting", "warpgbm"], {
            "torchsort": ["torch"],
            "warpgbm": ["torch"],
        })

        self.assertEqual(2, len(batches))

        self.assertEqual(["torch"], batches[0].lines)
        self.assertTrue(batches[0].build_isolation)
        self.assertEqual({}, batches[0].needs)

        self.assertEqual(["torchsort", "warpgbm"], batches[1].lines)
        self.assertFalse(batches[1].build_isolation)
        self.assertEqual({"torchsort": ["torch"], "warpgbm": ["torch"]}, batches[1].needs)

    def test_user_version_and_specification_both_apply(self):
        batches = plan_installation(["Cython==0.29.36", "some-library"], {
            "some-library": ["Cython<3 ; python_version < \"3.12\""],
        })

        self.assertEqual(["Cython==0.29.36", "Cython<3; python_version < \"3.12\""], batches[0].lines)

    def test_lines_are_formatted_from_the_requirement(self):
        batches = plan_installation(["TorchSort[Extra] >=1  # sorting"], {"torchsort": ["torch;python_version<'3.13'"]})

        self.assertEqual(["torch; python_version < \"3.13\""], batches[0].lines)
        self.assertEqual(["TorchSort[extra]>=1"], batches[1].lines)

    def test_names_are_compared_in_lowercase(self):
        batches = plan_installation(["TorchSort"], {"torchsort": ["torch"]})

        self.assertEqual(["torch"], batches[0].lines)
        self.assertEqual(["TorchSort"], batches[1].lines)

    def test_chain(self):
        batches = plan_installation(["c"], {
            "c": ["b"],
            "b": ["a"],
        })

        self.assertEqual([["a"], ["b"], ["c"]], [batch.lines for batch in batches])
        self.assertEqual([True, False, False], [batch.build_isolation for batch in batches])
        self.assertEqual([{}, {"b": ["a"]}, {"c": ["b"]}], [batch.needs for batch in batches])

    def test_invalid_lines_are_skipped(self):
        batches = plan_installation(["--extra-index-url https://example.com", "torchsort"], {
            "torchsort": ["torch", "not a valid requirement !"],
        })

        self.assertEqual(["torch"], batches[0].lines)

    def test_cycle(self):
        with self.assertRaises(graphlib.CycleError):
            plan_installation(["a"], {
                "a": ["b"],
                "b": ["a"],
            })
