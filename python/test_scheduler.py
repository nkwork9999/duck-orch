import unittest

from duckorch_embedded import CriticalPathScheduler, ScheduledTask


class SchedulerTests(unittest.TestCase):
    def test_prioritizes_longest_path_within_tenant_slots(self):
        tasks = {
            "a-short": ScheduledTask("tenant-a", (), 1),
            "b-prep": ScheduledTask("tenant-a", (), 2),
            "c-long": ScheduledTask("tenant-a", ("b-prep",), 20),
            "d-prep": ScheduledTask("tenant-b", (), 3),
            "e-run": ScheduledTask("tenant-b", ("d-prep",), None),
            "f-done": ScheduledTask("tenant-b", ("e-run",), 1),
        }
        planner = CriticalPathScheduler(tasks, {"tenant-a": 1, "tenant-b": 1}, default_duration=4)
        self.assertEqual(planner.ready(completed=set(), running=set(), slots=2), ("b-prep", "d-prep"))
        self.assertEqual(planner.ready(completed={"b-prep"}, running={"d-prep"}, slots=2), ("c-long",))
        self.assertEqual(planner.ready(completed={"b-prep", "c-long", "d-prep"}, running=set(), slots=2),
                         ("e-run", "a-short"))
        self.assertEqual(planner.ready(completed={"b-prep", "c-long", "d-prep", "e-run"},
                                       running={"a-short"}, slots=2), ("f-done",))
        self.assertEqual(planner.ready(completed=set(tasks) - {"f-done"}, running=set(), slots=2), ("f-done",))

    def test_cycle_and_unregistered_tenant_are_rejected(self):
        with self.assertRaises(ValueError):
            CriticalPathScheduler({"loop": ScheduledTask("a", ("loop",), 1)}, {"a": 1})
        with self.assertRaises(ValueError):
            CriticalPathScheduler({"task": ScheduledTask("other", (), 1)}, {"a": 1})
        with self.assertRaises(ValueError):
            CriticalPathScheduler({"secret": ScheduledTask("a", (), 1),
                                   "foreign": ScheduledTask("b", ("secret",), 1)}, {"a": 1, "b": 1})
