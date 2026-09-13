import unittest

from synthigy import compose_forest, compose_tree


class TestCompose(unittest.TestCase):
    def test_tree_basic(self):
        flat = [
            {"xid": "a", "father": None},
            {"xid": "b", "father": {"xid": "a"}},
            {"xid": "c", "father": {"xid": "a"}},
            {"xid": "d", "father": {"xid": "b"}},
        ]
        tree = compose_tree(flat, "father", root_id="a")
        self.assertEqual(tree["xid"], "a")
        kids = {c["xid"] for c in tree["_children"]}
        self.assertEqual(kids, {"b", "c"})
        b = next(c for c in tree["_children"] if c["xid"] == "b")
        self.assertEqual(b["_children"][0]["xid"], "d")

    def test_tree_default_root_is_first(self):
        flat = [{"xid": "r", "parent": None},
                {"xid": "k", "parent": {"xid": "r"}}]
        self.assertEqual(compose_tree(flat, "parent")["xid"], "r")

    def test_tree_missing_root(self):
        self.assertIsNone(
            compose_tree([{"xid": "a", "p": None}], "p", root_id="zzz"))

    def test_kebab_snake_key_variants(self):
        flat = [{"xid": "a"},
                {"xid": "b", "reports_to": {"xid": "a"}}]
        tree = compose_tree(flat, "reports-to", root_id="a")
        self.assertEqual(tree["_children"][0]["xid"], "b")

    def test_parent_as_plain_id(self):
        flat = [{"xid": "a"}, {"xid": "b", "boss": "a"}]
        tree = compose_tree(flat, "boss", root_id="a")
        self.assertEqual(tree["_children"][0]["xid"], "b")

    def test_cycle_safe(self):
        flat = [{"xid": "a", "p": {"xid": "b"}},
                {"xid": "b", "p": {"xid": "a"}}]
        tree = compose_tree(flat, "p", root_id="a")
        self.assertEqual(tree["xid"], "a")
        self.assertEqual(tree["_children"][0]["xid"], "b")
        self.assertEqual(tree["_children"][0]["_children"], [])

    def test_forest_roots(self):
        flat = [
            {"xid": "r1", "p": None},
            {"xid": "r2", "p": {"xid": "gone"}},   # parent not in set → root
            {"xid": "k", "p": {"xid": "r1"}},
        ]
        forest = compose_forest(flat, "p")
        self.assertEqual([t["xid"] for t in forest], ["r1", "r2"])
        self.assertEqual(forest[0]["_children"][0]["xid"], "k")

    def test_custom_children_key(self):
        flat = [{"xid": "a"}, {"xid": "b", "p": {"xid": "a"}}]
        tree = compose_tree(flat, "p", root_id="a", children_key="kids")
        self.assertEqual(tree["kids"][0]["xid"], "b")

    def test_empty(self):
        self.assertIsNone(compose_tree([], "p"))
        self.assertEqual(compose_forest([], "p"), [])

    def test_on_required(self):
        with self.assertRaises(ValueError):
            compose_tree([{"xid": "a"}], None)


if __name__ == "__main__":
    unittest.main()
