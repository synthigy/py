"""Port of the JS selection-normalization test matrix (client.test.js).

FLAT-LEFT DECREE: join semantics are the SERVER's (absent _join = LEFT,
every op). The client injects nothing — selections travel as written, and
{"_join": "inner"} is the caller's explicit way to scope parents.
"""

import unittest

from synthigy import fields, normalize_selection, rel


class TestNormalizeSelection(unittest.TestCase):
    def test_scalars(self):
        self.assertEqual(normalize_selection({"name": None, "email": True}),
                         {"name": None, "email": None})

    def test_string_list_shorthand(self):
        self.assertEqual(normalize_selection(["name", "email"]),
                         {"name": None, "email": None})

    def test_fields_helper(self):
        self.assertEqual(fields("a", "b"), {"a": None, "b": None})

    def test_plain_object_relation_wraps_bare(self):
        self.assertEqual(
            normalize_selection({"roles": {"name": None}}),
            {"roles": [{"selections": {"name": None}}]})

    def test_nested_relations_stay_bare(self):
        out = normalize_selection({"roles": {"name": None,
                                             "perms": {"key": None}}})
        role_cfg = out["roles"][0]
        self.assertNotIn("args", role_cfg)
        self.assertNotIn("args", role_cfg["selections"]["perms"][0])

    def test_explicit_join_preserved(self):
        out = normalize_selection(
            {"roles": rel({"name": None}, args={"_join": "inner"})})
        self.assertEqual(out["roles"][0]["args"], {"_join": "inner"})

    def test_maybe_preserved(self):
        out = normalize_selection(
            {"roles": rel({"name": None}, args={"_maybe": {"x": {"_eq": 1}}})})
        self.assertNotIn("_join", out["roles"][0]["args"])

    def test_rel_args_pass_through_unchanged(self):
        out = normalize_selection(
            {"roles": rel({"name": None}, args={"_limit": 5})})
        self.assertEqual(out["roles"][0]["args"], {"_limit": 5})

    def test_rel_alias(self):
        out = normalize_selection(
            {"roles": rel({"name": None}, alias="active_roles")})
        self.assertEqual(out["roles"][0]["alias"], "active_roles")

    def test_multiple_rel_configs(self):
        out = normalize_selection({"roles": [
            rel({"name": None}, args={"_where": {"active": {"_eq": True}}},
                alias="on"),
            rel({"name": None}, args={"_where": {"active": {"_eq": False}}},
                alias="off"),
        ]})
        self.assertEqual(len(out["roles"]), 2)
        self.assertEqual(out["roles"][0]["alias"], "on")
        # no join is ever injected — the wire carries what the caller wrote
        self.assertNotIn("_join", out["roles"][1]["args"])

    def test_count_agg_subtrees_never_get_join(self):
        out = normalize_selection({
            "name": None,
            "_count": {"roles": None},
            "_agg": {"tasks": {"priority": ["avg"]}},
        })
        self.assertEqual(out["_count"], [{"selections": {"roles": None}}])
        agg_tasks = out["_agg"][0]["selections"]["tasks"][0]
        self.assertNotIn("args", agg_tasks)

    def test_nested_string_list(self):
        out = normalize_selection({"roles": ["name", "type"]})
        self.assertEqual(out["roles"][0]["selections"],
                         {"name": None, "type": None})
        self.assertNotIn("args", out["roles"][0])

    def test_none_passthrough(self):
        self.assertIsNone(normalize_selection(None))


if __name__ == "__main__":
    unittest.main()
