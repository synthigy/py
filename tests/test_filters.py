import unittest

from synthigy import (and_, eq, gt, gte, ilike, in_, is_not_null, is_null,
                      like, lt, lte, neq, nin, not_, or_)


class TestOperators(unittest.TestCase):
    def test_wire_key_mapping(self):
        # gte -> _ge and lte -> _le: the mapping ports get wrong
        self.assertEqual(eq(1), {"_eq": 1})
        self.assertEqual(neq(1), {"_neq": 1})
        self.assertEqual(gt(1), {"_gt": 1})
        self.assertEqual(gte(1), {"_ge": 1})
        self.assertEqual(lt(1), {"_lt": 1})
        self.assertEqual(lte(1), {"_le": 1})
        self.assertEqual(like("a%"), {"_like": "a%"})
        self.assertEqual(ilike("a%"), {"_ilike": "a%"})
        self.assertEqual(is_null(), {"_is_null": True})
        self.assertEqual(is_not_null(), {"_is_not_null": True})

    def test_in_flattens(self):
        self.assertEqual(in_(1, 2, 3), {"_in": [1, 2, 3]})
        self.assertEqual(in_([1, 2], 3), {"_in": [1, 2, 3]})
        self.assertEqual(nin([1, 2]), {"_nin": [1, 2]})

    def test_combinators(self):
        self.assertEqual(and_({"a": eq(1)}, {"b": eq(2)}),
                         {"_and": [{"a": {"_eq": 1}}, {"b": {"_eq": 2}}]})
        self.assertEqual(or_([{"a": eq(1)}, {"b": eq(2)}]),
                         {"_or": [{"a": {"_eq": 1}}, {"b": {"_eq": 2}}]})
        self.assertEqual(not_({"a": eq(1)}), {"_not": {"a": {"_eq": 1}}})


if __name__ == "__main__":
    unittest.main()
