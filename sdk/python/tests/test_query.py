import unittest

import halo_sdk


class Results(unittest.TestCase):
    def test_typed_without_a_switch(self) -> None:
        r = halo_sdk.to_result({"kind": "result", "completed": True, "result": {"kind": "rows", "columns": ["region", "avg price"],
                                                                                "rows": [{"region": "west", "avg price": 600.3}]}}, "s1")
        self.assertTrue(r.ok)
        self.assertEqual(r.rows, [{"region": "west", "avg price": 600.3}])
        self.assertRegex(r.text, r"REGION\s+AVG PRICE\nwest\s+600\.3")
        c = halo_sdk.to_result({"kind": "result", "completed": True, "result": {"kind": "rows", "note": "3", "answer": {"aggregate": "count", "count": 3}}}, "s1")
        self.assertEqual((c.value, c.text), (3, "3"))
        a = halo_sdk.to_result({"kind": "answer", "completed": True, "message": "125.66 lb"}, "s1")
        self.assertEqual((a.status, a.value), ("answered", "125.66 lb"))
        q = halo_sdk.to_result({"kind": "choice", "completed": False, "question": "Which file?", "options": ["a.pdf", "b.pdf"]}, "s1")
        self.assertEqual((q.status, q.ok, q.options), ("needs_input", False, ["a.pdf", "b.pdf"]))
        d = halo_sdk.to_result({"kind": "denied", "completed": False, "reason": "needs approval"}, "s1")
        self.assertEqual((d.status, d.text), ("denied", "needs approval"))


if __name__ == "__main__":
    unittest.main()
