"""Codegen tests — hermetic. The committed fixture (tests/fixtures/) was
built from a live op:"describe" + GET /schema and trimmed to the referenced
entities; the emitter renders from it offline. Wire behavior of generated
ops is asserted against the StubServer from test_client."""

import importlib.util
import io
import json
import py_compile
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

import synthigy
from synthigy import SynthigyError
from synthigy import codegen
from test_client import StubServer, ok_results  # noqa: F401

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixtures():
    ir = json.loads((FIXTURES / "example.ir.json").read_text())
    schema = json.loads((FIXTURES / "schema.json").read_text())
    return ir, schema


def render_fixture(**kw):
    ir, schema = load_fixtures()
    return codegen.render(ir, schema, input_name="example.xsql", **kw)


class TestRender(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.code = render_fixture()

    def test_header_carries_hash_and_do_not_edit(self):
        ir, _ = load_fixtures()
        self.assertIn("DO NOT EDIT", self.code)
        self.assertIn(f"# sourceHash: {ir['sourceHash']}", self.code)

    def test_read_tier_signatures(self):
        # all-optional params → params optional; search → list[Row]
        self.assertIn("def list(params: MovieListParams | None = None, "
                      "**opts: Any) -> list[MovieListRow]:", self.code)
        # required param → positional; get → Row | None
        self.assertIn("def detail(params: MovieDetailParams, "
                      "**opts: Any) -> MovieDetailRow | None:", self.code)
        self.assertIn("op='get'", self.code)
        # STRICT wire: no entity field — the document carries it
        self.assertNotIn("entity='movie'", self.code.split("def watch_")[0])
        # sql-template rides sql_template, typed from @returns
        self.assertIn("def count(params: MovieCountParams | None = None, "
                      "**opts: Any) -> list[MovieCountRow]:", self.code)
        self.assertIn('"total": int,', self.code)

    def test_row_types(self):
        # nullable:false → non-null; default → | None; relations NotRequired
        self.assertIn('"xid": str,', self.code)
        self.assertIn('"title": str | None,', self.code)
        self.assertIn('"genres": NotRequired[list[MovieListRowGenres]],',
                      self.code)
        # one-cardinality nests a single TypedDict (path-named)
        self.assertIn('"created_by": '
                      "NotRequired[MovieDetailRowMovieRatingsCreatedBy],",
                      self.code)
        # _count / _agg → honest maps
        self.assertIn('"_count": NotRequired[dict[str, int]],', self.code)
        self.assertIn('"_agg": NotRequired[dict[str, Any]],', self.code)
        # sql @returns float? → float | None
        self.assertIn('"avg_rating": float | None,', self.code)

    def test_watch_variants(self):
        self.assertIn("def watch_list(", self.code)
        self.assertIn("synthigy.watch_query_xsql(_SRC_MOVIE_LIST", self.code)
        self.assertIn("entity='movie'", self.code)
        # sql watch threads the @watch entity list as entities=
        self.assertIn("entities=['movie', 'user_rating', 'movie_actor']",
                      self.code)

    def test_namespaces(self):
        self.assertIn("class Movie:", self.code)
        self.assertIn("class Dashboard:", self.code)   # entity-less @namespace

    def test_batch(self):
        self.assertIn("def overview(params: OverviewBatchParams | None = None",
                      self.code)
        self.assertIn("synthigy.exec_(_operations", self.code)
        self.assertIn('"list": _batch_result(_results[0]', self.code)
        self.assertIn('"stats": _batch_result(_results[1]', self.code)

    def test_write_tier(self):
        # nullable:false attr → REQUIRED input key; others NotRequired
        self.assertIn('"value": float,', self.code)              # user_rating
        self.assertIn('"title": NotRequired[str | None],', self.code)
        # enum → Literal on input
        self.assertIn("Literal['PERSON', 'SERVICE', 'SECRET', 'ROBOT', "
                      "'OAUTH_CLIENT']", self.code)
        # relation on input = link or nested write (forward-ref string)
        self.assertIn('"NotRequired[list[_Link | MovieGenreInput]]"', self.code)
        self.assertIn('_Link = TypedDict("_Link", {"xid": str})', self.code)
        for fn in ("sync_movie", "stack_movie", "delete_movie",
                   "sync_user_rating"):
            self.assertIn(f"def {fn}(", self.code)
        self.assertIn('"xid": str,', self.code)   # WriteResult xid required

    def test_no_writes(self):
        code = codegen.render(load_fixtures()[0], None, writes=False)
        self.assertNotIn("sync_movie", code)
        self.assertNotIn("_Link", code)
        self.assertIn("class Movie:", code)   # read tier intact

    def test_output_compiles(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "gen.py"
            p.write_text(self.code)
            py_compile.compile(str(p), doraise=True)


class TestFailLoud(unittest.TestCase):
    """The stale-IR lesson: never silently default an op kind to search."""

    def render_ops(self, operations, schema=None):
        return codegen.render({"sourceHash": "x", "operations": operations},
                              schema or {"entities": {}}, input_name="t.xsql")

    def test_empty_op_kind_is_fatal(self):
        with self.assertRaises(SystemExit) as cm, redirect_stderr(io.StringIO()):
            self.render_ops([{"name": "broken", "op": "", "entity": "movie"}])
        self.assertEqual(cm.exception.code, 1)

    def test_unknown_op_kind_is_fatal(self):
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            self.render_ops([{"name": "x", "op": "frobnicate",
                              "entity": "movie"}])

    def test_mutation_op_skipped_loudly(self):
        buf = io.StringIO()
        with redirect_stderr(buf):
            code = self.render_ops([{"name": "save", "op": "sync",
                                     "entity": "movie"}])
        self.assertIn("skipped @sync save", buf.getvalue())
        self.assertNotIn("def save", code)

    def test_sql_template_without_namespace_is_fatal(self):
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            self.render_ops([{"name": "raw", "op": "sql-template",
                              "entity": None, "source": "SELECT 1",
                              "result": {"fields": []}}])

    def test_duplicate_identity_is_fatal(self):
        op = {"name": "list", "op": "search", "entity": "movie",
              "source": "movie\n  title", "params": [],
              "result": {"fields": []}}
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            self.render_ops([op, dict(op)])

    def test_batch_unknown_member_is_fatal(self):
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            self.render_ops([{"name": "b", "batch": True,
                              "members": ["nope"]}])

    def test_batch_ambiguous_bare_member_is_fatal(self):
        mk = lambda e: {"name": "list", "op": "search", "entity": e,
                        "source": f"{e}\n  xid", "params": [],
                        "result": {"fields": []}}
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            self.render_ops([mk("movie"), mk("user_rating"),
                             {"name": "b", "batch": True, "members": ["list"]}])


class TestGeneratedWire(unittest.TestCase):
    """Import the generated module and assert the exact wire bodies its ops
    produce, against the stub /data server."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        gen_path = Path(cls.tmp.name) / "example_gen.py"
        gen_path.write_text(render_fixture())
        spec = importlib.util.spec_from_file_location("example_gen", gen_path)
        cls.gen = importlib.util.module_from_spec(spec)
        sys.modules["example_gen"] = cls.gen
        spec.loader.exec_module(cls.gen)

    @classmethod
    def tearDownClass(cls):
        sys.modules.pop("example_gen", None)
        cls.tmp.cleanup()

    def setUp(self):
        self.srv = StubServer()
        self.addCleanup(self.srv.close)
        synthigy.connect(self.srv.endpoint, token="tok")
        self.addCleanup(synthigy.disconnect)

    def body(self):
        return json.loads(self.srv.requests_to("/data")[-1]["body"])

    def test_search_op_wire_body(self):
        self.srv.route("/data", (200, ok_results([{"xid": "m1",
                                                   "title": "Dune"}])))
        rows = self.gen.Movie.list({"limit": 2})
        self.assertEqual(rows, [{"xid": "m1", "title": "Dune"}])
        ops = self.body()["operations"]
        self.assertEqual(len(ops), 1)
        self.assertEqual(ops[0]["op"], "xsql")
        self.assertNotIn("entity", ops[0])
        self.assertEqual(ops[0]["params"], {"limit": 2})
        # embedded XSQL DOCUMENT goes over the wire verbatim
        self.assertEqual(ops[0]["xsql"], self.gen._SRC_MOVIE_LIST)
        self.assertTrue(ops[0]["xsql"].startswith(
            "@search list\nmovie (release_year > ?since:int=1980"))

    def test_get_op_returns_none_when_missing(self):
        self.srv.route("/data", (200, ok_results(None)))
        self.assertIsNone(self.gen.Movie.detail({"xid": "nope"}))
        op = self.body()["operations"][0]
        self.assertEqual((op["op"], op["params"]), ("xsql", {"xid": "nope"}))
        self.assertTrue(op["xsql"].startswith("@get detail\n"))

    def test_sql_template_wire_body(self):
        self.srv.route("/data", (200, ok_results([{"total": 7}])))
        self.assertEqual(self.gen.Movie.count({"since": 2000}),
                         [{"total": 7}])
        op = self.body()["operations"][0]
        self.assertEqual(op["op"], "sql-template")
        self.assertEqual(op["template"], self.gen._SRC_MOVIE_COUNT)
        self.assertEqual(op["params"], {"since": 2000})

    def test_batch_is_one_wire_request_with_error_values(self):
        self.srv.route("/data", (200, {"results": [
            {"ok": True, "data": [{"xid": "m1"}]},
            {"ok": False, "error": {"message": "boom", "code": "TEMPLATE_ERROR"}},
        ]}))
        out = self.gen.overview({"limit": 1})
        self.assertEqual(len(self.srv.requests_to("/data")), 1)   # ONE request
        ops = self.body()["operations"]
        self.assertEqual([o["op"] for o in ops], ["xsql", "sql-template"])
        self.assertEqual(ops[0]["params"], {"limit": 1})
        self.assertEqual(out["list"], [{"xid": "m1"}])
        # per-member error is a VALUE, not a raised batch-wide error
        self.assertIsInstance(out["stats"], SynthigyError)
        self.assertEqual(out["stats"].code, "TEMPLATE_ERROR")

    def test_write_fns_wire_body(self):
        self.srv.route("/data", (200, ok_results({"xid": "g1", "name": "N"})))
        self.gen.sync_movie_genre({"xid": "g1", "name": "N"})
        op = self.body()["operations"][0]
        self.assertEqual((op["op"], op["entity"], op["data"]),
                         ("sync", "movie_genre", {"xid": "g1", "name": "N"}))
        self.gen.delete_movie_genre("g1")
        op = self.body()["operations"][0]
        self.assertEqual((op["op"], op["data"]), ("delete", {"xid": "g1"}))

    def test_opts_kwargs_thread_through(self):
        self.srv.route("/data", (200, ok_results([])))
        self.gen.Movie.list({"limit": 1}, acting_as="u-1")
        self.assertEqual(self.body()["acting_as"], "u-1")


class TestAsyncEmission(unittest.TestCase):
    """Async twins: *Async classes + *_async batches run on the module
    default AsyncClient (synthigy.aconnect) with the SAME wire bodies."""

    @classmethod
    def setUpClass(cls):
        cls.code = render_fixture()

    def test_async_classes_and_signatures(self):
        self.assertIn("class MovieAsync:", self.code)
        self.assertIn("class DashboardAsync:", self.code)
        self.assertIn("async def list(params: MovieListParams | None = None, "
                      "**opts: Any) -> list[MovieListRow]:", self.code)
        self.assertIn("await synthigy.aclient().query(_SRC_MOVIE_LIST",
                      self.code)
        # async watch twin returns the native handle (not awaited)
        self.assertIn("synthigy.aclient().watch_query_xsql(_SRC_MOVIE_LIST",
                      self.code)
        self.assertIn("async def overview_async(", self.code)
        self.assertIn("await synthigy.aclient().exec_(_operations", self.code)

    def test_async_ops_same_wire_body(self):
        import asyncio
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        gen_path = Path(tmp.name) / "example_gen_a.py"
        gen_path.write_text(self.code)
        spec = importlib.util.spec_from_file_location("example_gen_a", gen_path)
        gen = importlib.util.module_from_spec(spec)
        sys.modules["example_gen_a"] = gen
        self.addCleanup(lambda: sys.modules.pop("example_gen_a", None))
        spec.loader.exec_module(gen)

        srv = StubServer()
        self.addCleanup(srv.close)
        srv.route("/data", (200, ok_results([{"xid": "m1", "title": "Dune"}])))
        synthigy.aconnect(srv.endpoint, token="tok")

        async def run():
            try:
                rows = await gen.MovieAsync.list({"limit": 2}, acting_as="u-9")
                self.assertEqual(rows, [{"xid": "m1", "title": "Dune"}])
                srv.route("/data", (200, {"results": [
                    {"ok": True, "data": [{"xid": "m1"}]},
                    {"ok": True, "data": [{"total": 1}]}]}))
                out = await gen.overview_async({"limit": 1})
                self.assertEqual(out["list"], [{"xid": "m1"}])
            finally:
                await synthigy.adisconnect()

        asyncio.run(run())
        first = json.loads(srv.requests_to("/data")[0]["body"])
        self.assertEqual(first["acting_as"], "u-9")
        self.assertEqual(first["operations"][0]["op"], "xsql")
        self.assertEqual(first["operations"][0]["xsql"], gen._SRC_MOVIE_LIST)


class TestCli(unittest.TestCase):
    """gen (offline from cached IR) + check (offline hash gate)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        (self.dir / "example.xsql").write_text(
            (FIXTURES / "example.xsql").read_text())
        (self.dir / "example.ir.json").write_text(
            (FIXTURES / "example.ir.json").read_text())
        (self.dir / "synthigy.schema.json").write_text(
            (FIXTURES / "schema.json").read_text())

    def _env(self):
        # SYNTHIGY_* cleared: offline paths must not need a backend
        env = {k: v for k, v in codegen.os.environ.items()
               if not k.startswith("SYNTHIGY_")}
        return mock.patch.dict(codegen.os.environ, env, clear=True)

    def test_gen_offline_from_cached_ir(self):
        with self._env(), redirect_stderr(io.StringIO()), \
                redirect_stdout(io.StringIO()):
            rc = codegen.main(["gen", str(self.dir / "example.xsql")])
        self.assertEqual(rc, 0)
        out = self.dir / "example_gen.py"
        self.assertTrue(out.exists())
        py_compile.compile(str(out), doraise=True)
        self.assertIn("class Movie:", out.read_text())

    def test_check_ok_offline_when_hash_matches(self):
        with self._env(), redirect_stdout(io.StringIO()):
            self.assertEqual(
                codegen.main(["check", str(self.dir / "example.xsql")]), 0)

    def test_check_fails_on_source_drift(self):
        p = self.dir / "example.xsql"
        p.write_text(p.read_text() + "\n# edited\n")
        with self._env(), redirect_stderr(io.StringIO()) as err:
            rc = codegen.main(["check", str(p)])
        self.assertEqual(rc, 1)
        self.assertIn("drifted", err.getvalue())
        self.assertIn("gen", err.getvalue())   # re-run hint

    def test_check_fails_without_saved_ir(self):
        (self.dir / "example.ir.json").unlink()
        with self._env(), redirect_stderr(io.StringIO()):
            self.assertEqual(
                codegen.main(["check", str(self.dir / "example.xsql")]), 1)

    def test_gen_never_emits_from_stale_ir(self):
        # sources drifted + no backend reachable → gen must FAIL, not emit
        p = self.dir / "example.xsql"
        p.write_text(p.read_text() + "\n# drift\n")
        with self._env(), redirect_stderr(io.StringIO()), \
                redirect_stdout(io.StringIO()), \
                mock.patch.object(codegen, "describe",
                                  side_effect=SynthigyError("down",
                                                            "NETWORK_ERROR")):
            with self.assertRaises(SystemExit) as cm:
                codegen.main(["gen", str(p)])
        self.assertEqual(cm.exception.code, 1)
        self.assertFalse((self.dir / "example_gen.py").exists())


if __name__ == "__main__":
    unittest.main()
