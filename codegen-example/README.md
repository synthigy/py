# Codegen example — movies

The same MovieLens-shaped dataset the other SDKs use. Only `movies.xsql` is
here. You produce the rest, because producing it is the example.

Point the generator at a running Synthigy with credentials in the environment:

```bash
export SYNTHIGY_ENDPOINT=http://localhost:7887
export SYNTHIGY_CLIENT_ID=... SYNTHIGY_CLIENT_SECRET=...

python -m synthigy.codegen pull "$SYNTHIGY_ENDPOINT" codegen-example/schema.json
python -m synthigy.codegen gen  codegen-example/movies.xsql --pull
```

Three files appear, in this order.

1. `schema.json` — the model your credentials are allowed to see. It is
   filtered by permissions, so two users can pull two different schemas from
   one instance.
2. `movies.ir.json` — the server's compiled form of your templates: the
   operations, their parameters and every projection shape, plus a hash of the
   source it came from.
3. `movies_gen.py` — the typed module, rendered from those two.

Drop `--pull` to regenerate offline from the saved intermediate form. That is
fast but blind: edit `movies.xsql` and the hash no longer matches, and the
generator will tell you so rather than emit a stale client.

In your own project you would commit the pulled files and generate offline in
continuous integration. An example commits none of them, so that the path is
something you walk rather than something you read about.
