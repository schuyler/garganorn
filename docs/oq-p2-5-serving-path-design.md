# OQ-P2-5 — Serving-path migration: implementation plan

Status: **implemented, pending merge/approval**. Tests green: 931 passed, 1
xfailed. Classification: **Standard**. Design gate: PASSED — round-2 review
found no CRITICALs; three IMPORTANT items (manifest-gate consistency, drop
`immutable`, traversal-test rigor) are resolved in the text above. Scope:
**garganorn repo only.** Deploy/infra wiring is a deferred follow-up (see end).

## Why this exists

Phase 2 writes tiles to `<output_dir>/<source>/tiles/current/<qk[:6]>/<qk>.json.gz`
(`current` = symlink to the latest timestamped run). The garganorn dev
`config.yaml` still points at the pre-Phase-2 `tiles/<short>/current` layout, and
the current tile-file route serves from a single global `serve_dir`. Two problems
with just repointing paths:

1. **Over-exposure.** `<output_dir>/<source>/` holds the source's `places.parquet`
   and `containment/` *beside* its `tiles/` subdir. A global `serve_dir` rooted
   there makes `GET /tiles/<source>/places.parquet` resolve on disk (`safe_join`
   blocks `..` escapes, not siblings). There is no tiles-only directory to serve.
2. **Ugly coupling.** A shared `serve_dir` forces the public URL to mirror the
   disk path, producing `.../tiles/<source>/tiles/current/...` (two unrelated
   `tiles` segments — the serving root and the pipeline's per-source subdir).

## The decision: collection-slug-aware route (option 1)

Make the tile-file route resolve a **public kebab slug** → that collection's own
`tiles_dir`. This decouples the public URL from the disk layout entirely: the
snake_case on-disk source names never appear in a URL, and the route can only
reach *inside* a registered collection's tile dir, closing the parquet exposure.
The Phase 2 on-disk layout is **unchanged** (it was byte-validated by the §9
acceptance; do not touch it).

### Slug ↔ collection ↔ disk map

| public slug | collection NSID (config key) | disk source dir |
|---|---|---|
| `foursquare` | `org.atgeo.places.foursquare` | `foursquare/tiles/current` |
| `overture-place` | `org.atgeo.places.overture.place` | `overture_place/tiles/current` |
| `osm` | `org.atgeo.places.osm` | `osm/tiles/current` |
| `overture-division` (future) | `org.atgeo.places.overture.division` | `overture_division/tiles/current` |

Client URL: `https://places.atgeo.org/tiles/overture-place/023010/023010123.json.gz`

Three namespaces, deliberately distinct: NSID dotted, disk source_key snake_case
(private, never leaves the box — that's why the ugly `tiles/current` doubling is
acceptable), public slug kebab.

### Who consumes these ("clients")

External atgeo tile-protocol consumers, **not** browsers, and **currently zero**
(prod serves no tiles yet, so the URL shape is greenfield — no cache or discovery
migration). Flow: a consumer calls XRPC `getCoverage(collection, bbox)`
(`server.py:212`) → gets a sorted list of absolute tile URLs, each built as
`f"{base_url}/{qk[:6]}/{qk}.json.gz"` (`quadtree.py:121`) → GETs those `.json.gz`
URLs (the route below). `base_url` is per-collection config; it is the *only*
knob for the public URL, and it is consumed at runtime from config (not baked
into stored tiles), so changing it is a pure config change.

### WoF is not involved

Deliberately noted to prevent a wrong turn: the tile pipeline's place-tile
containment uses **`overture_division`** boundaries (built in-run — `run_all`
runs division first and writes `overture_division/boundaries.duckdb`,
`quadtree.py:265-338`), *not* WoF. WoF (`wof-boundaries.duckdb`) is only the
server's runtime `boundaries:` reverse-geocode lookup (`config.py:28`,
`server.py:89-96`) — a separate, deprecated feature this task does not touch.

## Changes (garganorn repo)

### Change A — `garganorn/__main__.py`: replace the global-serve_dir route

Remove `serve_dir` and the `@app.route("/tiles/<path:tile_path>")` handler
(currently `__main__.py:83-97`). In `create_app()`, inside the existing
`if tiles_config:` block that loops over `collections` (~`__main__.py:22-41`),
also build a slug map, and add a slug-aware route. Thread `cache_ttl` into a
`Cache-Control` header (it is currently dead config — carried but read nowhere),
and fail fast if `base_url` doesn't match the slug (else `getCoverage` emits URLs
no route can serve).

```python
tile_dirs = {}  # slug -> (tiles_dir, cache_ttl)
for collection, coll_cfg in tiles_config.get("collections", {}).items():
    manifest_path = coll_cfg.get("manifest")
    base_url = coll_cfg.get("base_url")
    slug = coll_cfg.get("slug")
    # Config sanity — fires regardless of manifest presence: a getCoverage URL
    # that no route can serve is a config error, not a dev-checkout state.
    if base_url and slug and not base_url.rstrip("/").endswith("/" + slug):
        raise ValueError(
            f"{collection}: base_url must end with '/{slug}' to match its serving route"
        )
    if manifest_path and not os.path.isfile(manifest_path):
        app.logger.warning(
            "Tile manifest configured for %s but not found: %s "
            "(tile serving disabled for this collection)", collection, manifest_path,
        )
    if manifest_path and os.path.isfile(manifest_path):
        tile_manifests[collection] = TileManifest(manifest_path, coll_cfg["base_url"])
        if "tiles_dir" in coll_cfg:
            tile_collections[collection] = TileBackedCollection(
                collection=collection, manifest_db_path=manifest_path,
                tiles_dir=coll_cfg["tiles_dir"], attribution=coll_cfg.get("attribution", ""),
            )
            # Gate serving on the SAME manifest-exists condition (I1): the route
            # must not serve a collection whose tiles are otherwise disabled.
            if slug:
                tile_dirs[slug] = (coll_cfg["tiles_dir"], coll_cfg.get("cache_ttl"))
            elif base_url:
                app.logger.warning(
                    "Collection %s has base_url but no slug; getCoverage URLs will 404",
                    collection,
                )

@app.route("/tiles/<slug>/<path:tile_path>")
def serve_tile(slug, tile_path):
    entry = tile_dirs.get(slug)
    if entry is None:
        return ("Not found", 404)
    tiles_dir, cache_ttl = entry
    full_path = safe_join(tiles_dir, tile_path)
    if full_path is None or not os.path.isfile(full_path):
        return ("Not found", 404)
    response = send_file(full_path, mimetype="application/json")
    response.headers["Content-Encoding"] = "gzip"
    if cache_ttl:
        # NOT `immutable`: `current` is a symlink repointed each pipeline run, so
        # the same URL can return new bytes; immutable would let caches serve stale
        # tiles for the full max-age. Staleness window == cache_ttl (operator's
        # tradeoff; URLs are not content-hashed). This assignment fully replaces
        # any Cache-Control that send_file sets by default.
        response.headers["Cache-Control"] = f"public, max-age={cache_ttl}"
    return response
```

Notes:
- The `tile_dirs` build is folded **into** the existing manifest-exists block, so a
  slug is serveable iff its manifest exists — matching the existing warn-and-skip
  invariant. The base_url↔slug `ValueError` is the one check that fires
  unconditionally (pure config validation).
- `safe_join(tiles_dir, tile_path)` roots every fetch inside
  `<source>/tiles/current`; siblings (`places.parquet`, `containment/`) are
  unreachable — `..` and URL-encoded `%2e%2e` both resolve to `None` (Werkzeug
  decodes before the handler). Only registered slugs resolve.
- `tiles_dir` is used as-is (relative or absolute). The dev config uses relative
  paths, resolved against the process CWD — consistent with how
  `TileBackedCollection` already uses `tiles_dir` (`tile_reader.py:51`). No new
  CWD assumption is introduced.

### Change B — `garganorn/config.yaml`: new schema (slug, Phase 2 paths, no serve_dir)

Replace the `tiles:` block. Remove `serve_dir`. Add `slug` per collection, point
`manifest`/`tiles_dir` at the Phase 2 `<source>/tiles/current` layout, and set
`base_url` to the kebab slug URL.

```yaml
tiles:
  max_per_tile: 1000
  memory_limit: 48GB
  collections:
    org.atgeo.places.foursquare:
      slug: foursquare
      manifest: tiles/foursquare/tiles/current/manifest.duckdb
      tiles_dir: tiles/foursquare/tiles/current
      base_url: https://places.atgeo.org/tiles/foursquare
      attribution: https://docs.foursquare.com/data-products/docs/access-fsq-os-places
      cache_ttl: 86400
    org.atgeo.places.overture.place:
      slug: overture-place
      manifest: tiles/overture_place/tiles/current/manifest.duckdb
      tiles_dir: tiles/overture_place/tiles/current
      base_url: https://places.atgeo.org/tiles/overture-place
      attribution: https://docs.overturemaps.org/attribution/
      cache_ttl: 86400
    org.atgeo.places.osm:
      slug: osm
      manifest: tiles/osm/tiles/current/manifest.duckdb
      tiles_dir: tiles/osm/tiles/current
      base_url: https://places.atgeo.org/tiles/osm
      attribution: https://www.openstreetmap.org/copyright
      cache_ttl: 86400
  max_coverage_tiles: 50
```

The `tiles/current` doubling is intentional and internal (never in a URL). Dev
manifests won't exist in a checkout, so tile serving stays disabled via the
existing warn-and-skip path (`__main__.py:27-31`) — expected.

## Tests — TDD, write red first (`venv/bin/pytest`, DuckDB 1.2.1)

The existing `serve_dir` tests **will break** and must be rewritten, not merely
added to (no dangling tests):

- **`tests/test_app.py`** — the fixtures `tile_client`/`tile_client_empty`
  (~lines 143-172) and tests `test_tile_served_successfully`/
  `test_tile_missing_returns_404` (~lines 175-189) build `{"serve_dir": ...}`
  configs. Port them to the slug schema. New/updated assertions:
  - `/tiles/<slug>/<qk6>/<qk>.json.gz` resolves to that collection's dir and 200s.
  - Unknown slug → 404.
  - `Cache-Control: public, max-age=86400` present when `cache_ttl`
    set; header absent when the key is omitted.
  - **Traversal at true sibling depth** — port the existing escape test
    (~line 193). `tiles_dir` is `<source>/tiles/current`, so the sibling
    `<source>/places.parquet` is **two** `..` up (`current`→`tiles`→`<source>`):
    `/tiles/overture-place/../../places.parquet` → 404. A cross-source escape is
    three `..`: `/tiles/overture-place/../../../foursquare/tiles/current/x` → 404.
    Include a `%2e%2e`-encoded variant. **Physically create `<source>/places.parquet`
    at the escape target in the fixture** so a 404 proves traversal was blocked,
    not merely that the file was absent (otherwise the test is tautological).
- **`tests/test_config.py`** — a config with `slug` per collection loads; assert
  `base_url.rstrip('/').endswith(slug)` for each; `tiles_dir`/`manifest` carry the
  doubled `tiles/current`. (Verified: test_config.py builds synthetic configs via
  `_write_config(tmp_path, ...)` and does **not** load the real `config.yaml`, so
  Change B breaks nothing here — these are additive positive-coverage assertions.)
- **base_url↔slug validation** — a collection whose `base_url` does not end with
  `/<slug>` → `ValueError` from `create_app`.
- **URL contract (guard)** — `getCoverage` emits `<base_url>/<qk6>/<qk>.json.gz`
  with the kebab `base_url`, and that URL matches the new route pattern.

Final: run the **full** suite (`venv/bin/pytest tests/`), not a subset. Baseline
before starting was 916 passed / 1 xfailed; expect the rewritten tests to keep it
green.

## Implementation pipeline (per Standard rules) — COMPLETE

1. Design → **this doc** → review → gate. PASSED.
2. Red: write the failing tests above → review → gate. PASSED.
3. Green: Changes A + B → review → gate. PASSED.
4. Docs: update this doc's status + any user-facing note if warranted → review. DONE.
5. Full `pytest tests/` — 931 passed, 1 xfailed.
6. Acceptance: pending merge to `main`.

## Out of scope / deferred follow-ups

- **Deploy/infra wiring (separate ticket).** `atgeo-server-config` needs a `tiles:`
  block in `roles/garganorn/templates/config.yaml.j2` (absolute
  `{{ garganorn_home }}/tiles/...` paths, gated by `garganorn_source_*`) and an
  on-demand `tiles.yml` task (tagged `['tiles','never']`, `async: 7200 poll: 30`)
  that runs the pipeline division-first (or via the `run_all` orchestrator) so
  containment reads `overture_division/boundaries.duckdb` — **no WoF/`--boundaries`
  needed**. **Also fix a latent pre-existing bug there:** `config.yaml.j2:8` emits
  `type: overture`, but `config.py` `DATABASE_TYPES` only has `overture_place`, so
  enabling `garganorn_source_overture` crashes boot. Do this when we decide to
  serve tiles in prod.
- Disk source dirs stay snake_case (renaming re-opens the accepted §9 on-disk
  contract for a client-invisible cosmetic gain).
- `overture-division` as a served collection: the slug map accommodates it; only
  place/foursquare/osm register now.
- Pipeline scheduling (cron/timer); envelope amendment (OQ-P2-1); idf
  crash-safety — all separate.
