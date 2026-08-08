# Garganorn

Garganorn is intended to be a test bed for experimenting with adding location data to the ATmosphere.

Currently, the project implements an ATProtocol XRPC server designed to serve static location datasets ("gazetteers").

**WARNING: This code has not been formally released and interfaces WILL change without warning. YMMV. Patches welcome.**

The project is named after the earliest recorded [mammoth goose](https://en.wikipedia.org/wiki/Garganornis).

![Garganornis ballmanni](https://upload.wikimedia.org/wikipedia/commons/thumb/c/c5/Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg/374px-Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg)

## Configuration

Garganorn loads its data sources from a YAML config file. By default it looks for `config.yaml` in the current directory, or you can set the `GARGANORN_CONFIG` environment variable to point elsewhere. Copy [`config.yaml.example`](config.yaml.example) to `config.yaml` and edit it for your environment — `config.yaml` is gitignored since it's deployment-specific.

```yaml
repo: places.atgeo.org
tiles:
  collections:
    org.atgeo.places.overture.place:
      slug: overture-place
      manifest: data/overture_place/tiles/current/manifest.duckdb
      tiles_dir: data/overture_place/tiles/current
      base_url: https://places.atgeo.org/tiles/overture-place
```

## Data import

Look in [`scripts/import-overture-extract.sh`](scripts/import-overture-extract.sh) for an example of how to import data.

Building one of these databases takes a few minutes for a reasonable bounding box on a reasonable machine with a reasonable Internet connection. You must build at least one database locally for the service to have data to serve.

If `db/density.parquet` exists at import time, places are assigned density-based importance scores for ranking. Category IDF is computed inline during import from the places table itself. If the density file is absent, importance defaults to 0.

The density table is an optional artifact built separately from a global places dataset. This produces a versioned parquet file in `db/` with a symlink (`density.parquet`). Rebuilding is rarely needed — global density patterns change slowly.

See [`docs/s2_duckdb_design.md`](docs/s2_duckdb_design.md) for design details.

## Tile export pipeline

`python -m garganorn.quadtree` builds quadtree tile exports from parquet data. It takes a subcommand: `run` builds one source end to end, `all` builds every source named in a config file, and `density`, `idf`, and `covering` build the shared artifacts the sources depend on. Invoking it with no subcommand is an error — use `run`.

Each source produces a timestamped directory of gzipped JSON tile files under `<output>/<source>/tiles/<timestamp>/`, with a `<output>/<source>/tiles/current` symlink pointing to the latest run. Each stage also writes a parquet artifact alongside — `places.parquet`, `tile_assignments.parquet`, `containment/` — and skips itself when that artifact is still newer than its inputs, so re-running only rebuilds what changed. Pass `--force` to rebuild regardless.

Supported sources (for `run --source`):

| `--source` | Input | Collection |
|---|---|---|
| `overture_place` | `--parquet <glob>` | `org.atgeo.places.overture.place` |
| `osm` | `--parquet-dir <dir>` | `org.atgeo.places.osm` |
| `overture_division` | `--division-parquet <path> --division-area-parquet <path>` | `org.atgeo.places.overture.division` |

### overture_division

Imports Overture Maps administrative boundaries from the `division` and `division_area` parquet themes. Produces two outputs:

- **Tile files** under `<output>/overture_division/tiles/current/` — one gzipped JSON file per quadtree tile, each record carrying a `community.lexicon.location.bbox` location and attributes (subtype, country, region, admin_level, wikidata, population).
- **`boundaries.duckdb`** at `<output>/overture_division/boundaries.duckdb` — a DuckDB file with an R-tree spatial index for point-in-polygon containment queries. Used by the venue tile pipeline via `--boundaries`.

```
python -m garganorn.quadtree run \
  --source overture_division \
  --division-parquet /data/overture/division.parquet \
  --division-area-parquet /data/overture/division_area.parquet \
  --output /srv/data
```

To enrich another source's tiles with division containment (adds `relations.within` to each record):

```
python -m garganorn.quadtree run \
  --source overture_place \
  --parquet '/data/overture/places/*.parquet' \
  --boundaries /srv/data/overture_division/boundaries.duckdb \
  --output /srv/data
```

Optional arguments (`run`, all sources):

| Argument | Default | Description |
|---|---|---|
| `--bbox XMIN YMIN XMAX YMAX` | none | Restrict import to a bounding box |
| `--memory-limit` | `48GB` | DuckDB memory limit |
| `--max-per-tile` | `1000` | Maximum records per tile |
| `--temp-directory` | DuckDB default | Volume DuckDB spills to when a stage exceeds `--memory-limit` |
| `--max-temp-directory-size` | `250GB` | Ceiling on that spill; a runaway query fails with "temp directory full" instead of filling the volume |
| `--export-workers` | CPU count | Threads for tile gzip compression |
| `--force` | off | Rebuild every stage, ignoring artifact freshness |
| `--config` | none | YAML config file; `run` reads `memory_limit`, `max_per_tile`, `temp_directory`, and `max_temp_directory_size` from the `pipeline:` section (`all` reads the rest) |

Point `--temp-directory` at a volume with room to spare: a global import spills
tens of gigabytes, and left unset DuckDB spills wherever its default lands —
which may be the root filesystem. `--max-temp-directory-size` bounds the damage
when a query spills more than expected.

To build every configured source in one shot:

```
python -m garganorn.quadtree all --config config.yaml
```

`all` runs, in order: the shared density extract, per-source category IDF, `overture_division`, then the remaining sources. It reads the `pipeline:` section of the config (paths, `memory_limit`, `max_per_tile`, `temp_directory`, `max_temp_directory_size`, `bbox`, and the per-source inputs); the `tiles:` section is server-side config only.

## Running the server

Install and start a Flask dev server on `localhost:8000`:

```
pip install -e .
python -m garganorn
```

For production, use gunicorn:

```
gunicorn "garganorn.__main__:create_app()" --bind 0.0.0.0:8000 --workers 2
```

## Querying the XRPC service

The collection name for each data source is set by the database class. For Overture Maps it's `org.atgeo.places.overture.place`.

### getRecord

```
$ curl 'http://127.0.0.1:8000/xrpc/com.atproto.repo.getRecord?repo=places.atgeo.org&collection=org.atgeo.places.overture.place&rkey=<id>'
```

## Proposed Lexicon schemas

* [`org.atgeo.place`](garganorn/lexicon/place.json)
* [`community.lexicon.location.geo`](garganorn/lexicon/geo.json)
* [`community.lexicon.location.address`](garganorn/lexicon/address.json)

**NOTE**: These schemas are only *proposed*, and have not been adopted by the Lexicon community.

## Lexicon dependencies

* `com.atproto.repo.getRecord`

## Development

As aforementioned, this project is under development and should not be used for production purposes. I intend to try to track the work of the lexicon.community ATGeo working group as it evolves.

Patches are extremely welcome.

Come find us in the BlueSky API Touchers Discord.

## License etc.

It's MIT licensed, yo. See [LICENSE](LICENSE) for details. If it breaks, you get to keep the pieces.
