import os, logging
from flask import Flask, abort, send_file
from werkzeug.utils import safe_join
from lexrpc.flask_server import init_flask
from lexrpc.base import XrpcError
from garganorn import Server
from garganorn.config import load_config
from garganorn.boundaries import BoundaryLookup

DEFAULT_CONFIG = "config.yaml"

def create_app():
    config_path = os.getenv("GARGANORN_CONFIG", DEFAULT_CONFIG)
    repo, dbs, boundaries_path, tiles_config = load_config(config_path)

    app = Flask("garganorn")
    app.logger.setLevel(logging.INFO)
    boundaries = BoundaryLookup(boundaries_path) if boundaries_path else None
    tile_manifests = {}
    tile_collections = {}
    max_coverage_tiles = 50
    tile_dirs = {}  # slug -> (tiles_dir, cache_ttl)
    if tiles_config:
        from garganorn.quadtree import TileManifest
        from garganorn.tile_reader import TileBackedCollection
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
                    "Tile manifest configured for %s but not found: %s (tile serving disabled for this collection)",
                    collection, manifest_path,
                )
            if manifest_path and os.path.isfile(manifest_path):
                tile_manifests[collection] = TileManifest(manifest_path, coll_cfg["base_url"])
                if "tiles_dir" in coll_cfg:
                    tile_collections[collection] = TileBackedCollection(
                        collection=collection,
                        manifest_db_path=manifest_path,
                        tiles_dir=coll_cfg["tiles_dir"],
                        attribution=coll_cfg.get("attribution", ""),
                    )
                    # Gate serving on the SAME manifest-exists condition: the route
                    # must not serve a collection whose tiles are otherwise disabled.
                    if slug:
                        tile_dirs[slug] = (coll_cfg["tiles_dir"], coll_cfg.get("cache_ttl"))
                    elif base_url:
                        app.logger.warning(
                            "Collection %s has base_url but no slug; getCoverage URLs will 404",
                            collection,
                        )
        max_coverage_tiles = tiles_config.get("max_coverage_tiles", 50)
    gazetteer = Server(repo, dbs, app.logger, boundaries=boundaries,
                       tile_manifests=tile_manifests, tile_collections=tile_collections,
                       max_coverage_tiles=max_coverage_tiles)
    init_flask(gazetteer.server, app)

    lexicon_map = gazetteer.lexicon_map

    @app.route('/<nsid>')
    def get_lexicon(nsid):
        lexicon = lexicon_map.get(nsid)
        if lexicon is None:
            abort(404)
        return lexicon

    @app.route('/.well-known/did.json')
    def did_document():
        return {
            "id": f"did:web:{gazetteer.repo}",
            "alsoKnownAs": [f"at://{gazetteer.repo}"],
            "service": [
                {
                    "id": "#atproto_pds",
                    "type": "AtprotoPersonalDataServer",
                    "serviceEndpoint": f"https://{gazetteer.repo}",
                }
            ],
        }

    @app.route('/health')
    def health_check():
        return {"status": "ok", "service": "garganorn"}, 200

    @app.route('/<collection>/<path:rkey>')
    def get_resource(collection, rkey):
        try:
            result = gazetteer.get_record({}, gazetteer.repo, collection, rkey)
            return result["value"]
        except XrpcError as e:
            status = 404 if e.name in ("CollectionNotFound", "RecordNotFound") else 400
            return {"error": e.name, "message": str(e)}, status

    @app.route("/tiles/<slug>/<path:tile_path>")
    def serve_tile(slug, tile_path):
        """Serve a gzipped JSON tile file with correct headers."""
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
            # tiles for the full max-age.
            response.headers["Cache-Control"] = f"public, max-age={cache_ttl}"
        return response

    return app

if __name__ == "__main__":
    app = create_app()
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', '8000'))
    app.run(debug=debug, host=host, port=port)
