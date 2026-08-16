import gzip, os, logging
from flask import Flask, abort, request, Response
from werkzeug.utils import safe_join
from lexrpc.flask_server import init_flask
from lexrpc.base import XrpcError
from garganorn import Server
from garganorn.config import load_config

DEFAULT_CONFIG = "config.yaml"

def create_app():
    config_path = os.getenv("GARGANORN_CONFIG", DEFAULT_CONFIG)
    repo, tiles_config = load_config(config_path)

    app = Flask("garganorn")
    app.logger.setLevel(logging.INFO)
    tile_manifests = {}
    tile_collections = {}
    max_coverage_tiles = 50
    tile_dirs = {}  # slug -> tiles_dir
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
            # Completeness guard: a run is only ready to serve when BOTH
            # manifest.duckdb AND its sibling manifest.json (the completeness
            # marker, written last) exist. `and` short-circuits so
            # os.path.dirname never sees manifest_path=None.
            ready = (
                bool(manifest_path)
                and os.path.isfile(manifest_path)
                and os.path.isfile(os.path.join(os.path.dirname(manifest_path), "manifest.json"))
            )
            if manifest_path and not ready:
                app.logger.warning(
                    "Tile run for %s is incomplete or missing (no manifest.json completeness "
                    "marker); tile serving disabled for this collection", collection,
                )
            if ready:
                run_dir = os.path.realpath(os.path.dirname(manifest_path))
                stamp = os.path.basename(run_dir)
                tiles_root = os.path.dirname(run_dir)
                tile_manifests[collection] = TileManifest(manifest_path, f"{base_url}/{stamp}")
                if "tiles_dir" in coll_cfg:
                    tile_collections[collection] = TileBackedCollection(
                        collection=collection,
                        manifest_db_path=manifest_path,
                        tiles_dir=run_dir,
                        source_url=coll_cfg.get("source", ""),
                        license_url=coll_cfg.get("license", ""),
                    )
                    # Gate serving on the SAME readiness condition: the route
                    # must not serve a collection whose tiles are otherwise disabled.
                    if slug:
                        tile_dirs[slug] = tiles_root
                    elif base_url:
                        app.logger.warning(
                            "Collection %s has base_url but no slug; getCoverage URLs will 404",
                            collection,
                        )
        max_coverage_tiles = tiles_config.get("max_coverage_tiles", 50)
    gazetteer = Server(repo, app.logger,
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
        """Serve a tile as plain JSON, decompressed from its on-disk gzip
        file -- storage format and wire format are separate decisions;
        Caddy owns wire compression negotiation via Accept-Encoding."""
        if not tile_path.endswith(".json.gz"):
            return ("Not found", 404)
        tiles_dir = tile_dirs.get(slug)
        if tiles_dir is None:
            return ("Not found", 404)
        full_path = safe_join(tiles_dir, tile_path)
        if full_path is None or not os.path.isfile(full_path):
            return ("Not found", 404)
        with gzip.open(full_path, "rb") as f:
            data = f.read()
        response = Response(data, mimetype="application/json")
        response.headers["Cache-Control"] = "public, max-age=604800, immutable"
        return response

    @app.after_request
    def add_coverage_cache_control(response):
        if (
            request.endpoint == "xrpc-endpoint"
            and request.view_args.get("nsid") == "org.atgeo.getCoverage"
            and response.status_code == 200
        ):
            response.headers["Cache-Control"] = "public, max-age=3600"
        return response

    return app

if __name__ == "__main__":
    app = create_app()
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', '8000'))
    app.run(debug=debug, host=host, port=port)
