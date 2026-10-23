import gzip, json, os, logging
from flask import Flask, abort, jsonify, request, Response
from werkzeug.exceptions import HTTPException
from werkzeug.utils import safe_join
from lexrpc.flask_server import init_flask
from lexrpc.base import NSID_RE, XrpcError
from garganorn import Server
from garganorn.config import load_config

DEFAULT_CONFIG = "config.yaml"

def create_app():
    config_path = os.getenv("GARGANORN_CONFIG", DEFAULT_CONFIG)
    repo, tiles_config, public_base = load_config(config_path)

    app = Flask("garganorn")
    app.logger.setLevel(logging.INFO)
    tilesets = {}  # slug -> ServedTileset, in config order
    tile_dirs = {}  # slug -> directory holding the run directories
    max_tiles = 50
    if tiles_config:
        from garganorn.quadtree import TileManifest
        from garganorn.server import ServedTileset
        from garganorn.tile_reader import TileBackedCollection
        for cfg in tiles_config.get("tilesets", []):
            # A run is only ready to serve when manifest.json (the completeness
            # marker, written last) exists beside manifest.duckdb and tileset.json.
            run_dir = os.path.realpath(cfg["run_dir"])
            if not all(os.path.isfile(os.path.join(run_dir, f))
                       for f in ("manifest.duckdb", "manifest.json", "tileset.json")):
                app.logger.warning(
                    "Tile run %s is incomplete or missing; tile serving disabled for it",
                    cfg["run_dir"],
                )
                continue
            with open(os.path.join(run_dir, "tileset.json")) as f:
                descriptor = json.load(f)
            slug = descriptor["slug"]
            base_url = cfg["base_url"]
            if not base_url.rstrip("/").endswith("/" + slug):
                raise ValueError(
                    f"base_url must end with '/{slug}' to match its serving route"
                )
            if slug in tilesets:
                raise ValueError(f"two tilesets share the slug '{slug}'")
            manifest_path = os.path.join(run_dir, "manifest.duckdb")
            stamp = os.path.basename(run_dir)
            tilesets[slug] = ServedTileset(
                descriptor,
                TileManifest(manifest_path, f"{base_url}/{stamp}"),
                TileBackedCollection(manifest_path, run_dir),
            )
            tile_dirs[slug] = os.path.dirname(run_dir)
        max_tiles = tiles_config.get("max_tiles", 50)
    gazetteer = Server(repo, tilesets, max_tiles, public_base)
    init_flask(gazetteer.server, app)

    lexicon_map = gazetteer.lexicon_map

    def not_found(error, message):
        """A 404 with the same {error, message} JSON body shape XRPC errors use.
        The plain-HTTP routes answer with this, never Flask's HTML page."""
        response = jsonify({"error": error, "message": message})
        response.status_code = 404
        return response

    @app.errorhandler(HTTPException)
    def http_error_as_json(e):
        """Every HTTP error Flask or Werkzeug raises -- unmatched routes,
        wrong methods, 500s, and the ones lexrpc's XRPC view lets through
        (415 for a body that isn't JSON, 400 for malformed JSON, 405 for a
        method the route doesn't take) -- gets the {error, message} JSON
        body instead of Werkzeug's HTML page. Errors lexrpc returns itself
        (InvalidRequest, MethodNotImplemented, the lexicon errors) never
        reach this handler. Redirects are handled by Flask before it."""
        name = e.name.replace(" ", "")
        if e.code >= 500:
            # Flask logged an uncaught exception's traceback already; an
            # explicit abort(5xx) has none, so log it. The body stays generic.
            if getattr(e, "original_exception", None) is None:
                app.logger.error("HTTP %s %s: %s", e.code, e.name, e.description)
            message = "The server hit an unexpected error."
        else:
            message = e.description
            if e.code == 400 and request.path.startswith("/xrpc/"):
                name = "InvalidRequest"  # atproto's name for a malformed request
        response = jsonify({"error": name, "message": message})
        response.status_code = e.code
        for header, value in e.get_headers():  # keeps Allow on a 405
            if header.lower() != "content-type":
                response.headers[header] = value
        return response

    @app.route('/<nsid>')
    def get_lexicon(nsid):
        if not NSID_RE.fullmatch(nsid):
            abort(404)  # /favicon.ico, /xrpc, /tiles...: not a lexicon request
        lexicon = lexicon_map.get(nsid)
        if lexicon is None:
            return not_found("LexiconNotFound", f"Unknown lexicon: {nsid}")
        return lexicon

    @app.route('/.well-known/did.json')
    def did_document():
        return {
            "id": f"did:web:{gazetteer.repo}",
            "service": [
                {
                    "id": "#atgeo_tiles",
                    "type": "AtgeoTileService",
                    "serviceEndpoint": gazetteer.public_base,
                }
            ],
        }

    @app.route("/tilesets/<slug>")
    def tileset_descriptor(slug):
        tileset = gazetteer.tilesets.get(slug)
        if tileset is None:
            return not_found("TilesetNotFound", f"Unknown tileset: {slug}")
        return tileset.descriptor

    @app.route("/<dataset>/<collection>/<id>")
    def place(dataset, collection, id):
        urn = f"urn:atgeo:{dataset}:{collection}:{id}"
        if "." not in dataset:
            return not_found("PlaceNotFound", f"No such place: {urn}")
        try:
            return gazetteer.get_place(None, urn)
        except XrpcError as e:
            return not_found(e.name, e.message)

    @app.route('/health')
    def health_check():
        return {"status": "ok", "service": "garganorn"}, 200

    @app.route("/tiles/<slug>/<path:tile_path>")
    def serve_tile(slug, tile_path):
        """Serve a tile as plain JSON, decompressed from its on-disk gzip
        file -- storage format and wire format are separate decisions;
        Caddy owns wire compression negotiation via Accept-Encoding."""
        tiles_dir = tile_dirs.get(slug)
        if tiles_dir is None:
            return not_found("TilesetNotFound", f"Unknown tileset: {slug}")
        full_path = safe_join(tiles_dir, tile_path) if tile_path.endswith(".json.gz") else None
        if full_path is None or not os.path.isfile(full_path):
            return not_found("TileNotFound", f"No such tile: {slug}/{tile_path}")
        with gzip.open(full_path, "rb") as f:
            data = f.read()
        response = Response(data, mimetype="application/json")
        response.headers["Cache-Control"] = "public, max-age=604800, immutable"
        return response

    CACHED_QUERIES = {"org.atgeo.getTiles", "org.atgeo.describeService"}

    @app.after_request
    def allow_cross_origin_reads(response):
        # Assignment, not add: lexrpc's XRPC views already set this header.
        response.headers["Access-Control-Allow-Origin"] = "*"
        # Error messages reflect request paths; never let a browser sniff them as HTML.
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.after_request
    def add_cache_control(response):
        if (
            request.endpoint == "xrpc-endpoint"
            and request.view_args.get("nsid") in CACHED_QUERIES
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
