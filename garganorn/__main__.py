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
    max_coverage_tiles = 50
    if tiles_config:
        from garganorn.quadtree import TileManifest
        for collection, coll_cfg in tiles_config.get("collections", {}).items():
            tile_manifests[collection] = TileManifest(coll_cfg["manifest"], coll_cfg["base_url"])
        max_coverage_tiles = tiles_config.get("max_coverage_tiles", 50)
    gazetteer = Server(repo, dbs, app.logger, boundaries=boundaries,
                       tile_manifests=tile_manifests, max_coverage_tiles=max_coverage_tiles)
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

    serve_dir = None
    if tiles_config:
        serve_dir = tiles_config.get("serve_dir")

    @app.route("/tiles/<path:tile_path>")
    def serve_tile(tile_path):
        """Serve a gzipped JSON tile file with correct headers."""
        if serve_dir is None:
            return ("Not found", 404)
        full_path = safe_join(serve_dir, tile_path)
        if full_path is None or not os.path.isfile(full_path):
            return ("Not found", 404)
        response = send_file(full_path, mimetype="application/json")
        response.headers["Content-Encoding"] = "gzip"
        return response

    return app

if __name__ == "__main__":
    app = create_app()
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', '8000'))
    app.run(debug=debug, host=host, port=port)
