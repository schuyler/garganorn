import os, logging
from flask import Flask, abort
from lexrpc.flask_server import init_flask
from lexrpc.base import XrpcError
from garganorn import Server
from garganorn.config import load_config
from garganorn.boundaries import BoundaryLookup

DEFAULT_CONFIG = "config.yaml"

def create_app():
    config_path = os.getenv("GARGANORN_CONFIG", DEFAULT_CONFIG)
    repo, dbs, boundaries_path = load_config(config_path)

    app = Flask("garganorn")
    app.logger.setLevel(logging.INFO)
    boundaries = BoundaryLookup(boundaries_path) if boundaries_path else None
    gazetteer = Server(repo, dbs, app.logger, boundaries=boundaries)
    init_flask(gazetteer.server, app)

    lexicon_map = {lex["id"]: lex for lex in gazetteer.lexicons}

    @app.route('/<nsid>')
    def get_lexicon(nsid):
        lexicon = lexicon_map.get(nsid)
        if lexicon is None:
            abort(404)
        return lexicon

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

    return app

if __name__ == "__main__":
    app = create_app()
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', '8000'))
    app.run(debug=debug, host=host, port=port)
