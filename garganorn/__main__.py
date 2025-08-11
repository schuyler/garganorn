import os, logging
from flask import Flask
from lexrpc.flask_server import init_flask
from garganorn import Server, OvertureMaps, FoursquareOSP

dbs = [
    OvertureMaps("db/overture-maps.duckdb"),
    FoursquareOSP("db/fsq-osp.duckdb")
]

if __name__ == "__main__":
    app = Flask("garganorn")
    app.logger.setLevel(logging.INFO)
    gazetteer = Server("gazetteer.social", dbs, app.logger)
    init_flask(gazetteer.server, app)
    
    # Add a simple health check endpoint
    @app.route('/health')
    def health_check():
        return {"status": "ok", "service": "garganorn"}, 200
    
    # Configure for Docker/production environment
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', '8000'))
    
    app.run(debug=debug, host=host, port=port)