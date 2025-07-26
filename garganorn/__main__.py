from flask import Flask
from lexrpc.flask_server import init_flask
from garganorn import Server, OvertureMaps, FoursquareOSP

dbs = [
    OvertureMaps("db/overture-maps.duckdb"),
    FoursquareOSP("db/fsq-osp.duckdb")
]

if __name__ == "__main__":
    gazetteer = Server("gazetteer.social", dbs)
    app = Flask("garganorn")
    init_flask(gazetteer.server, app)
    app.run(debug=True)