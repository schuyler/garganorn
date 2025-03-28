from flask import Flask
from lexrpc.flask_server import init_flask
from garganorn import Server, OvertureMaps

if __name__ == "__main__":
    db = OvertureMaps("db/overture-maps.duckdb")
    gazetteer = Server(db)
    app = Flask("garganorn")
    init_flask(gazetteer.server, app)
    app.run(debug=True)