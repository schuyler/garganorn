from flask import Flask
from lexrpc.flask_server import init_flask
from garganorn.server import server

if __name__ == "__main__":
    app = Flask("garganorn")
    init_flask(server, app)
    app.run(debug=True)