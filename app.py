from flask import Flask, request
# from flask_cors import CORS
from namespaces import configure_namespaces
from flask_restx import Resource
import sys

configure_namespaces()

from setupUtils import (configureLogger, configureRouteHandlers, configureAPI)

app = Flask(__name__)

configureLogger(app)

app.logger.info("Starting SODA-for-SPARC-Validation-Server")

api = configureAPI()

configureRouteHandlers(api)

api.init_app(app)

@api.route("/sodaforsparc-validator_shutdown", endpoint="shutdown")
class Shutdown(Resource):
    def get(self):
        func = request.environ.get("werkzeug.server.shutdown")
        api.logger.info("Shutting down server")

        if func is None:
            print("Not running with the Werkzeug Server")
            return

        func()

if __name__ == '__main__':
    api.logger.info(f"Starting server on port {9009}")
    app.run(host="127.0.0.1", port=9009)