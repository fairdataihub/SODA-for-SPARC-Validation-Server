from flask import Flask, request
# from flask_cors import CORS
from namespaces import configure_namespaces
from flask_restx import Resource
import sys
from werkzeug.middleware.proxy_fix import ProxyFix

configure_namespaces()

from setupUtils import (configureLogger, configureRouteHandlers, configureAPI)

app = Flask(__name__)

configureLogger(app)

app.logger.info("Starting SODA-for-SPARC-Validation-Server")

api = configureAPI()

configureRouteHandlers(api)

@api.route("/hi")
class Shutdown(Resource):
    def get(self):
        return "Hello"

app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

api.init_app(app)


if __name__ == '__main__':
    api.logger.info(f"Starting server on port {4000}")
    app.run(host="127.0.0.1", port=4000)