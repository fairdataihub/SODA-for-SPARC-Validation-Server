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


if __name__ == '__main__':
    api.logger.info(f"Starting server on port {9009}")
    app.run(host="127.0.0.1", port=9009)