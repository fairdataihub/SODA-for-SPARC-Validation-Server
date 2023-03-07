from apis import validate_dataset_resource, api_version_resource


def configureRouteHandlers(api):
    """
    Configure the route handlers for the Flask application.
    """
    api.add_namespace(validate_dataset_resource)
    api.add_namespace(api_version_resource)