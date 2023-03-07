from flask_restx import Namespace
from enum import Enum


# namespaces enums
class NamespaceEnum(Enum):
    VALIDATE_DATASET = "validator"


# namespaces dictionary that is given a namespace name as a key and returns the corresponding namespace object as a value
namespaces = { }

def configure_namespaces():
    """
    Create namespaces for each pysoda file: pysoda ( now manage_datasets), prepare_metadata, etc
    """

    validate_dataset_namespace = Namespace(NamespaceEnum.VALIDATE_DATASET.value, description='Routes for handling validate dataset functionality')
    namespaces[NamespaceEnum.VALIDATE_DATASET] = validate_dataset_namespace



def get_namespace(namespace_name):
    return namespaces[namespace_name]