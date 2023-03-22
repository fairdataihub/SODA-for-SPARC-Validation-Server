import multiprocessing

from namespaces import get_namespace, NamespaceEnum
from flask_restx import Resource
from validator import  val_dataset_local_pipeline, create, has_required_metadata_files
from flask import request

api = get_namespace(NamespaceEnum.VALIDATE_DATASET)

@api.route('/validate')
class ValidateDatasetLocal(Resource):
    @api.doc(responses={201: "Success", 400: "Bad Request", 500: "Internal Server Error"}, 
             description="Create a validation report for a dataset given the constituent pieces of the dataset",
             params={"dataset_structure": "SODA JSON Structure", "manifests": "JSON of a pandas dataframe", "metadata_files": "JSON of a pandas dataframe", "clientUUID": "A unique identifier for creating the folder structure"}
            )
    def post(self):
        """
        Validate a dataset given the constituent pieces by making a skeleton then validating it
        """
        data = request.get_json()
        dataset_structure = data["dataset_structure"]["dataset-structure"]
        manifests = data["manifests"]
        metadata_files = data["metadata_files"]
        clientUUID = data["clientUUID"]

        # 400 of missing one of the arguments
        if not dataset_structure  or not metadata_files or not clientUUID:
            api.abort(400, "Missing required arguments")


        # 400 if missing required metadata files for validation to be successful 
        if not has_required_metadata_files(metadata_files):
            api.abort(400, "Missing required metadata files")

        try:
            generation_location = create(dataset_structure, manifests, metadata_files, clientUUID)
        except Exception as e:
            raise e
        
        with multiprocessing.Pool() as pool:
            pool.map(val_dataset_local_pipeline, [generation_location])

        print("Finished a validation")

        # try:
        #     return val_dataset_local_pipeline(generation_location)
        # except Exception as e:
        #     raise e
        

@api.route('/validate/result/<string:clientUUID>')
class ValidateDatasetLocalResult(Resource):
    def get(self, clientUUID):
        """
        Get the result of a validation report
        """
        pass