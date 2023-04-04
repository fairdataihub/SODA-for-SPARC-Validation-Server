from namespaces import get_namespace, NamespaceEnum
from flask_restx import Resource
from validator import  create, has_required_metadata_files, createGuidedMode, delete_validation_directory
from flask import request
import subprocess
import os.path
from os.path import expanduser
import json 

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

        guided_mode = False


        if "clientUUID" not in data:
            api.abort(400, "Request is missing required clientUUID argument")

        if "dataset_structure" not in data:
            api.abort(400, f"{clientUUID}: Missing required argument dataset_structure")
        

        if "manifests" not in data:
            api.abort(400, f"{clientUUID}: Missing required argument manifests")

        if "metadata_files" not in data:
            api.abort(400, f"{clientUUID}: Missing required argument metadata_files")


        if "guided-options" in data["dataset_structure"]:
            guided_mode = True


        dataset_structure = data["dataset_structure"]["dataset-structure"]
        manifests = data["manifests"]
        metadata_files = data["metadata_files"]
        clientUUID = data["clientUUID"]

        api.logger.info(f"{clientUUID}: Starting validation ( Guided: {guided_mode} ) ")

        # 400 if missing required metadata files for validation to be successful
        if not guided_mode:
            api.logger.info(f"{clientUUID}: Checking required metadata files ( Guided: {guided_mode} ) ")
            if not has_required_metadata_files(metadata_files):
                api.abort(400, f"{clientUUID}: Missing required metadata files")


        try:
            api.logger.info(f"{clientUUID}: 1. Creating skeleton dataset ( Guided: {guided_mode} ) ")
            if "guided-options" in data["dataset_structure"]:
                generation_location = createGuidedMode(data["dataset_structure"], clientUUID, manifests)
            else:
                generation_location = create(dataset_structure, manifests, metadata_files, clientUUID)
        except Exception as e:
            # remove any directory that was created, if created
            delete_validation_directory(clientUUID)
            api.abort(500, f"{clientUUID}: {e}")


        try:
            api.logger.info(f"{clientUUID}: 4. Validating the dataset ( Guided: {guided_mode} ) ")
            # return val_dataset_local_pipeline(generation_location, clientUUID)
            subprocess.Popen(["python3", "validate.py", generation_location, clientUUID])
            print("We are done early")
        except Exception as e:
            # remove the directory that was created, if created
            delete_validation_directory(clientUUID)
            api.abort(500, f"{clientUUID}: {e}")
        

@api.route('/results/<string:clientUUID>')
class ValidateDatasetLocalResult(Resource):
    def get(self, clientUUID):
        """
        Get the result of a validation report
        """
        results_path = os.path.join(expanduser("~"), "SODA", "results")
        user_file_path = os.path.join(results_path, f"{clientUUID}.json")

        # check if a file exists in the results directory with the given clientUUID
        if not os.path.exists(user_file_path):
            return {"status": "WIP", "parsed_report": {}, "full_report": {}}
        
        # read the file and return the contents
        results = {}
        with open(user_file_path, "r") as f:
            results = json.load(f)

        # remove the results file 
        os.remove(user_file_path)

        return results
