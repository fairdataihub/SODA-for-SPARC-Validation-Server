"""
Given a sodaJSONObject dataset-structure key, create a skeleton of the dataset structure on the user's filesystem. 
Then pass the path to the skeleton to the validator.
Works within Organize Datasets to allow a user to validate their dataset before uploading it to Pennsieve/Generating it locally.
"""

# create a dummy file in the current directory  
# this is used to check if the script is running in the correct directory

from flask import Flask, abort
import os
import shutil
from xml.dom import InvalidStateErr
import copy
from os.path import expanduser
from pathlib import Path
from sparcur.paths import Path as SparCurPath
from sparcur.simple.validate import main as validate
import pandas as pd 
import json 


app = Flask(__name__)



path = os.path.join(expanduser("~"), "SODA", "skeleton")
completed_jobs_dir = os.path.join(expanduser("~"), "SODA", "completed_jobs")



def has_required_metadata_files(metadata_files_json):
    """
    Checks that the dataset has the required metadata files. These are: dataset_description.xlsx, 
    subjects.xlsx, samples.xlsx, and submission.xlsx. 
    """

    REQUIRED_METADATA_FILES = {
        "submission": False, 
        "dataset_description": False,
        "subjects": False
    }

    for file_name, _ in metadata_files_json:
        if file_name in REQUIRED_METADATA_FILES:
            REQUIRED_METADATA_FILES[file_name] = True
            

    # return True if all the required metadata files are present
    return all(REQUIRED_METADATA_FILES.values())

def create_validation_error_message(base_message, ds_path):
    error_message = base_message
    if not has_required_metadata_files(ds_path):
        error_message += "Please make sure that you have the required metadata files in your dataset."
    error_message += f"To view the raw report, please see the validation.json file in your SODA folder at {expanduser('~')}/SODA/validation.json"
    return error_message

def create_skeleton(dataset_structure, path):
    """
    Create a skeleton of the dataset structure on the user's filesystem.
    """
    for folder in dataset_structure["folders"]:
        dp = (os.path.join(path, folder)) 
        if not os.path.exists(dp):
            os.mkdir(dp)

        create_skeleton(dataset_structure["folders"][folder], os.path.join(path, folder))
    for file_key in dataset_structure["files"]:
        # TODO: If the type is bf then create a generic file with the name of the file key ( and write information to it )
        # if dataset_structure["files"][file_key]["type"] in ["bf"]:
        #     continue
            
        with open(os.path.join(path, file_key), "w") as f:
            f.write("SODA")

def validate_validation_result(export):
    """
        Verifies the integriy of an export retrieved from remote or generated locally.
        Input: export - A dictionary with sparcur.simple.validate or remote validation results.
    """

    # 1. check if the export was not available for retrieval yet even afer waiting for the current maximum wait time
    if export is None:
        raise InvalidStateErr("We had trouble validating your dataset. Please try again. If the problem persists, please contact us at help@fairdataihub.org.")

    # 2. check if the export was a failed validation run TODO: discern between a failed validation run and a dataset with no metadata files 
    inputs = export.get('inputs')

    # NOTE: May not be possible to be None but just in case
    if inputs is None:
        InvalidStateErr("Please add metadata files to your dataset to receive a validation report.")

# # return the errors from the error_path_report that should be shown to the user.
# # as per Tom (developer of the Validator) for any paths (the keys in the Path_Error_Report object)
# # return the ones that do not have any errors in their subpaths. 
# # e.g., If given #/meta and #/meta/technique keys only return #/meta/technique (as this group doesn't have any subpaths)
def parse(error_path_report):

  user_errors = copy.deepcopy(error_path_report)

  keys = error_path_report.keys()

  # go through all paths and store the paths with the longest subpaths for each base 
  # also store matching subpath lengths together
  for k in keys:
    prefix = get_path_prefix(k)

    # check if the current path has inputs as a substring
    if prefix.find("inputs") != -1:
      # as per Tom ignore inputs paths' so
      # remove the given prefix with 'inputs' in its path
      del user_errors[k]
      continue 

    # check for a suffix indicator in the prefix (aka a forward slash at the end of the prefix)
    if prefix[-1] == "/":
      # if so remove the suffix and check if the resulting prefix is an existing path key
      # indicating it can be removed from the errors_for_users dictionary as the current path
      # will be an error in its subpath -- as stated in the function comment we avoid these errors 
      prefix_no_suffix_indicator = prefix[0 : len(prefix) - 1]

      if prefix_no_suffix_indicator in user_errors:
        del user_errors[prefix_no_suffix_indicator]


  
  return user_errors
  

def get_path_prefix(path):
  if path.count('/') == 1:
    # get the entire path as the "prefix" and return it
    return path
  # get the path up to the final "/" and return it as the prefix
  final_slash_idx = path.rfind("/")
  return path[:final_slash_idx + 1]

### START OF SCRIPT ###

def create_manifests(manifest_struct, path):
   # go through the high level keys
   for key in manifest_struct:
      manifest_obj = manifest_struct[key]

      manifest_df = pd.DataFrame(json.loads(manifest_obj))

      # write to the ~/SODA/skeleton/key folder
      manifest_df.to_excel(f"{path}/{key}/manifest.xlsx", index=False)

def create_metadata_files(metadata_struct, path):
   for metadata_file_name in metadata_struct:
      metadata_obj = metadata_struct[metadata_file_name]

      metadata_df = pd.DataFrame(metadata_obj)

      metadata_df.to_excel(f"{path}/{metadata_file_name}.xlsx", index=False, engine="openpyxl")

def create(dataset_structure, manifests_struct, metadata_files, clientUUID):
    """
    Creates a skeleton dataset ( a set of empty data files but with valid metadata files ) of the given soda_json_structure on the local machine.
    Used for validating a user's dataset before uploading it to Pennsieve.
    NOTE: This function is only used for validating datasets ( both local and on Pennsieve ) that are being organized in the Organize Datasets feature of SODA.
    The reason for this being that those datasets may exist in multiple locations on a user's filesystem ( or even on multiple machines ) and therefore cannot be validated 
    until they have been put together in a single location.

    """
    path = os.path.join(expanduser("~"), "SODA", "skeleton")

    # check if the skeleton directory exists
    if not os.path.exists(path):
        # create the skeleton directory
        os.makedirs(path)

    # check if the unique path for the client exists
    path = os.path.join(path, clientUUID)
    if os.path.exists(path):
        # remove the directory and all its contents
        shutil.rmtree(path)
    
    # create the directory for the client
    os.mkdir(path)

    create_skeleton(dataset_structure, path)

    # use pandas to parse the manifest files as data frames then write them to the correct folder
    create_manifests(manifests_struct, path)

    # create metadata files 
    create_metadata_files(metadata_files, path)

    return path


# validate a local dataset at the target directory 
def val_dataset_local_pipeline(ds_path):
    # convert the path to absolute from user's home directory
    joined_path = os.path.join(expanduser("~"), ds_path.strip())

    # check that the directory exists 
    valid_directory = os.path.isdir(joined_path)

    # give user an error 
    if not valid_directory:
        raise OSError(f"The given directory does not exist: {joined_path}")

    # convert to Path object for Validator to function properly
    norm_ds_path = Path(joined_path)

    # validate the dataset
    blob = None 
    try: 
        blob = validate(norm_ds_path)
    except Exception as e:
       abort(500, e)

    if 'status' not in blob or 'path_error_report' not in blob['status']:
        return {"parsed_report": {}, "full_report": str(blob), "status": "Incomplete"}
    
    # peel out the status object 
    status = blob.get('status')

    # peel out the path_error_report object
    path_error_report = status.get('path_error_report')

    # get the errors out of the report that do not have errors in their subpaths (see function comments for the explanation)
    parsed_report = parse(path_error_report)  

    # remove any false positives from the report
    # TODO: Implement the below function
    # parsed_report = remove_false_positives(parsed_report, blob)

    return {"parsed_report": parsed_report, "full_report": str(blob), "status": "Complete"}





if __name__ == '__main__':
    app.run(host='127.0.0.1', port=9000)

    

    

    
      




    