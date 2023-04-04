import os.path
from os.path import expanduser
from pathlib import Path
from sparcur.paths import Path as SparCurPath
from sparcur.simple.validate import main as validate
from sparcur.simple.clean_metadata_files import main as clean_metadata_files
import shutil
import json 
import copy
import sys




"""
Delete the validation directory for the given clientUUID.
"""
def delete_validation_directory(clientUUID):
    # check if there is a skeleton dataset directory with this clientUUID as the name
    path = os.path.join(expanduser("~"), "SODA", "skeleton", clientUUID)
    if os.path.exists(path):
        # remove the directory and all its contents
        shutil.rmtree(path)


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


def remove_false_positives(parsed_report, blob):

    # remove the 'path_metadata' is a required proeprty error message
    if '#/' in parsed_report:
        messages = parsed_report['#/']['messages']
        # remove the message from the messages list with 'path_metadata' as a substring
        for message in messages:
            if 'path_metadata' in message:
                messages.remove(message)
        


    # remove the #/id error from the parsed report if not dealing with Pennsieve
    # TODO: In time we should be able to pull from Pennsieve then validate correctly. Otherwise just pull their ID ourselves and run a regex. 
    if '#/id' in parsed_report:
        del parsed_report['#/id']



# validate a local dataset at the target directory 
def val_dataset_local_pipeline(ds_path, clientUUID):
    results_path = os.path.join(expanduser("~"), "SODA", "results")
    temp_log_path = os.path.join(expanduser("~"), "validation_progress.txt")

    if not os.path.exists(results_path):
      os.mkdir(results_path)

    user_results_file = os.path.join(results_path, f"{clientUUID}.json")
    if os.path.exists(user_results_file):
      os.remove(user_results_file)

    print("About to clean metadata files")
    # clean the manifest and metadata files to prevent hanging caused by openpyxl trying to open manifest/metadata files with 
    # excessive amounts of empty rows/columns
    skeleton_path = os.path.join(expanduser("~"), "SODA", "skeleton", clientUUID)
    clean_metadata_files(path=SparCurPath(skeleton_path), cleaned_output_path=SparCurPath(skeleton_path))
    print("Cleaned metadata files")


    # write to a file that we got this far
    with open(temp_log_path, "w") as f:
       f.write("Cleaned the metadata files")
      
    # convert the path to absolute from user's home directory
    joined_path = os.path.join(expanduser("~"), ds_path.strip())

    # check that the directory exists 
    valid_directory = os.path.isdir(joined_path)

    # give user an error 
    if not valid_directory:
        raise OSError(f"The given directory does not exist: {joined_path}")

    # convert to Path object for Validator to function properly
    norm_ds_path = Path(joined_path)

    print("Starting validation")


    # validate the dataset
    blob = None 
    try: 
        blob = validate(norm_ds_path)
    except Exception as e:
       delete_validation_directory(ds_path) 
       # write the results to a json file 
       results = {"status": "Error", "error": str(e), "parsed_report": {}, "full_report": {}}
       # write results to a json file
       with open(user_results_file, "w") as f:
            json.dump(results, f)
       # we are done now
       return

    print("Finished validation")

    # write to a file that we got this far
    with open(temp_log_path, "w") as f:
       f.write("Created the blob") 

    # delete_validation_directory(ds_path)

    if 'status' not in blob or 'path_error_report' not in blob['status']:
        # namespace_logger.info(f"{clientUUID}: 4.1 Validation Run Incomplete ( Guided: True )")
        results = {"status": "Incomplete", "parsed_report": {}, "full_report": str(blob)}
        # write results to a json file
        with open(user_results_file, "w") as f:
              json.dump(results, f)
        return 
    
    # namespace_logger.info(f"{clientUUID}: 4.2 Parsing dataset results( Guided: True ) ")
    
    # peel out the status object 
    status = blob.get('status')

    # peel out the path_error_report object
    path_error_report = status.get('path_error_report')

    # get the errors out of the report that do not have errors in their subpaths (see function comments for the explanation)
    parsed_report = parse(path_error_report)  

    # remove any false positives from the report
    # TODO: Implement the below function
    remove_false_positives(parsed_report, blob)

    # write to a file that we got this far
    with open(temp_log_path, "w") as f:
       f.write("Parsed and Removed False Positives")

    results = {"status": "Complete", "parsed_report": parsed_report, "full_report": str(blob)}
    with open(user_results_file, "w") as f:
      json.dump(results, f)



if __name__ == '__main__':
    # get the args passed in from subprocess call
    args = sys.argv[1:]
    generation_location = args[0]
    clientUUID = args[1]

    val_dataset_local_pipeline(generation_location, clientUUID)