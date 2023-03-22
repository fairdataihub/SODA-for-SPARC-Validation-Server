import os
from os.path import expanduser
from argparse import ArgumentParser

from pathlib import Path
from sparcur.paths import Path as SparCurPath
from sparcur.simple.validate import main as validate


parser = ArgumentParser()
parser.add_argument("ds_path", type=str)
args = parser.parse_args()

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
       # stop the subprocess execution by returning "Failed" to the calling process
       return "Failed"

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


val_dataset_local_pipeline(args.ds_path)