from sparcur.paths import Path as SparCurPath
import os.path 
from sparcur.simple.clean_metadata_files import main as clean_metadata_files
from sparcur.simple.validate import main as validate


path = os.path.join(os.path.expanduser("~"), "validator-60000")
# output_path = os.path.join(os.path.expanduser("~"), "validator-1000-cleaned")

# # # clean the metadata files to prevent vaidator hanging on large datasets
# clean_metadata_files(path=SparCurPath(path), cleaned_output_path=SparCurPath(output_path))

blob = validate(path)

print("WE DONE NOW")


