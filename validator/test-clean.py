from sparcur.simple.clean_metadata_files import main as clean_metadata_files
from sparcur.paths import Path
from os.path import expanduser

# path to desktop
path = expanduser("~") + "/Desktop/validator-1000"
path = Path(path)

clean_metadata_files(cleaned_output_path=path)