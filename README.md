# osint-threat-intel-pipeline

### Download and Activate Libs 
- conda env create -f environment.yaml
- conda activate osint
- python -m spacy download en_core_web_trf

### Linting / Flaking / Formatting Commands
- flake8 config_loader.py
- pylint config_loader.py
- black --check app/

__NOTE__: To load do -> spacy.load('en_core_web_trf')