# osint-threat-intel-pipeline

## Download and Activate Libs

- conda env create -f environment.yaml
- conda activate osint
- python -m spacy download en_core_web_trf

### Linting / Flaking / Formatting Commands

- flake8 config_loader.py
- pylint config_loader.py
- black --check app/
- yamllint .\settings.yaml
- mypy .\news_producer.py

### Commands for ingestion

fetch news from newsapi on topics  

- python -m ingestion.run_news

### Check docker kafka topics

- docker exec -it osint-threat-intel-pipeline-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic osint.news --from-beginning --max-messages 50

__NOTE__: To load do -> spacy.load('en_core_web_trf')

### Command to update conda env

- conda env update -f environment.yaml --prune

### Docker start/shutdown commands

- docker compose down
- docker compose up
