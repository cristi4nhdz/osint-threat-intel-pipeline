FROM continuumio/miniconda3

WORKDIR /app

COPY . /app

RUN conda env create -f environment.yaml

SHELL ["conda", "run", "-n", "osint", "/bin/bash", "-c"]

RUN python -m spacy download en_core_web_trf

CMD ["conda", "run", "--no-capture-output", "-n", "osint", "python", "-m", "flows.deploy_flows"]