# OSINT Threat Intelligence Pipeline

> **Work In Progress**

This pipeline pulls cybersecurity and threat intel news from NewsAPI, pushes raw events into Kafka, and runs NLP enrichment using spaCy's transformer models to extract entities and signals. The enriched output feeds into a dashboard for review. Not done yet, but the core ingestion and enrichment stages are in progress.

---

## Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Environment Setup](#environment-setup)
  - [Running the Pipeline](#running-the-pipeline)
- [Project Structure](#project-structure)

---

## Overview

Pulls cybersecurity and threat intel news from NewsAPI, pushes raw events into Kafka, and runs NLP enrichment via spaCy transformer models to extract entities and signals. Enriched output feeds into a dashboard for review. Core ingestion and enrichment are working, dashboard is still being built out.

---

## Tech Stack

| Layer | Technology |
| --- | --- |
| Language | Python 3.x |
| NLP | spaCy (`en_core_web_trf`) |
| Messaging | Apache Kafka |
| Storage | Snowflake |
| Orchestration | Docker Compose |
| Environment | Conda |
| Linting | flake8, pylint, black, mypy, yamllint |

---

## Features

- **News Ingestion** — Fetches articles from NewsAPI on configurable topics and publishes them to a Kafka topic
- **NLP Enrichment** — Entity extraction and threat signal classification using spaCy's `en_core_web_trf` transformer model, with keyword-based matching for threat actors, malware, and attack techniques
- **Relevance Scoring** — Articles are scored and filtered before publishing to the enriched topic
- **Snowflake Storage** *(in progress)* — Enriched articles written to a Snowflake table with threat intel fields and relevance score
- **Kafka-Backed Event Bus** — Decoupled producer/consumer architecture for resilience and replay capability
- **Config-Driven** — YAML-based configuration for sources, topics, and pipeline behavior
- **Containerized** — Full Docker Compose setup for local development

---

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) & Docker Compose
- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or Anaconda
- A [NewsAPI](https://newsapi.org/) API key

### Environment Setup

**1. Clone the repository:**

```bash
git clone https://github.com/cristi4nhdz/osint-threat-intel-pipeline.git
cd osint-threat-intel-pipeline
```

**2. Create and activate the Conda environment:**

```bash
conda env create -f environment.yaml
conda activate osint
```

**3. Download the spaCy transformer model:**

```bash
python -m spacy download en_core_web_trf
```

**4. Configure your environment:**

Copy or edit the settings file with your API keys and topic preferences:

```bash
cp config/settings.example.yaml config/settings.yaml
# edit settings.yaml with your keys
```

### Running the Pipeline

**Start the infrastructure:**

```bash
docker compose up
```

**Run the Snowflake setup:**

```bash
python -m storage.snowflake_setup
```

**Run the news ingestion producer:**

```bash
python -m ingestion.run_news
```

**Run the enrichment pipeline:**

```bash
python -m processing.run_enrichment
```

**Shut down:**

```bash
docker compose down
```

---

## Project Structure

```text
osint-threat-intel-pipeline/
|-- config/               # YAML configuration files
|-- dashboard/            # Dashboard consumer and visualization
|-- ingestion/            # News fetching and Kafka producer
|-- processing/           # NLP enrichment, entity extraction, Kafka consumer
|-- storage/              # Snowflake setup and writer
|-- docker-compose.yml    # Container orchestration
|-- environment.yaml      # Conda environment spec
|-- README.md
```
