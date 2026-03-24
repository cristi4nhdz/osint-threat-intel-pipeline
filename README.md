# OSINT Threat Intelligence Pipeline

> **Work In Progress**

This pipeline pulls cybersecurity and threat intel from NewsAPI, AlienVault OTX, RSS feeds, Abuse.ch, and MITRE ATT&CK, pushes raw events into Kafka, runs NLP enrichment using spaCy transformer models, loads enriched data into Snowflake, builds an actor relationship graph in Neo4j, and visualizes everything in a Streamlit dashboard.

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

Pulls cybersecurity and threat intel from NewsAPI, AlienVault OTX, RSS feeds, Abuse.ch, and MITRE ATT&CK, pushes raw events into Kafka, and runs NLP enrichment via spaCy transformer models to extract entities and signals. Enriched output is loaded into Snowflake for storage and Neo4j for relationship graph building. A Streamlit dashboard provides live visualization of threats, actors, IOCs, and geo activity.

---

## Tech Stack

| Layer | Technology |
| --- | --- |
| Language | Python 3.x |
| NLP | spaCy (`en_core_web_trf`) |
| Messaging | Apache Kafka |
| Storage | Snowflake |
| Graph | Neo4j |
| Dashboard | Streamlit, Plotly, D3.js |
| Orchestration | Docker Compose |
| Environment | Conda |
| Linting | flake8, pylint, black, mypy, yamllint |

---

## Features

- **News Ingestion** — Fetches articles from NewsAPI on configurable topics and publishes them to a Kafka topic
- **OTX Ingestion** — Fetches recent threat pulses from AlienVault OTX and publishes them to Kafka on a configurable interval
- **RSS Ingestion** — Fetches articles from threat intelligence RSS feeds and publishes them to Kafka
- **Abuse.ch Ingestion** — Fetches IOC and malware data from Abuse.ch and publishes them to Kafka
- **MITRE ATT&CK Ingestion** — Fetches threat groups from MITRE ATT&CK and publishes them to a dedicated Kafka topic
- **NLP Enrichment** — Entity extraction and threat signal classification using spaCy's `en_core_web_trf` transformer model, with keyword-based matching for threat actors, malware, and attack techniques
- **Relevance Scoring** — Articles are scored and filtered before publishing to the enriched topic
- **Snowflake Storage** — Enriched articles consumed from Kafka and loaded into Snowflake with URL deduplication
- **IOC Storage** — Indicators of compromise loaded from Kafka into Snowflake
- **Neo4j Graph** — Builds an actor relationship graph from enriched articles and MITRE ATT&CK data, linking threat actors, malware, and locations
- **Streamlit Dashboard** — Six-page live dashboard with overview metrics, geo threat map, interactive D3 actor graph, MITRE ATT&CK actor intelligence, IOC explorer, and raw data explorer with CSV export
- **IOC Explorer** — Search and filter IOCs by type, actor, and malware family with article cross-referencing and source breakdown
- **Live Pipeline Status** — Sidebar indicators for Snowflake, Neo4j, Kafka, and data freshness
- **Kafka-Backed Event Bus** — Decoupled producer/consumer architecture for resilience and replay capability
- **Config-Driven** — YAML-based configuration for sources, topics, and pipeline behavior
- **Containerized** — Full Docker Compose setup for local development

---

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) & Docker Compose
- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or Anaconda
- [NewsAPI](https://newsapi.org/) API key
- [AlienVault OTX](https://otx.alienvault.com/) API key
- [Abuse.ch](https://hunting.abuse.ch/api/) API key

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

**Run the OTX ingestion producer:**

```bash
python -m ingestion.run_otx
```

**Run the RSS ingestion producer:**

```bash
python -m ingestion.run_rss
```

**Run the Abuse.ch ingestion producer:**

```bash
python -m ingestion.run_abuse
```

**Run the MITRE ATT&CK ingestion producer:**

```bash
python -m ingestion.run_mitre
```

**Run the enrichment pipeline:**

```bash
python -m processing.run_enrichment
```

**Run the Snowflake loader:**

```bash
python -m storage.run_loader
```

**Run the IOC loader:**

```bash
python -m storage.run_ioc_loader
```

**Run the Neo4j graph loader:**

```bash
python -m storage.run_neo4j
```

**Run the dashboard:**

```bash
streamlit run dashboard/app.py
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
|-- dashboard/            # Streamlit dashboard and page sections
|   |-- _sections/        # Overview, threat map, actor graph, actor intel, IOC explorer, raw data
|-- ingestion/            # News, OTX, RSS, Abuse.ch, and MITRE ATT&CK producers
|-- processing/           # NLP enrichment, entity extraction, Kafka consumer
|-- storage/              # Snowflake, IOC, and Neo4j loaders
|-- docker-compose.yml    # Container orchestration
|-- environment.yaml      # Conda environment spec
|-- README.md
```
