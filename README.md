# OSINT Threat Intelligence Pipeline

> **Work In Progress**

A real-time cybersecurity intelligence pipeline that ingests threat data from 5 sources across 4 Kafka topics, enriches articles using spaCy NLP with 200+ mapped threat actors and 100+ malware families, stores enriched data in Snowflake and Neo4j, and displays insights through a 6-page Streamlit dashboard, with 3 Prefect flows automating the entire pipeline on a scheduled basis.

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

Pulls cybersecurity and threat intel from NewsAPI, AlienVault OTX, RSS feeds, Abuse.ch, and MITRE ATT&CK, pushes raw events into Kafka, and runs NLP enrichment via spaCy transformer models to extract entities and signals. Enriched output is loaded into Snowflake for storage and Neo4j for relationship graph building across 200+ threat actors. A 6-page Streamlit dashboard provides live visualization of threats, actors, IOCs, and geo activity. 3 Prefect flows orchestrate the entire pipeline automatically inside Docker.

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
| Orchestration | Prefect, Docker Compose |
| Testing | pytest, pytest-cov |
| Environment | Conda |
| Linting | flake8, pylint, black, mypy, yamllint |

---

## Features

- **5-Source Ingestion** — NewsAPI, AlienVault OTX, RSS feeds, Abuse.ch, and MITRE ATT&CK all publishing to Kafka
- **NLP Enrichment** — Entity extraction and threat signal classification using spaCy's `en_core_web_trf` transformer model, with keyword-based matching across 200+ threat actors and 100+ malware families
- **Relevance Scoring** — Articles scored 0.0–1.0 and filtered before publishing to the enriched topic
- **Snowflake Storage** — Enriched articles loaded into Snowflake with automatic table setup and URL deduplication
- **IOC Storage** — Indicators of compromise loaded from Kafka into Snowflake every 6 hours
- **Neo4j Graph** — Actor relationship graph linking 200+ threat actors to malware, locations, and origin countries, refreshed every 6 hours via Prefect
- **6-Page Streamlit Dashboard** — Overview metrics, geo threat map, interactive D3 actor graph, MITRE ATT&CK actor intelligence, IOC explorer, and raw data explorer with CSV export, served via Docker
- **IOC Explorer** — Search and filter IOCs by type, actor, and malware family with article cross-referencing and source breakdown
- **Live Pipeline Status** — Sidebar indicators for Snowflake, Neo4j, Kafka, and data freshness
- **Prefect Orchestration** — 3 scheduled flows running in Docker: ingestion every 24 hours, enrichment and storage loading every 6 hours, IOC loading every 6 hours
- **Kafka-Backed Event Bus** — Uses 4 Kafka topics with decoupled producers and consumers to improve reliability and enable message replay
- **Test Suite** — pytest tests covering entity extraction, producers, consumers, and loaders with mocked Kafka, Snowflake, and Neo4j connections
- **Config-Driven** — YAML-based configuration for sources, topics, and pipeline behavior
- **Containerized** — Full Docker Compose setup for Kafka, Neo4j, Prefect server, flow runner, and dashboard

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

**3. Configure your environment:**

```bash
cp config/settings.example.yaml config/settings.yaml
# edit settings.yaml with your keys
```

### Running the Pipeline

**Start the full pipeline:**

```bash
docker compose up
```

This starts Kafka, Neo4j, the Prefect server at `http://localhost:4200`, the Streamlit dashboard at `http://localhost:8501`, and automatically deploys and runs all 3 scheduled flows:

- `osint-ingestion-flow` — runs all 5 ingestion producers every 24 hours
- `enrichment-loader-flow` — runs NLP enrichment, Snowflake loader, and Neo4j graph builder every 6 hours
- `ioc-loader-flow` — loads new IOCs into Snowflake every 6 hours

**Run tests:**

```bash
pytest
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
|-- flows/                # Prefect flow definitions and deployment script
|-- ingestion/            # News, OTX, RSS, Abuse.ch, and MITRE ATT&CK producers
|-- processing/           # NLP enrichment, entity extraction, Kafka consumer
|-- storage/              # Snowflake, IOC, and Neo4j loaders
|-- tests/                # pytest test suite for ingestion, processing, and storage
|-- docker-compose.yml    # Container orchestration
|-- Dockerfile            # Container image definition
|-- environment.yaml      # Conda environment spec
|-- README.md
```
