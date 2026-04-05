# flows/osint_flows.py
"""OSINT ingestion, enrichment, and storage flows for Prefect orchestration."""

import subprocess
import sys
import os
from prefect import flow, task

# encoding fix for windows
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"


# tasks for ingestion
@task
def run_news():
    result = subprocess.run(
        ["python", "-m", "ingestion.run_news"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"News ingestion failed: {result.stderr}")


@task
def run_otx():
    result = subprocess.run(
        ["python", "-m", "ingestion.run_otx"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"OTX ingestion failed: {result.stderr}")


@task
def run_rss():
    result = subprocess.run(
        ["python", "-m", "ingestion.run_rss"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"RSS ingestion failed: {result.stderr}")


@task
def run_abuse():
    result = subprocess.run(
        ["python", "-m", "ingestion.run_abuse"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Abuse.ch ingestion failed: {result.stderr}")


@task
def run_mitre():
    result = subprocess.run(
        ["python", "-m", "ingestion.run_mitre"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"MITRE ingestion failed: {result.stderr}")


# tasks for processing
@task
def run_enrichment():
    result = subprocess.run(
        ["python", "-m", "processing.run_enrichment"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Enrichment failed: {result.stderr}")


# tasks for storage
@task
def run_snowflake_loader():
    result = subprocess.run(
        ["python", "-m", "storage.run_loader"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Snowflake loading failed: {result.stderr}")


@task
def run_neo4j_loader():
    result = subprocess.run(
        ["python", "-m", "storage.run_neo4j"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Neo4j loading failed: {result.stderr}")


@task
def run_embedding_loader():
    result = subprocess.run(
        ["python", "-m", "storage.run_embedding_loader"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Embedding loading failed: {result.stderr}")


@task
def run_ioc_loader():
    result = subprocess.run(
        ["python", "-m", "storage.run_ioc_loader"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"IOC loading failed: {result.stderr}")


@task
def run_s3_archive():
    result = subprocess.run(
        ["python", "-m", "storage.run_s3_archiver"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"S3 Archiving failed: {result.stderr}")


# flows
@flow
def osint_ingestion_flow():
    run_news()
    run_otx()
    run_rss()
    run_abuse()
    run_mitre()


@flow
def enrichment_loader_flow():
    run_enrichment()
    run_snowflake_loader()
    run_neo4j_loader()
    run_embedding_loader()


@flow
def ioc_loader_flow():
    run_ioc_loader()


@flow
def s3_archive_flow():
    run_s3_archive()
