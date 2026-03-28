# flows/deploy_flows.py
"""Deploy OSINT flows to Prefect using Python SDK."""

from datetime import timedelta
from prefect import serve
from config.validator import ConfigValidator
from flows.osint_flows import (
    osint_ingestion_flow,
    enrichment_loader_flow,
    ioc_loader_flow,
)

if __name__ == "__main__":
    validator = ConfigValidator()
    if not validator.validate():
        exit(1)
    print("Deploying flows")

    serve(
        # Ingestion flow - every 24 hours
        osint_ingestion_flow.to_deployment(
            name="ingestion",
            interval=timedelta(hours=24),
        ),
        # Enrichment + Storage loader, every 6 hours
        enrichment_loader_flow.to_deployment(
            name="enrichment-loader",
            interval=timedelta(hours=6),
        ),
        # IOC loader, every 6 hours
        ioc_loader_flow.to_deployment(
            name="ioc-loader",
            interval=timedelta(hours=6),
        ),
    )
