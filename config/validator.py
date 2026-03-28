"""Configuration validation for settings.yaml with direct logging."""

import sys
import os
from pathlib import Path
from typing import Any, Dict
import logging
import yaml

os.makedirs("/app/logs", exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler("/app/logs/validate.log")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s")
)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s")
)
logger.addHandler(console_handler)


class ConfigValidator:
    """Validates configuration settings for settings.yaml."""

    REQUIRED_FIELDS = {
        "kafka": ["bootstrap_servers", "topics"],
        "kafka.topics": ["news", "mitre", "enriched", "iocs"],
        "snowflake": ["account", "user", "password", "database", "schema", "warehouse"],
        "neo4j": ["uri", "user", "password"],
        "aws": ["bucket_name", "region", "access_key_id", "secret_access_key"],
        "apis": ["newsapi_key", "alienvault_otx_key", "abuse_ch"],
        "nlp": ["spacy_model", "confidence_threshold"],
    }

    NUMERIC_RANGES = {
        "nlp.confidence_threshold": (0.0, 1.0),
    }

    PLACEHOLDER_KEYWORDS = ["YOUR_", "_HERE", "PLACEHOLDER"]

    def __init__(self, config_path: str = "config/settings.yaml"):
        self.config_path = Path(config_path)
        self.config: Dict[str, Any] = {}

    def load_config(self) -> bool:
        """Load and parse config file."""
        if not self.config_path.exists():
            logger.error("Config file not found: %s", self.config_path)
            return False
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f)
            logger.info("Configuration file loaded successfully")
            return True
        except yaml.YAMLError as e:
            logger.error("YAML parse error: %s", e)
            return False

    def validate_required_fields(self) -> bool:
        """Check all required fields exist."""
        valid = True
        for field_path, subfields in self.REQUIRED_FIELDS.items():
            # Navigate nested fields
            parts = field_path.split(".")
            current = self.config
            for part in parts:
                if part not in current:
                    logger.error("Missing required field: %s", field_path)
                    valid = False
                    break
                current = current[part]
            if isinstance(current, dict):
                for subfield in subfields:
                    if subfield not in current:
                        logger.error(
                            "Missing required field: %s.%s", field_path, subfield
                        )
                        valid = False
        return valid

    def validate_placeholder_values(self) -> bool:
        """Check for placeholder values that weren't replaced."""
        valid = True

        def check_placeholders(obj: Any, path: str = ""):
            nonlocal valid
            if isinstance(obj, dict):
                for key, value in obj.items():
                    check_placeholders(value, f"{path}.{key}" if path else key)
            elif isinstance(obj, str):
                for keyword in self.PLACEHOLDER_KEYWORDS:
                    if keyword in obj:
                        logger.error(
                            "Placeholder value detected at %s: %s", path, obj[:50]
                        )
                        valid = False

        check_placeholders(self.config)
        return valid

    def validate_numeric_ranges(self) -> bool:
        """Check numeric values are in valid ranges."""
        valid = True
        for field_path, (min_val, max_val) in self.NUMERIC_RANGES.items():
            parts = field_path.split(".")
            current = self.config
            for part in parts:
                if part not in current:
                    break
                current = current[part]
            else:
                # Field exists, check range
                if not isinstance(current, (int, float)):
                    logger.error(
                        "%s must be numeric, got %s", field_path, type(current)
                    )
                    valid = False
                elif not (min_val <= current <= max_val):
                    logger.error(
                        "%s must be between %s and %s, got %s",
                        field_path,
                        min_val,
                        max_val,
                        current,
                    )
                    valid = False
        return valid

    def validate(self) -> bool:
        """Run all validations."""
        logger.info("Starting configuration validation")

        if not self.load_config():
            return False

        results = [
            self.validate_required_fields(),
            self.validate_placeholder_values(),
            self.validate_numeric_ranges(),
        ]

        valid = all(results)
        if valid:
            logger.info("Configuration validation passed")
        else:
            logger.error("Configuration validation failed")

        return valid


def main():
    """CLI entry point for config validation."""
    validator = ConfigValidator()
    if validator.validate():
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
