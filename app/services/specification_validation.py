"""Run shared draft checks that need only the converted ISA-JSON."""

import logging
import tempfile
from pathlib import Path

CHECKS = ("isa_schema", "phm_protocols", "phm_metadata", "phm_experiment_type", "phm_sensors")
LIMITATION = "Dataset files and paths were not checked: measurement files are not uploaded for conversion."
logger = logging.getLogger("isa_phm_backend")


def unavailable(message: str) -> dict:
    return {
        "profile": "draft", "specification_version": None, "conformant": None,
        "status": "unavailable", "checks": {}, "not_selected": list(CHECKS),
        "limitations": [message, LIMITATION],
    }


def validate_export(raw_json: str) -> dict:
    try:
        from validator import validate_dataset
    except ImportError:
        logger.warning("Shared specification validator could not be imported", exc_info=True)
        return unavailable("The shared ISA-PHM validator is not available on the conversion service.")

    try:
        with tempfile.TemporaryDirectory(prefix="isa-phm-validation-") as directory:
            path = Path(directory) / "export.isa-phm.json"
            path.write_text(raw_json, encoding="utf-8")
            report = validate_dataset(path, checks=CHECKS).to_dict()
    except Exception:
        logger.exception("Shared specification validation failed")
        return unavailable("The shared ISA-PHM validator failed to run; contact the service administrator.")
    report["limitations"].append(LIMITATION)
    return report
