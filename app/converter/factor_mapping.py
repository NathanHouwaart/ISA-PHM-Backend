from __future__ import annotations

from logging import Logger
from typing import Any, Callable, Dict, List

from isatools.model import Comment, FactorValue, OntologyAnnotation, Study, StudyFactor

from .normalization import as_comment_value


def add_study_factors(study_obj: Study, study_variables: List[Dict[str, Any]]) -> None:
    for variable in study_variables:
        study_factor = StudyFactor(
            name=variable.get("name", ""),
            factor_type=OntologyAnnotation(variable.get("type", "unknown")),
        )
        study_factor.comments.append(Comment(name="description", value=as_comment_value(variable.get("description", ""))))
        study_factor.comments.append(Comment(name="unit", value=as_comment_value(variable.get("unit", ""))))
        study_factor.comments.append(Comment(name="min", value=as_comment_value(variable.get("min", ""))))
        study_factor.comments.append(Comment(name="max", value=as_comment_value(variable.get("max", ""))))
        study_factor.comments.append(Comment(name="step", value=as_comment_value(variable.get("step", ""))))
        study_obj.factors.append(study_factor)


def assign_factor_values(
    study_obj: Study,
    study_payload: Dict[str, Any],
    study_variables: List[Dict[str, Any]],
    study_total_runs: int,
    add_unit_to_study: Callable[[Study, Any], Any],
    logger: Logger,
) -> None:
    for run_number in range(1, study_total_runs + 1):
        sample = study_obj.samples[run_number - 1]

        for variable in study_variables:
            variable_name = variable.get("name", "")
            study_factor = next((factor for factor in study_obj.factors if factor.name == variable_name), None)
            if not study_factor:
                logger.warning("Missing study factor for variable", extra={"variable_name": variable_name})
                continue

            mapping = next(
                (
                    mapping_value
                    for mapping_value in study_payload.get("study_to_study_variable_mapping", [])
                    if mapping_value.get("variableName") == variable_name
                    and mapping_value.get("runNumber") == run_number
                ),
                None,
            )

            if not mapping:
                logger.warning(
                    "No mapping found for run variable",
                    extra={"variable_name": variable_name, "run_number": run_number},
                )
                continue

            factor_value = FactorValue()
            factor_value.factor_name = study_factor
            factor_value.value = mapping.get("value", "unknown")
            factor_value.unit = add_unit_to_study(study_obj, variable.get("unit", ""))
            sample.factor_values.append(factor_value)
