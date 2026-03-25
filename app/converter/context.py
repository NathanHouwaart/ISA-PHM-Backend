from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from uuid import uuid4

from isatools.model import OntologyAnnotation, Study


def _normalize_term(value: Any) -> str:
    if value is None:
        return ""
    term = str(value).strip()
    return term


@dataclass
class ConversionContext:
    units_by_term: Dict[str, OntologyAnnotation] = field(default_factory=dict)
    roles_by_term: Dict[str, OntologyAnnotation] = field(default_factory=dict)

    def get_or_create_unit(self, unit_term: Any) -> Optional[OntologyAnnotation]:
        normalized = _normalize_term(unit_term)
        if not normalized:
            return None

        existing = self.units_by_term.get(normalized)
        if existing:
            return existing

        created = OntologyAnnotation(term=normalized, id_=f"#unit/{uuid4()}")
        self.units_by_term[normalized] = created
        return created

    def get_or_create_role(self, role_term: Any) -> Optional[OntologyAnnotation]:
        normalized = _normalize_term(role_term)
        if not normalized:
            return None

        existing = self.roles_by_term.get(normalized)
        if existing:
            return existing

        created = OntologyAnnotation(term=normalized)
        self.roles_by_term[normalized] = created
        return created

    def add_unit_to_study(self, study_obj: Study, unit_term: Any) -> Optional[OntologyAnnotation]:
        unit = self.get_or_create_unit(unit_term)
        if not unit:
            return None

        if all(existing_unit.term != unit.term for existing_unit in study_obj.units):
            study_obj.units.append(unit)

        return unit
