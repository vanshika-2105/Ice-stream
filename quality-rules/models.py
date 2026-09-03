from dataclasses import dataclass, field
from typing import List


@dataclass
class ValidationError:
    field: str
    code: str
    message: str


@dataclass
class ValidationResult:
    valid: bool
    event_id: str | None = None
    errors: List[ValidationError] = field(default_factory=list)