from dataclasses import dataclass, asdict
import json
# Define a data class for the customer table
@dataclass
class Hit:
    lookup_id: int
    lookup_value: str
    linked_records: str


@dataclass
class SearchResult:
    fields: Hit


