from dataclasses import dataclass
from typing import List

@dataclass
class Command:
    index: int
    base_command: str
    doc_names: List[str]
    output_dir: str