from dataclasses import dataclass
from pathlib import Path

@dataclass
class GeneratorConfig:
    tables_metadata_dir: Path
    agid_name_subfoler_size: int