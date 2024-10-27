from dataclasses import dataclass
from pathlib import Path

@dataclass
class ExecutorConfig:
    data_dir: Path
    state: Path
    state_save_interval: int
    min_free_space_percent: float
    workers: int
