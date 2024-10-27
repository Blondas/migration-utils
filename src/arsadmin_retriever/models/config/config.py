from generator_config import GeneratorConfig
from executor_config import ExecutorConfig
from db2_config import DB2Config
from dataclasses import dataclass
import yaml
from pathlib import Path

@dataclass
class Config:
    commands: Path
    db_config: DB2Config
    generator_config: GeneratorConfig
    executor_config: ExecutorConfig

    @classmethod
    def load_from_yaml(cls, config_path: str) -> Config:
        with open(config_path, 'r') as file:
            config_data = yaml.safe_load(file)

        db_config = DB2Config(**config_data['db_config'])
        generator_config = GeneratorConfig(**config_data['generator_config'])
        executor_config = ExecutorConfig(**config_data['executor_config'])

        return cls(
            commands=Path(config_data['commands']),
            db_config=db_config,
            generator_config=generator_config,
            executor_config=executor_config
        )
