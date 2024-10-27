from abc import ABC, abstractmethod
from typing import List, Tuple, Any

class DatabaseInterface(ABC):
    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def disconnect(self) -> None: ...

    @abstractmethod
    def execute_query(self, sql: str) -> Tuple[List[str], List[List[Any]]]: ...

    @abstractmethod
    def get_table_list(self) -> List[Tuple[str, str, str]]: ...

    @abstractmethod
    def get_table_metadata(self, table_name: str) -> List[Tuple[str, str, str]]: ...