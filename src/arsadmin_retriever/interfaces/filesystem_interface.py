from abc import ABC, abstractmethod
from typing import List, Any

class FilesystemInterface(ABC):
    @abstractmethod
    def ensure_directory_exists(self, directory: str) -> None: ...

    @abstractmethod
    def save_to_csv(self, filename: str, columns: List[str], data: List[List[Any]]) -> None: ...

    @abstractmethod
    def remove_directory(self, path: str) -> None: ...

    @abstractmethod
    def remove_file(self, path: str) -> None: ...

    @abstractmethod
    def get_directory_size(self, path: str) -> int: ...

    @abstractmethod
    def path_exists(self, path: str) -> bool: ...