import os
import shutil
import csv
from typing import List, Any
from src.arsadmin_retriever.interfaces.filesystem_interface import FilesystemInterface

class LocalFilesystem(FilesystemInterface):
    def ensure_directory_exists(self, directory: str) -> None:
        os.makedirs(directory, exist_ok=True)

    def save_to_csv(self, filename: str, columns: List[str], data: List[List[Any]]) -> None:
        self.ensure_directory_exists(os.path.dirname(filename))
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(data)

    def remove_directory(self, path: str) -> None:
        if os.path.exists(path):
            shutil.rmtree(path)

    def remove_file(self, path: str) -> None:
        if os.path.exists(path):
            os.remove(path)

    def get_directory_size(self, path: str) -> int:
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp: str = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp) # type: ignore
        return total_size

    def path_exists(self, path: str) -> bool:
        return os.path.exists(path)