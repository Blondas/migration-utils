from typing import List, Tuple
from src.arsadmin_retriever.models.config.config import Config
from src.arsadmin_retriever.models.command import Command
from src.arsadmin_retriever.interfaces.database_interface import DatabaseInterface
from src.arsadmin_retriever.interfaces.filesystem_interface import FilesystemInterface
from logging import Logger

class CommandGenerator:
    config: Config
    database: DatabaseInterface
    filesystem: FilesystemInterface
    logger: Logger

    def __init__(self, config: Config, database: DatabaseInterface, filesystem: FilesystemInterface, logger: Logger):
        self.config = config
        self.database = database
        self.filesystem = filesystem
        self.logger = logger

    def generate_commands(self) -> List[Command]:
        self.database.connect()
        try:
            table_list = self.database.get_table_list()
            self._save_table_list(table_list)
            self._save_tables_metadata(table_list)
            return self._generate_arsadmin_commands(table_list)
        finally:
            self.database.disconnect()

    def _save_table_list(self, table_list: List[Tuple[str, str, str]]) -> None:
        documents_file = os.path.join(self.config.out_dir, 'documents.csv')
        self.filesystem.save_to_csv(documents_file, ['AGNAME', 'AGID_NAME', 'TABLE_NAME'], table_list)
        self.logger.info(f"Saved table list to {documents_file}")

    def _save_tables_metadata(self, table_list: List[Tuple[str, str, str]]) -> None:
        total_metadata_rows = 0
        for _, _, table_name in table_list:
            metadata = self.database.get_table_metadata(table_name)
            total_metadata_rows += len(metadata)
            metadata_file = os.path.join(self.config.tables_metadata_dir, f"{table_name}.csv")
            self.filesystem.save_to_csv(metadata_file, ['DOC_NAME', 'PRI_NID', 'SEC_NID'], metadata)
            self.logger.info(f"Saved metadata for {table_name} to {metadata_file}")
        self.logger.info(f"Total metadata rows fetched: {total_metadata_rows}")

    def _generate_arsadmin_commands(self, table_list: List[Tuple[str, str, str]]) -> List[Command]:
        commands = []
        for agname, agid_name, table_name in table_list:
            metadata_file = os.path.join(self.config.tables_metadata_dir, f"{table_name}.csv")
            with open(metadata_file, 'r') as mf:
                metadata_reader = csv.DictReader(mf)
                current_command = None
                current_doc_count = 0
                current_pri_sec = None

                for metadata_row in metadata_reader:
                    doc_name = metadata_row['DOC_NAME']
                    pri_nid = metadata_row['PRI_NID']
                    sec_nid = metadata_row['SEC_NID']

                    if (current_doc_count >= 1000 or
                            (current_pri_sec and current_pri_sec != f"{pri_nid}-{sec_nid}")):
                        if current_command:
                            commands.append(current_command)
                        current_command = None
                        current_doc_count = 0

                    if not current_command:
                        current_command = Command(
                            base_command=f"arsadmin retrieve -I ODLAHD01 -u admin -g {agname}",
                            pri_sec=f"{pri_nid}-{sec_nid}",
                            output_dir=f"./out/data/{agid_name}/",
                            doc_names=[]
                        )
                    current_command.doc_names.append(doc_name)
                    current_doc_count += 1
                    current_pri_sec = f"{pri_nid}-{sec_nid}"

                if current_command:
                    commands.append(current_command)

        return commands