from typing import List, Tuple, cast
from src.arsadmin_retriever.interfaces.database_interface import DatabaseInterface
import ibm_db
from src.arsadmin_retriever.models.config.db2_config import DB2Config

class DB2Database(DatabaseInterface):
    def __init__(self, config: DB2Config):
        self.config = config
        self.conn = None

    def connect(self) -> None:
        conn_string: str = f"DATABASE={self.config.database};"

        if self.config.hostname:
            conn_string += f"HOSTNAME={self.config.hostname};"
        if self.config.port:
            conn_string += f"PORT={self.config.port};"
        if self.config.username:
            conn_string += f"UID={self.config.username};"
        if self.config.password:
            conn_string += f"PWD={self.config.password};"

        conn_string += "PROTOCOL=TCPIP;"

        self.conn = ibm_db.connect(conn_string, "", "")

    def disconnect(self) -> None:
        if self.conn:
            ibm_db.close(self.conn)
            self.conn = None

    def execute_query(self, sql: str) -> Tuple[List[str], List[List[str]]]:
        if not self.conn:
            raise RuntimeError("Database connection not established")

        stmt = ibm_db.exec_immediate(self.conn, sql)
        columns: list[str] = [ibm_db.field_name(stmt, i) for i in range(ibm_db.num_fields(stmt))]
        results = []
        while ibm_db.fetch_row(stmt):
            row = [str(ibm_db.result(stmt, i)) for i in range(len(columns))]
            results.append(row)
        return columns, results

    def get_table_list(self) -> List[Tuple[str, str, str]]:
        table_list_sql: str = """
        SELECT TRIM(TRANSLATE(ag.name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', '')), 
               ag.agid_name, 
               seg.table_name
        FROM arsag ag
        INNER JOIN arsseg seg ON ag.agid = seg.agid
        WHERE ag.name NOT LIKE 'System%'
        ORDER BY 2, 3
        """
        _, results  = self.execute_query(table_list_sql)
        return cast(List[Tuple[str, str, str]], results)

    def get_table_metadata(self, table_name: str) -> List[Tuple[str, str, str]]:
        table_metadata_sql: str = f"""
        SELECT DISTINCT doc_name, pri_nid, sec_nid
        FROM {table_name}

        UNION

        SELECT DISTINCT
        CAST(resource AS VARCHAR(10)),
        pri_nid, sec_nid
        FROM {table_name}
        WHERE resource > 0

        UNION

        SELECT DISTINCT
        TRIM(TRANSLATE(doc_name, '', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')) ||
        LEFT(TRIM(TRANSLATE(doc_name, '', '0123456789')), 3) || '1',
        pri_nid, sec_nid
        FROM {table_name}
        """
        _, results = self.execute_query(table_metadata_sql)
        return cast(List[Tuple[str, str, str]], results)