import ibm_db
import csv
import os
import datetime
import shutil

from db2_config import DB2_CONFIG

def connect_to_db():
    conn_string = (
        f"DATABASE={DB2_CONFIG['database']};"
        f"HOSTNAME={DB2_CONFIG['hostname']};"
        f"PORT={DB2_CONFIG['port']};"
        f"PROTOCOL=TCPIP;"
        f"UID={DB2_CONFIG['username']};"
        f"PWD={DB2_CONFIG['password']};"
    )
    return ibm_db.connect(conn_string, "", "")

def execute_query(conn, sql):
    stmt = ibm_db.exec_immediate(conn, sql)
    columns = [ibm_db.field_name(stmt, i) for i in range(ibm_db.num_fields(stmt))]
    results = []
    while ibm_db.fetch_row(stmt):
        row = [str(ibm_db.result(stmt, i)) for i in range(len(columns))]
        results.append(row)
    return columns, results

def save_to_csv(filename, columns, data):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(data)

def prepare_output_directory():
    if os.path.exists('./out/sql'):
        creation_time = os.path.getctime('./out/sql')
        datetime_str = datetime.datetime.fromtimestamp(creation_time).strftime('%Y%m%d_%H%M%S')
        shutil.move('./out/sql', f'./out/sql_{datetime_str}')
    os.makedirs('./out/sql/documents_metadata', exist_ok=True)

def generate_commands(documents_file, metadata_dir):
    commands = []
    current_command = []
    current_command_doc_count = 0
    current_agname = ""
    current_agid_name = ""
    current_pri_sec_nid = ""

    with open(documents_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            agname = row['AGNAME']
            agid_name = row['AGID_NAME']
            table_name = row['TABLE_NAME']

            metadata_file = os.path.join(metadata_dir, f"{table_name}.csv")
            with open(metadata_file, 'r') as mf:
                metadata_reader = csv.DictReader(mf)
                for metadata_row in metadata_reader:
                    doc_name = metadata_row['DOC_NAME']
                    pri_nid = metadata_row['PRI_NID']
                    sec_nid = metadata_row['SEC_NID']

                    if (agname != current_agname or 
                        agid_name != current_agid_name or 
                        f"{pri_nid}-{sec_nid}" != current_pri_sec_nid or 
                        current_command_doc_count >= 1000):
                        if current_command:
                            commands.append(" ".join(current_command))
                        current_command = [
                            f"arsadmin retrieve -I LAZARI4 -u t320818 -g {agname}",
                            f"-n {pri_nid}-{sec_nid}",
                            f"-d ./out/data/{agid_name}/"
                        ]
                        current_command_doc_count = 0
                        current_agname = agname
                        current_agid_name = agid_name
                        current_pri_sec_nid = f"{pri_nid}-{sec_nid}"

                    current_command.append(doc_name)
                    current_command_doc_count += 1

    if current_command:
        commands.append(" ".join(current_command))

    return commands

def main():
    try:
        prepare_output_directory()
        conn = connect_to_db()
        print("Connected to database")

        # Execute table_list_sql
        table_list_sql = """
        SELECT TRIM(TRANSLATE(ag.name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', '')), ag.agid_name, seg.table_name
        FROM arsag ag
        INNER JOIN arsseg seg ON ag.agid = seg.agid
        WHERE ag.name NOT LIKE 'System%'
        ORDER BY 2, 3
        """
        columns, results = execute_query(conn, table_list_sql)
        print(f"Fetched {len(results)} rows from table_list_sql")
        save_to_csv('./out/sql/documents.csv', ['AGNAME', 'AGID_NAME', 'TABLE_NAME'], results)

        total_metadata_rows = 0
        for row in results:
            table_name = row[2]
            table_metadata_sql = f"""
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
            columns, results = execute_query(conn, table_metadata_sql)
            total_metadata_rows += len(results)
            save_to_csv(f'./out/sql/documents_metadata/{table_name}.csv', ['DOC_NAME', 'PRI_NID', 'SEC_NID'], results)

        print(f"Total metadata rows fetched: {total_metadata_rows}")

        # Generate commands
        commands = generate_commands('./out/sql/documents.csv', './out/sql/documents_metadata')
        
        with open('./out/arsadmin_retrieve.txt', 'w') as f:
            for command in commands:
                f.write(f"{command}\n")

        print(f"Successfully created {len(commands)} entries in arsadmin_retrieve.txt")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if 'conn' in locals():
            ibm_db.close(conn)
            print("Connection closed")

if __name__ == "__main__":
    main()
