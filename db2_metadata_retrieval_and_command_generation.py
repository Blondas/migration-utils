import ibm_db
import csv
import os
import logging
from datetime import datetime
import shutil
from db2_config import DB2_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def connect_to_db():
    """Establish a connection to the DB2 database."""
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
    """Execute a SQL query and return the results."""
    stmt = ibm_db.exec_immediate(conn, sql)
    columns = [ibm_db.field_name(stmt, i) for i in range(ibm_db.num_fields(stmt))]
    results = []
    while ibm_db.fetch_row(stmt):
        row = [str(ibm_db.result(stmt, i)) for i in range(len(columns))]
        results.append(row)
    return columns, results

def save_to_csv(filename, columns, data):
    """Save data to a CSV file."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(data)

def prepare_output_directory():
    """Prepare the output directory, renaming existing one if necessary."""
    out_dir = "./out/sql"
    if os.path.exists(out_dir):
        creation_time = datetime.fromtimestamp(os.path.getctime(out_dir))
        new_name = f"./out/sql_{creation_time.strftime('%Y%m%d_%H%M%S')}"
        shutil.move(out_dir, new_name)
        logger.info(f"Renamed existing output directory to {new_name}")
    os.makedirs(out_dir)
    logger.info(f"Created new output directory: {out_dir}")

def generate_commands(documents_file, metadata_dir):
    """Generate arsadmin retrieve commands based on the documents and metadata."""
    commands = []
    current_command = []
    current_agname = ""
    current_agid_name = ""
    current_pri_sec = ""
    doc_count = 0

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
                    
                    # Check if we need to start a new command
                    if (agname != current_agname or 
                        agid_name != current_agid_name or 
                        f"{pri_nid}-{sec_nid}" != current_pri_sec or 
                        doc_count >= 1000):
                        if current_command:
                            commands.append(" ".join(current_command))
                        current_command = [f"arsadmin retrieve -I LAZARI4 -u t320818 -g {agname} -n {pri_nid}-{sec_nid} -d ./out/data/{agid_name}/"]
                        current_agname = agname
                        current_agid_name = agid_name
                        current_pri_sec = f"{pri_nid}-{sec_nid}"
                        doc_count = 0
                    
                    current_command.append(doc_name)
                    doc_count += 1

    # Add the last command
    if current_command:
        commands.append(" ".join(current_command))

    return commands

def main():
    start_time = datetime.now()
    try:
        prepare_output_directory()
        conn = connect_to_db()
        logger.info("Connected to database")

        # Execute table_list_sql and save results
        table_list_sql = """
        SELECT TRIM(TRANSLATE(ag.name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', '')), ag.agid_name, seg.table_name
        FROM arsag ag
        INNER JOIN arsseg seg ON ag.agid = seg.agid
        WHERE ag.name NOT LIKE 'System%'
        ORDER BY 2, 3
        """
        columns, results = execute_query(conn, table_list_sql)
        save_to_csv('./out/sql/documents.csv', ['AGNAME', 'AGID_NAME', 'TABLE_NAME'], results)
        logger.info(f"Saved table list to ./out/sql/documents.csv. Rows fetched: {len(results)}")

        # Execute table_metadata_sql for each table
        total_rows = 0
        for row in results:
            table_name = row[2]  # Assuming table_name is the third column
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
            columns, metadata_results = execute_query(conn, table_metadata_sql)
            save_to_csv(f'./out/sql/documents_metadata/{table_name}.csv', ['DOC_NAME', 'PRI_NID', 'SEC_NID'], metadata_results)
            total_rows += len(metadata_results)
            logger.info(f"Saved metadata for {table_name} to ./out/sql/documents_metadata/{table_name}.csv")

        logger.info(f"Total rows fetched from all tables: {total_rows}")

        # Generate arsadmin retrieve commands
        commands = generate_commands('./out/sql/documents.csv', './out/sql/documents_metadata')
        with open('./out/arsadmin_retrieve.txt', 'w') as f:
            f.write("\n".join(commands))
        logger.info(f"Generated {len(commands)} arsadmin retrieve commands and saved to ./out/arsadmin_retrieve.txt")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        if 'conn' in locals():
            ibm_db.close(conn)
            logger.info("Database connection closed")

    end_time = datetime.now()
    logger.info(f"Script execution time: {end_time - start_time}")

if __name__ == "__main__":
    main()
