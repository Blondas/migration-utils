import ibm_db
import csv
import os
import logging
import shutil
from datetime import datetime
from db2_config import DB2_CONFIG

# Set up logging
def setup_logging():
    log_dir = './out/logs'
    os.makedirs(log_dir, exist_ok=True)
    
    # Configure logging to console and file
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'{log_dir}/script.log')
        ]
    )
    
    # Set up error logging
    error_logger = logging.getLogger('error_logger')
    error_logger.setLevel(logging.ERROR)
    error_handler = logging.FileHandler(f'{log_dir}/error.log')
    error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    error_logger.addHandler(error_handler)
    
    return logging.getLogger(__name__), error_logger

# Database connection function
def connect_to_db():
    """Establish connection to the DB2 database."""
    conn_string = (
        f"DATABASE={DB2_CONFIG['database']};"
        f"HOSTNAME={DB2_CONFIG['hostname']};"
        f"PORT={DB2_CONFIG['port']};"
        f"PROTOCOL=TCPIP;"
        f"UID={DB2_CONFIG['username']};"
        f"PWD={DB2_CONFIG['password']};"
    )
    return ibm_db.connect(conn_string, "", "")

# Query execution function
def execute_query(conn, sql):
    """Execute SQL query and return results."""
    stmt = ibm_db.exec_immediate(conn, sql)
    columns = [ibm_db.field_name(stmt, i) for i in range(ibm_db.num_fields(stmt))]
    results = []
    while ibm_db.fetch_row(stmt):
        row = [str(ibm_db.result(stmt, i)) for i in range(len(columns))]
        results.append(row)
    return columns, results

# CSV writing function
def save_to_csv(filename, columns, data):
    """Save data to a CSV file."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(data)

# Command generation function
def generate_commands(documents_file, metadata_dir, output_file):
    """Generate arsadmin retrieve commands based on documents and metadata."""
    commands = []
    current_command = []
    current_doc_count = 0
    current_pri_sec = None

    with open(documents_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            agname = row['AGNAME']
            agid_name = row['AGID_NAME']
            table_name = row['TABLE_NAME']
            
            # Read metadata for the current table
            metadata_file = os.path.join(metadata_dir, f"{table_name}.csv")
            with open(metadata_file, 'r') as mf:
                metadata_reader = csv.DictReader(mf)
                for metadata_row in metadata_reader:
                    doc_name = metadata_row['DOC_NAME']
                    pri_nid = metadata_row['PRI_NID']
                    sec_nid = metadata_row['SEC_NID']
                    
                    # Check if we need to start a new command
                    if (current_doc_count >= 1000 or 
                        (current_pri_sec and current_pri_sec != f"{pri_nid}-{sec_nid}")):
                        if current_command:
                            commands.append(" ".join(current_command))
                        current_command = []
                        current_doc_count = 0
                    
                    # Start or add to the current command
                    if not current_command:
                        current_command = [
                            f"arsadmin retrieve -I LAZARI4 -u t320818 -g {agname}",
                            f"-n {pri_nid}-{sec_nid}",
                            f"-d ./out/data/{agid_name}/"
                        ]
                    current_command.append(doc_name)
                    current_doc_count += 1
                    current_pri_sec = f"{pri_nid}-{sec_nid}"
    
    # Add the last command if there's any
    if current_command:
        commands.append(" ".join(current_command))
    
    # Write commands to file
    with open(output_file, 'w') as f:
        for command in commands:
            f.write(f"{command}\n")
    
    return len(commands)

def main():
    logger, error_logger = setup_logging()
    start_time = datetime.now()

    try:
        # Create or rename the out directory
        out_dir = './out/sql'
        if os.path.exists(out_dir):
            old_out_dir = f"{out_dir}_{datetime.fromtimestamp(os.path.getctime(out_dir)).strftime('%Y%m%d_%H%M%S')}"
            os.rename(out_dir, old_out_dir)
            logger.info(f"Renamed existing out directory to {old_out_dir}")
        os.makedirs(out_dir, exist_ok=True)
        
        conn = connect_to_db()
        logger.info("Connected to database")

        # Execute table_list_sql
        table_list_sql = """
        SELECT TRIM(TRANSLATE(ag.name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', '')), ag.agid_name, seg.table_name
        FROM arsag ag
        INNER JOIN arsseg seg ON ag.agid = seg.agid
        WHERE ag.name NOT LIKE 'System%'
        ORDER BY 2, 3
        """
        columns, results = execute_query(conn, table_list_sql)
        logger.info(f"Fetched {len(results)} rows from table_list_sql")
        
        # Save results to documents.csv
        documents_file = os.path.join(out_dir, 'documents.csv')
        save_to_csv(documents_file, ['AGNAME', 'AGID_NAME', 'TABLE_NAME'], results)
        logger.info(f"Saved table list to {documents_file}")

        # Process each table
        metadata_dir = os.path.join(out_dir, 'documents_metadata')
        os.makedirs(metadata_dir, exist_ok=True)
        total_metadata_rows = 0

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
            columns, results = execute_query(conn, table_metadata_sql)
            total_metadata_rows += len(results)
            
            metadata_file = os.path.join(metadata_dir, f"{table_name}.csv")
            save_to_csv(metadata_file, ['DOC_NAME', 'PRI_NID', 'SEC_NID'], results)
            logger.info(f"Saved metadata for {table_name} to {metadata_file}")

        logger.info(f"Total metadata rows fetched: {total_metadata_rows}")

        # Generate commands
        command_file = './out/arsadmin_retrieve.txt'
        command_count = generate_commands(documents_file, metadata_dir, command_file)
        logger.info(f"Generated {command_count} arsadmin retrieve commands in {command_file}")

    except Exception as e:
        error_logger.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        if 'conn' in locals():
            ibm_db.close(conn)
            logger.info("Database connection closed")
    
    end_time = datetime.now()
    logger.info(f"Script execution completed. Total runtime: {end_time - start_time}")

if __name__ == "__main__":
    main()
