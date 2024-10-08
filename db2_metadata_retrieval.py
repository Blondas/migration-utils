import ibm_db
import csv
import os
import datetime

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

def manage_output_directory():
    if os.path.exists('./out') and os.listdir('./out'):
        creation_time = datetime.datetime.fromtimestamp(os.path.getctime('./out'))
        new_name = f"./out_{creation_time.strftime('%Y%m%d_%H%M%S')}"
        os.rename('./out', new_name)
        print(f"Renamed existing 'out' directory to '{new_name}'")
    os.makedirs('./out', exist_ok=True)

def main():
    try:
        manage_output_directory()

        conn = connect_to_db()
        print("Connected to database")

        # Execute table_list_sql and save results
        table_list_sql = """
        SELECT TRIM(TRANSLATE(ag.name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', '')), ag.agid_name, seg.table_name
        FROM arsag ag
        INNER JOIN arsseg seg ON ag.agid = seg.agid
        WHERE ag.name NOT LIKE 'System%'
        ORDER BY 2, 3
        """
        columns, results = execute_query(conn, table_list_sql)
        save_to_csv('./out/table_list_sql.csv', columns, results)
        print(f"Saved table list to ./out/table_list_sql.csv")
        print(f"Number of rows fetched: {len(results)}")

        # Initialize all_table_metadata list with header
        all_table_metadata = [columns]

        # Execute table_metadata_sql for each table
        for row in results:
            table_name = row[2]  # Assuming table_name is the third column
            table_metadata_sql = f"""
            SELECT DISTINCT doc_name, pri_nid
            FROM {table_name}

            UNION

            SELECT DISTINCT
            CAST(resource AS VARCHAR(10)),
            pri_nid
            FROM {table_name}
            WHERE resource > 0

            UNION

            SELECT DISTINCT
            TRIM(TRANSLATE(doc_name, '', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')) ||
            LEFT(TRIM(TRANSLATE(doc_name, '', '0123456789')), 3) || '1',
            pri_nid
            FROM {table_name}
            """
            columns, table_results = execute_query(conn, table_metadata_sql)

            # Save individual table results
            save_to_csv(f'./out/table_{table_name}_metadata.csv', columns, table_results)
            print(f"Saved metadata for {table_name} to ./out/table_{table_name}_metadata.csv")

            # Append to all_table_metadata
            all_table_metadata.extend(table_results)

        # Save all table metadata
        save_to_csv('./out/all_table_metadata.csv', columns, all_table_metadata[1:])  # Skip header row
        print("Saved all table metadata to ./out/all_table_metadata.csv")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if 'conn' in locals():
            ibm_db.close(conn)
            print("Connection closed")

if __name__ == "__main__":
    main()