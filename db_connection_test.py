import ibm_db
from db2_config import DB2_CONFIG

# Create the connection string
conn_string = (
    f"DATABASE={DB2_CONFIG['database']};"
    f"HOSTNAME={DB2_CONFIG['hostname']};"
    f"PORT={DB2_CONFIG['port']};"
    f"PROTOCOL=TCPIP;"
    f"UID={DB2_CONFIG['username']};"
    f"PWD={DB2_CONFIG['password']};"
)

try:
    # Establish the connection
    conn = ibm_db.connect(conn_string, "", "")
    print("Connected to database")

    # Execute a query
    # sql = "SELECT * FROM your_table_name"
    sql = """
    SELECT TRIM(TRANSLATE(ag.name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', '')), ag.agid_name, seg.table_name
    FROM arsag ag
    INNER JOIN arsseg seg ON ag.agid = seg.agid
    WHERE ag.name NOT LIKE 'System%'
    ORDER BY 2, 3
    """
    stmt = ibm_db.exec_immediate(conn, sql)

    # Get column information
    num_columns = ibm_db.num_fields(stmt)
    column_names = [ibm_db.field_name(stmt, i) for i in range(num_columns)]

    # Print column names
    print(" | ".join(column_names))
    print("-" * (sum(len(name) for name in column_names) + 3 * (num_columns - 1)))

    # Fetch and print results
    while ibm_db.fetch_row(stmt):
        row = [str(ibm_db.result(stmt, i)) for i in range(num_columns)]
        print(" | ".join(row))

except Exception as e:
    print(f"Error connecting to database: {e}")

finally:
    # Close the connection
    if 'conn' in locals():
        ibm_db.close(conn)
        print("Connection closed")
