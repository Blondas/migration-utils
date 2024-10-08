import ibm_db
from db2_config import DB2_CONFIG

# Create the connection string
conn_string = (
    f"DATABASE={database};"
    f"HOSTNAME={hostname};"
    f"PORT={port};"
    f"PROTOCOL=TCPIP;"
    f"UID={username};"
    f"PWD={password};"
)

try:
    # Establish the connection
    conn = ibm_db.connect(conn_string, "", "")
    print("Connected to database")

    # Execute a query
    sql = "SELECT * FROM your_table_name"
    stmt = ibm_db.exec_immediate(conn, sql)

    # Fetch and print results
    while ibm_db.fetch_row(stmt) != False:
        # Print or process each row
        print(ibm_db.result(stmt, 0), ibm_db.result(stmt, 1))

except Exception as e:
    print(f"Error connecting to database: {e}")

finally:
    # Close the connection
    if 'conn' in locals():
        ibm_db.close(conn)
        print("Connection closed")