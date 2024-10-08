1. All output files are saved to ./out directory
2. If not empty out directory already exists, rename it to ./out_{DATETIME} (take datefime from the folder creation time)
3. this is the initial query:
table_metadata_sql = """
SELECT TRIM(TRANSLATE(ag.name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', '')), ag.agid_name, seg.table_name
FROM arsag ag
INNER JOIN arsseg seg ON ag.agid = seg.agid
WHERE ag.name NOT LIKE 'System%'
ORDER BY 2, 3
"""
4. save the result in csv format with header consisting column names to ./out/table_metadata_sql
5. for each table_name run following query:
arsadmin_retrieve_metadata_sql = """
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
   FROM {table_name};
   """
6. save the result in cvs with header consisting column names as:
    - the result from all tables concatenated to ./out/arsadmin_retrieve_metadata_sql
    - the result for each table to ./out/arsadmin_retrieve_metadata_{table_name}_sql
7. 
8. 

