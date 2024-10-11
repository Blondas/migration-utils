The script consists of 2 parts:
1. Query sql database, and build cmd commands
2. Execute cmd commands - IGNORE THIS ONE FOR NOW

General comments:
- log well formatted messages including level, format, date format. It should be logged to console and to `./out folder`, make error log separate. 
- log runtime after 1 script part is executed
- make comments in the code to easier understand logic, especially in longer and more complicated methods

Part 1:
1. All results from sql queries should be saved to `./out/sql directory`
2. If not empty out directory already exists, rename it to `./out/sql_{DATETIME}` (take datetime from the folder creation time)
3. this is the initial query:
`table_list_sql = """
SELECT TRIM(TRANSLATE(ag.name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', '')), ag.agid_name, seg.table_name
FROM arsag ag
INNER JOIN arsseg seg ON ag.agid = seg.agid
WHERE ag.name NOT LIKE 'System%'
ORDER BY 2, 3
"""`
4. log to console number of rows fetched
5. save the result in csv format with header consisting column names (AGNAME, AGID_NAME, TABLE_NAME) to `./out/sql/documents.csv`
6. for each table_name run following query:
`table_metadata_sql = """
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
FROM {table_name};
"""`
7. save the result for each table in csv with header consisting column names (DOC_NAME, PRI_NID, SEC_NID) as `./out/sql/documents_metadata/{TABLE_NAME}.csv`
8. log to console total number of rows fetched (sum from all tables)
9. Iterate through documents.csv and use `{TABLE_NAME}.csv` as lookup table and construct cmd commands. the logic:
* command template: `arsadmin retrieve -I {INSTANCE_NAME} -u {USER} -g {AGNAME} -n {PRI_NID}-{SEC_NID} -d ./out/data/{AGID_NAME}/ {DOC_NAME1} ... {DOC_NAMEN}`
* if in lookup table there is more than 1000 DOC_NAMEs for given TABLE_NAME then move it to the next arsadmin retrieve command
- in case there are different pairs PRI_NID and SEC_NID for given entry: move it to the next arsadmin retrieve command
- save (append the entries) to file `./out/arsadmin_retrieve.txt`
- log to console success (with context details i.e. how many entries created in `arsadmin_retrieve.txt`) or fail

------------------------------------
OK. This was PART 1 of the script. We will go back to it later. Now let's focus on PART 2 of the script. We will work on it step by step.

1. Write me a function which will run prepared arsadmin commands with following logic:
   - context: 
     - this is sample arsadmin retrieve command: 
     `arsadmin retrieve -I {INSTANCE_NAME} -u {USER} -g {AGNAME} -n {PRI_NID}-{SEC_NID} -d ./out/data/{AGID_NAME}/ {DOC_NAME1} {DOC_NAME2} ... {DOC_NAME1000}`
       - each arsadmin command in the input file has various number of  document names (between 0 and 1000). SO everything after following part of command is a document name. example:
         - this is base command: `arsadmin retrieve -I {INSTANCE_NAME} -u {USER} -g {AGNAME} -n {PRI_NID}-{SEC_NID} -d ./out/data/{AGID_NAME}/`
         - this is document list: `{DOC_NAME1} {DOC_NAME2} ... {DOC_NAME1000}`
       - how arsadmin retrieve command works: it will download all the `{DOC_NAMES}` one by one. When it fails due to data corruption, not available etc. for one of them, the rest of the requested objects are not downloaded.
     - business logic:
       - Check the return code, if nonzero the error should be logged for further investigation (mentioning the `{DOC_NAME}`)
       - Error handling:
         - initial command: `arsadmin retrieve -I {INSTANCE_NAME} -u {USER} -g {AGNAME} -n {PRI_NID}-{SEC_NID} -d ./out/data/{AGID_NAME}/ {DOC_NAME1} <{DOC_NAME2} {DOC_NAME3} ... {DOC_NAME1000}`
           - error message1: 
            >ARS1107E An error occurred.  Contact your system administrator and/or consult the System Log.  File=arsadmin.c, Line=2816 
              ARS1159E Unable to retrieve the object {DOC_NAME2}
           - handling: 
             - log error message with base command + only failing document: `arsadmin retrieve -I {INSTANCE_NAME} -u {USER} -g {AGNAME} -n {PRI_NID}-{SEC_NID} -d ./out/data/{AGID_NAME}/ {DOC_NAME2}`
             - rerun the command with corrected list of `{DOC_NAMES}` starting for the next one (after the corrupted): `arsadmin retrieve -I {INSTANCE_NAME} -u {USER} -g {AGNAME} -n {PRI_NID}-{SEC_NID} -d ./out/data/{AGID_NAME}/ {DOC_NAME3} ... {DOC_NAME1000}`
           - error message2: 
            > ARS1168E Unable to determine Storage Node
           - handling:
             - log error message with the whole command, execute the next command
           - error message2: 
            > ARS1110E The application group >AGNAME< does not exist or user >USER< does not have permission to access the application group
           - handling:
             - log error message with the whole command, execute the next command from 
2. The script should maintain the state, so when stopped, it can be resumed from the command on which it stopped.
3. If there is less than given % of free space on disk (default 10%) do not execute next commands - finish after running the current command.

----------------------------------------------------------------------------------
Write me a function wchich
PART 2:
10. When the arsadmin fails due to data corruption, not available etc. the rest of the requested objects are not fulfilled. hence it is required to "check" the ret code, if nonzero it should be logged for further investigation and the arsadmin retrieve command should be rerun starting from the next not corrupted document 
11. the arsadmin retireve commands should run in separate threads - number configurable, by default 8
12. In case the script is stopped, it should be able to resume it from where it finished
13. support performance testing, i.e. how fast all together threads download 10gb of data, depends on with  how many threads it is running: 2, 4, 8, 12, 16, threads. Log the result to the file. ideally this should be loosely coupled with the main script
14. If there is less then a given % of free space on disk, do not start the next threads - only allow those running to finish their job. log the information.

arsadmin retrieve -I LAZARI4 -u {USER} -g {AGNAME} -n {PRI_NID}-{SEC_NID} -d ./out/data/{AGID_NAME}/ {DOC_NAME}
 
