tabela:

the database is db2
let's work on the table called MIG3_AGID_NAME_1:
	- it should have field: od_inst: varchar()
    - table columns:
			- object_name (32): object_1, object_2, ...
			- pri_nid: int
			- tape_id: string (32)
			- status: string  (20)
			- dest_relative_path (255)
			- retrieve_dt: datetime
    - table access patterns:
        - read in big bulks (50k rows), pattern: select * where status is not completed order by tape_id, pri_nid
        - status update writes in smaller bulks(1000 rows)