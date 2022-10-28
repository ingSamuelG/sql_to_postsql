from PostSqlConnect import PostSql
import conts

print("Trying to connect")
Postql = PostSql.PostSqlConnect(conts.POSG_HOST, conts.POSG_USER, conts.POSG_PASS,conts.POSG_DB, conts.POSG_END )
print("OK")
print("Get tables names and sequence name")
seq_tables = Postql.get_seq_t_name()
print("OK")

for table_name, seq_name in seq_tables:
    print(f"Get the last id count on the table: {table_name}")
    results = Postql.get_last_record_id(table_name)
    print("OK")

    if results == None:
        print(f'{table_name} has no records, skipping')
        print("OK")
        continue
    else:
        id = results[0]
        print(f'setting {seq_name} value to last id count: {id}')
        res_tup = Postql.set_val_for_seq(seq_name,id)
        print("OK")
