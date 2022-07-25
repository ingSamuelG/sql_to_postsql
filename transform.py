from MslqConnect import MslqConnect
from PostSqlConnect import PostSql

MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_DB = "big"
MYSQL_PASS = ""

POSG_HOST = "localhost"
POSG_DB = "testBest"
POSG_USER = "postgres"
POSG_PASS = "1234"
POSG_END = "utf8"
MSQL_IMPORT_QUERY_LIMIT= 1000000

Mysql = MslqConnect.MslqConnect(MYSQL_HOST,MYSQL_USER, MYSQL_PASS, MYSQL_DB)
Postql = PostSql.PostSqlConnect(POSG_HOST, POSG_USER, POSG_PASS,POSG_DB, POSG_END )

for table_name in Mysql.tables_names:
    Postql.drop_existing_table(table_name)
    index_rows = Mysql.show_table_indexes(table_name)
    describe_rows = Mysql.describe_table(table_name)
    unique_idex , indexes = Postql.create_indexes_from_mysql(index_rows)
    Postql.create_table_from_discribe_mysql(describe_rows, table_name, unique_idex)

    offset = 0
    pages = Mysql.get_records_pages_per_limit(MSQL_IMPORT_QUERY_LIMIT, table_name)
    for i in range(0,pages+1):
        print("insert {} of {} in table {}".format(i, pages, table_name))
        data_rows = Mysql.get_records_with_limits_offset(table_name,MSQL_IMPORT_QUERY_LIMIT,offset)
        offset+=MSQL_IMPORT_QUERY_LIMIT
        Postql.import_data_from_mysql(data_rows, table_name)
    
    Postql.execute_indexes()
    Postql.commit_all_transactions()

Postql.close_conections()
Mysql.close_conections()
