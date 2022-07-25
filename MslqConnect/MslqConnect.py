import MySQLdb
from math import ceil

class MslqConnect:

    def __init__(self, host, user, password, database) -> None:
        self.host = host
        self.user = user
        self.db_name= database
        self.password = password
        self.connection = MySQLdb.connect(host= self.host,
                     user= self.user , # your username
                      passwd= self.password, # your password
                      db= self.db_name) # name of the data base
        self.cursor = self.connection.cursor()
        self.tables_names = self.get_table_names()

    def get_table_names(self):
        tables_names = []
        mysql_stament = '''show tables from {}'''.format(self.db_name)
        self.cursor.execute(mysql_stament)
        results = self.cursor.fetchall()
        for tables_data in results: tables_names.append(tables_data[0])
        return tables_names

    def describe_table(self, table_name):
        describe_statement='''describe {}.{}'''.format(self.db_name,table_name)
        self.cursor.execute(describe_statement); describe_rows=self.cursor.fetchall()
        return describe_rows
    
    def show_table_indexes(self, table_name):
        index_statement = '''SHOW INDEX FROM {}.{}'''.format(self.db_name, table_name)
        self.cursor.execute(index_statement); index_rows=self.cursor.fetchall()
        return index_rows

    def get_records_pages_per_limit(self, limit, table_name):
        data_count ='''select count(*) from {};'''.format(table_name)
        self.cursor.execute(data_count)
        count_of_rows = self.cursor.fetchone()
        try:
            pages = ceil(count_of_rows[0]/limit)
        except:
            pages = 1

        return pages
    
    def get_records_with_limits_offset(self, table_name, limit ,offset):
        select_stament = "select * from {} limit {} offset {}".format(table_name, limit, offset)
        self.cursor.execute(select_stament)
        return self.cursor.fetchall()
    
    def close_conections(self):
        self.cursor.close()
        self.connection.close()


