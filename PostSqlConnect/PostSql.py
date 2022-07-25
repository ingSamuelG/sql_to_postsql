import psycopg2
import re
from datetime import datetime

def delete_null_char(value):
    if isinstance(value, str) and re.search('\x00',value):
        return value.replace("\x00", "\uFFFD")
    else:  
        return value


class PostSqlConnect:
    
    def __init__(self, host, user, password, database,encoding) -> None:
        self.host = host
        self.user = user
        self.db_name= database
        self.password = password
        # self.connection = psycopg2.connect(("dbname='{}' user={} password={}").format(database, user, password))
        self.connection = psycopg2.connect(host= self.host, dbname = self.db_name, user = self.user, password= self.password)
        self.cursor = self.connection.cursor()
        self.encoding = encoding
        self.cursor.execute("set client_encoding = " + encoding)
    
    def drop_existing_table(self, table_name):
        drop_stament='drop table if EXISTS %s;'%(table_name)
        self.cursor.execute(drop_stament); 
        self.connection.commit()

    def create_indexes_from_mysql(self, mysql_indexes):
        index_struct = {}
        unique_indexs = {}
        indexes = []
        index_psql_stament = 'CREATE INDEX {} ON {} ({});'
        index_psql_stament_w_tb = 'CREATE INDEX {}_{} ON {} ({});'
        for row in mysql_indexes:
            table=row[0]; non_unique=row[1]; key_name=row[2]; sueq_index = row[3]; name=row[4]; index_type=row[10]
            if row[1] == 1:
                if index_struct.get(key_name):
                    index_struct[key_name] = index_struct[key_name] + "," + name
                else:
                    index_struct[row[2]] = row[4]

            if non_unique == 0 and key_name != "PRIMARY" and not re.search('\\bindex',key_name):
                unique_indexs[name] = "unique"

            # if  non_unique == 1:
            #     indexes.append(index_psql_stament.format(key_name,table,name))
        
        for key, values in index_struct.items():
            if re.search(table,key):
                indexes.append(index_psql_stament.format(key,table,values))
            else:
                indexes.append(index_psql_stament_w_tb.format(table,key,table,values))
            
        self.indexes = indexes
        return (unique_indexs, indexes)

    def execute_indexes(self):
        for ind in self.indexes:
            self.cursor.execute(ind)

    def create_table_from_discribe_mysql(self, mysql_descriptions, table_name, unique_indexs):
        create_table_stament='create table %s ('%(table_name)
        primary_key = ""
        columns = "("
        columns_amount = ""
        for row in mysql_descriptions:
            name=row[0]; type=row[1]; is_null=row[2]; keys = row[3]; default=row[4]; extra=row[5]
            if re.search('\\bint',type) and extra == 'auto_increment': type='serial'
            if re.search('\\bbigint',type) and extra == 'auto_increment': type='serial'
            if re.search('\\bint',type): type='int'
            if re.search('\\bbigint',type): type='bigint'
            if re.search('\\bvarchar',type): type='varchar'
            if re.search('\\btinyint',type) or re.search('\\bsmallint',type) : type='smallint'; 
            if 'blob' in type: type='bytea'
            if 'datetime' in type: type='timestamptz'
            if 'text' in type: type='text'
            if 'enum' in type:
                type = 'varchar'
                print ("warning : conversion of enum to varchar %s(%s)" % (table_name, name))
            
            if 'NO' in is_null: is_null = "not null"
            if 'YES' in is_null: is_null = "null"

            if default  !=None:
                if type == 'timestamptz':
                    default = "default %s " % datetime.now().strftime("'%Y-%m-%d %H:%M:%S'")
                
                elif default == '':
                    default = "default '%s' " % default
                else:
                    default = "default %s " % default
            else:
                default = ""

            # if extra != "": extra = " auto_increment"

            if keys == 'PRI':  primary_key = name

            if unique_indexs.get(name): type = "{} {}".format(type,unique_indexs.get(name))

            create_table_stament+='%s %s %s%s,'%(name, type, default,is_null)
            columns = columns + name + ','
            columns_amount = columns_amount + "%s,"

        columns = columns.strip(',')
        columns_amount = columns_amount.strip(',')
        create_table_stament = create_table_stament.strip(',')
        if primary_key != "":
            create_table_stament= create_table_stament + ",primary key({})".format(primary_key)
        
        create_table_stament= create_table_stament +')'
        columns = columns + ")"
        self.cursor.execute(create_table_stament)
        self.columns_string = columns_amount
    
    def import_data_from_mysql(self, mysql_rows,table_name):
        if mysql_rows:
            try:
                args_str = ','.join(self.cursor.mogrify("({})".format(self.columns_string),x).decode('utf-8') for x in mysql_rows)
            except ValueError:
                formated_rows = []
                for r in mysql_rows:
                    formated_rows.append([delete_null_char(n) for n in r])
                
                args_str = ','.join(self.cursor.mogrify("({})".format(self.columns_string),x).decode('utf-8') for x in formated_rows)
            try:
                insert_stament = 'insert into {} values {};'.format(table_name,args_str)
                self.cursor.execute(insert_stament)
            except Exception as e:
                print(e)
            # o = 0
            # new_row = []
            # for r in row:
            #     if isinstance(r, str) and re.search('\x00',r):
            #         new_row.append(r.replace("\x00", "\uFFFD"))
            #     else:  
            #         new_row.append(r)
            #     o+=1

            #     # print(new_row)
            #     self.cursor.execute(insert_stament,new_row)

        # for row in mysql_rows:
        #     insert_stament = 'insert into {} values ({});'.format(table_name,self.columns_string)
    
        #     try:
        #         self.cursor.execute(insert_stament,row)
        #     except ValueError:
        #         o = 0
        #         new_row = []
        #         for r in row:
        #             if isinstance(r, str) and re.search('\x00',r):
        #                 new_row.append(r.replace("\x00", "\uFFFD"))
        #             else:  
        #                 new_row.append(r)
        #             o+=1

        #         # print(new_row)
        #         self.cursor.execute(insert_stament,new_row)
    
    def commit_all_transactions(self):
        try:
            self.connection.commit()
        except Exception as e:
            print (e)
            self.connection.rollback()
    
    def close_conections(self):
        self.cursor.close()
        self.connection.close()


    