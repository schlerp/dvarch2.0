import os
import pypyodbc
import time


# At this stage databases must support ODBC
# and have INFORMATION_SCHEMA availabale!


# SQL Server Types
MSSQL = 'MSSQL'
POSTGRES = 'postgres'
MYSQL = 'mysql'
CACHE = 'cache'
MARIADB = 'mariadb'
ORACLE = 'oracle'

SUPPORTED_SERVERS = [MSSQL, POSTGRES, MYSQL, CACHE, MARIADB, ORACLE]
PRETTY_NAMES = ['Microsoft SQL Server',
                'PostgreSQL Server',
                'MySQL Server',
                'Cache Database Server',
                'MariaDB Server',
                'Oracle Database pre 2015']

DRIVERS = {
    MSSQL: 'SQL Server' if os.name == 'nt' else 'ODBC Driver 17 for SQL Server',
    POSTGRES: 'https://www.postgresql.org/ftp/odbc/versions/',
    CACHE: 'ftp://ftp.intersys.com/pub/cache/odbc',
    MARIADB: 'https://downloads.mariadb.org/connector-odbc/',
    ORACLE: 'https://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html',
    MYSQL: 'https://dev.mysql.com/downloads/connector/odbc/'
}

ODBC_DRIVER_PATHS = {
    MSSQL: 'https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-2017',
    POSTGRES: 'https://www.postgresql.org/ftp/odbc/versions/',
    CACHE: 'ftp://ftp.intersys.com/pub/cache/odbc',
    MARIADB: 'https://downloads.mariadb.org/connector-odbc/',
    ORACLE: 'https://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html',
    MYSQL: 'https://dev.mysql.com/downloads/connector/odbc/'
}

sql_info_schema_tables = '''
    SELECT t1.TABLE_CATALOG
          ,t1.TABLE_SCHEMA
          ,t1.TABLE_NAME
          ,t2.TABLE_TYPE
          ,t1.COLUMN_NAME
          ,t1.ORDINAL_POSITION
          ,t1.IS_NULLABLE
          ,t1.DATA_TYPE
          ,t1.CHARACTER_MAXIMUM_LENGTH
          ,t1.CHARACTER_OCTET_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS t1 JOIN INFORMATION_SCHEMA.TABLES t2 ON t1.TABLE_NAME = t2.TABLE_NAME
    '''

sql_info_schema_column = '''
    SELECT t1.TABLE_CATALOG
          ,t1.TABLE_SCHEMA
          ,t1.TABLE_NAME
          ,t1.COLUMN_NAME
          ,t1.ORDINAL_POSITION
          ,t1.IS_NULLABLE
          ,t1.DATA_TYPE
          ,t1.CHARACTER_MAXIMUM_LENGTH
          ,t1.CHARACTER_OCTET_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS t1
    WHERE t1.TABLE_NAME = '{}' AND t1.COLUMN_NAME = '{}'
    '''

sql_info_schema_relations = '''
SELECT  
     KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME 
    ,KCU1.TABLE_NAME AS FK_TABLE_NAME 
    ,KCU1.COLUMN_NAME AS FK_COLUMN_NAME 
    ,KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION 
    ,KCU2.CONSTRAINT_NAME AS REFERENCED_CONSTRAINT_NAME 
    ,KCU2.TABLE_NAME AS REFERENCED_TABLE_NAME 
    ,KCU2.COLUMN_NAME AS REFERENCED_COLUMN_NAME 
    ,KCU2.ORDINAL_POSITION AS REFERENCED_ORDINAL_POSITION 
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC 
     INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1 
       ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
         AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
         AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
     INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2 
       ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG  
         AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA 
         AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME 
         AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION
'''


class ServerNotSupported(Exception):
    def __init__(self, msg=None):
        if not msg:
            msg = 'Server type is not supported!'
        self.msg = msg


def create_conn(server, database, db_type, 
                user=None, pwd=None, port=None):
    
    conn_string = ''
    
    conn_string += 'Driver={{{driver}}};'.format(driver=DRIVERS[db_type])

    conn_string += 'Server={server};'.format(server=server)

    conn_string += 'Database={database};'.format(database=database)
    
    if port:
        conn_string += 'Port={port};'.format(port=port)

    if user:
        conn_string += 'Uid={user};'.format(user=user)
        conn_string += 'Pwd={pwd};'.format(pwd=pwd)

    print(conn_string)

    conn = pypyodbc.connect(conn_string, timeout=1)

    return conn


def sql_to_dict(cursor, sql):
    cursor.execute(sql)
    columns = [column[0] for column in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))    
    return results


def get_schema(server, database, db_type=MSSQL, 
               user=None, pwd=None, port=None):
    
    # ensure server type is supported!
    if db_type not in SUPPORTED_SERVERS:
        raise ServerNotSupported()

    # create database cursor
    cursor = create_conn(server, database, db_type, user, pwd, port)

    tables = sql_to_dict(cursor, sql_info_schema_tables)
    relations = sql_to_dict(cursor, sql_info_schema_relations)

    return tables, relations


def get_column(table, column, database):
    # ensure server type is supported!
    if str(database['type']) not in SUPPORTED_SERVERS:
        raise ServerNotSupported()

    # create database cursor
    conn = create_conn(
        database['server'], 
        database['database'], 
        database['type'], 
        database['user'], 
        database['password'], 
        database['port']
    )

    cursor = conn.cursor()

    results = sql_to_dict(cursor, sql_info_schema_column.format(table, column))
    return results


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print('{}  {:2.2f} ms'.format(method.__name__, (te - ts) * 1000))
        return result    
    return timed


@timeit
def execute_transaction(sql, database):
    # ensure server type is supported!
    if str(database['type']) not in SUPPORTED_SERVERS:
        raise ServerNotSupported()

    # create database cursor
    conn = create_conn(
        database['server'], 
        database['database'], 
        database['type'], 
        database['user'], 
        database['password'], 
        database['port']
    )

    cursor = conn.cursor()

    cursor.execute(sql)

    # try:
    #     cursor.execute(sql)
    # except:
    #     print('Transaction failed, rolling back...')
    #     conn.rollback()
    #     return False
    # else:
    #     conn.commit()
    #     print('Transaction successful!')
    #     return True


if __name__ == '__main__':
    tables, relations = get_schema(server='DOHWLDCDB01-DEV\\ODS_TEST',
                                   database='DV_RAWVAULT')

    print(tables)
    print(relations)
