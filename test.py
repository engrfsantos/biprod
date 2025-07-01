import os
import env
import oracledb
import psycopg2

# Configurações de conexão
un = 'tsiuserdb'
cs = '10.0.0.1:1521/PRODCTRL'
pw = os.environ['PW_ORACLE']

os.environ['PYO_SAMPLES_ORACLE_CLIENT_PATH'] = os.getenv("ORACLE_HOME") 

#with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
oracle_conn_str = "userdb@10.0.0.1:1521/PRODCTRL"
postgres_conn_str = "dbname='producao' user='user' password='pass' host='172.16.0.1' port='5432'"


d = r"C:\dir\oracle"
oracledb.init_oracle_client(lib_dir=d)


with psycopg2.connect(postgres_conn_str) as postgres_conn:
    with postgres_conn.cursor() as postgres_cursor:
        postgres_cursor.execute("select max(dtserie) as dt from BI_IDADE")
        val = postgres_cursor.fetchone() 
        print(val)
