import os
import env
import oracledb
import psycopg2

# Configurações de conexão
un = 'tsiuserdb'
cs = '10.1.5.6:1521/PRODCTRL'
pw = os.environ['PW_ORACLE']

os.environ['PYO_SAMPLES_ORACLE_CLIENT_PATH'] = os.getenv("ORACLE_HOME") 

#with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
oracle_conn_str = "tsiuserdb@10.1.5.6:1521/PRODCTRL"
postgres_conn_str = "dbname='producao' user='producao' password='producao' host='172.16.0.40' port='5441'"


d = r"C:\telematica\oracle"
oracledb.init_oracle_client(lib_dir=d)


with psycopg2.connect(postgres_conn_str) as postgres_conn:
    with postgres_conn.cursor() as postgres_cursor:
        postgres_cursor.execute("select max(dtserie) as dt from BI_IDADE")
        val = postgres_cursor.fetchone() 
        print(val)
