import os
import env
import oracledb
import psycopg2

#lido = 1
#escrito = 1
# Função para conectar ao banco Oracle e buscar os dados
def fetch_data_from_oracle(query):
    with oracledb.connect(user=usro, password=pswo, dsn=ipusero) as oracle_conn:
        with oracle_conn.cursor() as oracle_cursor:
            oracle_cursor.execute(query)
            columns = [col[0] for col in oracle_cursor.description]
            rows = oracle_cursor.fetchall()
            lido = oracle_cursor.rowcount
            print(f"Lido Oracle: {lido}") 
            return columns, rows

# Função para inserir os dados no banco PostgreSQL
def insert_data_into_postgres(postgres_conn_str, table_name, columns, rows):
    with psycopg2.connect(postgres_conn_str) as postgres_conn:
        with postgres_conn.cursor() as postgres_cursor:
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            postgres_cursor.executemany(insert_query, rows)
            escrito = postgres_cursor.rowcount
            postgres_conn.commit()            
            print(f"Escrito PostgreSQL: {escrito}") 

def read_last_data_postgres(postgres_conn_str):
    with psycopg2.connect(postgres_conn_str) as postgres_conn:
        with postgres_conn.cursor() as postgres_cursor:
            postgres_cursor.execute("select max(dtserie) as dt from BI_IDADE")
            val = postgres_cursor.fetchone() 
            #print(val[0]) 
            return val[0]

# Configurações de conexão
#os.environ['ORACLE_USR'] 
#os.environ['ORACLE_IPUSER']
#os.environ['ORACLE_PSW']

#os.environ['POSTGRES_DB']
#os.environ['POSTGRES_USR']
#os.environ['POSTGRES_PSW']
#os.environ['POSTGRES_IP'] 
#os.environ['POSTGRES_PORT']

usro = os.environ['ORACLE_USR'] 
ipusero = os.environ['ORACLE_IPUSER'] 
pswo = os.environ['ORACLE_PSW'] 

dbp= os.environ['POSTGRES_DB'] 
usrp = os.environ['POSTGRES_USR']
pswp = os.environ['POSTGRES_PSW']
ipp = os.environ['POSTGRES_IP']
portp = os.environ['POSTGRES_PORT']


os.environ['PYO_SAMPLES_ORACLE_CLIENT_PATH'] = os.getenv("ORACLE_HOME") 

oracle_conn_str = f"{usro}@{ipusero}"
#print (oracle_conn_str)

postgres_conn_str = f"dbname='{dbp}' user='{usrp}' password='{pswp}' host='{ipp}' port='{portp}'"
#print(postgres_conn_str)

#Query para buscar última data migrada
max_data_migrada = read_last_data_postgres(postgres_conn_str)
#print (max_data_migrada)

# Query para buscar dados no Oracle
oracle_query_dt = f"SELECT * FROM BI_IDADE where DTSERIE > '{max_data_migrada}'" # and ISERIE != '18'"
#oracle_query_dt = '20240601'
#print(oracle_query_dt)

# Nome da tabela no PostgreSQL
postgres_table_name = "BI_IDADE"

#oracledb.init_oracle_client()

d = r"C:\telematica\oracle"
oracledb.init_oracle_client(lib_dir=d)

#oracledb.init_oracle_client(lib_dir=env.get_oracle_client(env.get_oracle_client()))

# Copiar dados de Oracle para PostgreSQL
columns, rows = fetch_data_from_oracle(oracle_query_dt)
insert_data_into_postgres(postgres_conn_str, postgres_table_name, columns, rows)

print(f"Dados Lidos lidos e escritos com sucesso!")
