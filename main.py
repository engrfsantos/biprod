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

def read_max_data_postgres(nome_tabela, campo):
    with psycopg2.connect(postgres_conn_str) as postgres_conn:
        with postgres_conn.cursor() as postgres_cursor:
            postgres_cursor.execute(f"select max({campo}) as dt from {nome_tabela}")
            val = postgres_cursor.fetchone() 
            print(val[0]) 
            return val[0]

def read_min_data_postgres(nome_tabela, campo):
    with psycopg2.connect(postgres_conn_str) as postgres_conn:
        with postgres_conn.cursor() as postgres_cursor:
            postgres_cursor.execute(f"select min({campo}) as dt from {nome_tabela}")
            val = postgres_cursor.fetchone() 
            print(val[0]) 
            return val[0]


def copiar_tabela_oracle_para_postgres(nome_tabela, exclui, trunca, campo_tabela, param1, param2):
    # Configurações de conexão
    # conn_oracle = oracledb.connect(user='oracle_user', password='oracle_password', dsn='oracle_dsn')
    # conn_pg = psycopg2.connect(host='postgres_host', database='postgres_db', user='postgres_user', password='postgres_password')
    
    conn_oracle = oracledb.connect(user=usro, password=pswo, dsn=ipusero) 
    cursor_oracle  =  conn_oracle.cursor()
    #cursor_oracle.execute(f"SELECT * FROM {nome_tabela}")
    
    # Obter a estrutura da tabela
    cursor_oracle.execute(f"""
        SELECT column_name, data_type, data_length 
        FROM user_tab_columns 
        WHERE table_name = '{nome_tabela.upper()}'
    """)

    print(f"Lendo campos da tabela {nome_tabela.upper()}")
    colunas_oracle = cursor_oracle.fetchall()
    
    conn_pg = psycopg2.connect(postgres_conn_str)
    cursor_pg = conn_pg.cursor()   

    #if cursor_pg.closed:
    #    cursor_pg = conn_pg.cursor()  
    
    if exclui:
        try:
            # Criar a tabela no PostgreSQL
            print(f"Excluindo, se Existir, Tabela No Postgres {nome_tabela.upper()}")                
            cursor_pg.execute(f"DROP TABLE IF EXISTS {nome_tabela.upper()}")                
        except :
            print(f"Tabela {nome_tabela.upper()} não existe")
            #criterio = ''
    
    if trunca:
        try:
            # Criar a tabela no PostgreSQL
            print(f"Excluindo, se Existir, Tabela No Postgres {nome_tabela.upper()}")                
            cursor_pg.execute(f"TRUNCATE TABLE IF EXISTS {nome_tabela.upper()}")                
        except :
            print(f"Tabela {nome_tabela.upper()} não existe")
            #criterio = ''
    
    #param1 == 'max'
    if param1=='max': 
        ultimo_registro = f"'{read_max_data_postgres(nome_tabela.upper(),campo_tabela)}'"   
        #aqui somente para testes, comentar essa linha abaixo após o teste
        #ultimo_registro = "'20240101'"                    
        if param2 == '': #validado
            criterio = f" where {campo_tabela} > {ultimo_registro}"
        else:
            criterio = f" where {campo_tabela} > {ultimo_registro} and {campo_tabela} <= {param2}"
    elif param1=='min': 
        primeiro_registro = f"'{read_min_data_postgres(nome_tabela.upper(),campo_tabela)}'"   
        #aqui somente para testes, comentar essa linha abaixo após o teste
        #ultimo_registro = "'20240101'"                    
        if param2 == '': #validado
            criterio = f" where {campo_tabela} < {primeiro_registro}"
        else:
            criterio = f" where {campo_tabela} < {primeiro_registro} and {campo_tabela} >= {param2}"

    else:
        #param1 != 'max' e != '' então é é o valor inferior                                                                       
        if param1 != '':
            #param1 é valor inferior e param2 é diferente de nulo, então tem inicio e fim
            if  param2 != '':
                criterio = f" where {campo_tabela} >= {param1} and {campo_tabela} <= {param2}"
            else:
                criterio = f" where {campo_tabela} >= {param1}"                
        else:
            if param2 != '':
                criterio = f" where {campo_tabela} >= {param1}"
            
        
    print(f"Lendo Toda Tabela {nome_tabela.upper()} no Oracle")     
    comando_oracle_inserir = f"SELECT * FROM {nome_tabela.upper()} {criterio}"           
    cursor_oracle.execute(comando_oracle_inserir)
    linhas_oracle = cursor_oracle.fetchall()

    if exclui:
        # Construir comando SQL para criar a tabela no PostgreSQL
        print(f"Montando String de Criação da Tabela {nome_tabela.upper()} No Postgres")
        sql_criar_tabela = f"CREATE TABLE {nome_tabela.upper()} ("
        for coluna in colunas_oracle:
            nome_coluna = coluna[0]
            tipo_dado = coluna[1]
            if tipo_dado == "VARCHAR2":
                tipo_pg = "CHARACTER VARYING"
                tamanho = coluna[2]
                sql_criar_tabela += f"{nome_coluna} {tipo_pg}({tamanho}), "
            elif tipo_dado == "CHAR":
                tipo_pg = "CHARACTER VARYING"
                tamanho = coluna[2]
                sql_criar_tabela += f"{nome_coluna} {tipo_pg}({tamanho}), "
            elif tipo_dado == "NUMBER":
                tipo_pg = "NUMERIC"
                sql_criar_tabela += f"{nome_coluna} {tipo_pg}, "
            elif tipo_dado == "DATE":
                tipo_pg = "TIMESTAMP"
                sql_criar_tabela += f"{nome_coluna} {tipo_pg}, "
            else:
                raise Exception(f"Tipo de dado não suportado: {tipo_dado}")
        sql_criar_tabela = sql_criar_tabela.rstrip(', ') + ")"

        try:
            # Criar a tabela no PostgreSQL
            print(f"Apagando, Se Existir, Tabela No Postgres {nome_tabela.upper()}")                
            cursor_pg.execute(f"DROP TABLE IF EXISTS {nome_tabela.upper()}")
            cursor_pg.execute(sql_criar_tabela)
        except :
            print(f"Tabela {nome_tabela.upper()} não existe")
            #criterio = ''
    
    
    # Inserir dados no PostgreSQL
    placeholders = ', '.join(['%s'] * len(colunas_oracle))
    colunas = ', '.join([col[0] for col in colunas_oracle])
    sql_inserir = f"INSERT INTO {nome_tabela} ({colunas}) VALUES ({placeholders})"
    n = 1
    for linha in linhas_oracle:                                 
        cursor_pg.execute(sql_inserir, linha)   
        mil, resto = divmod(n,10000)                 
        if resto==0:
            conn_pg.commit()
            print(f"Inserindo a linha linha:{n}: {linha} no Postgres")    
        n = n + 1
    
    # Commit e fechar conexões
    conn_pg.commit()
    if cursor_oracle.__getstate__:
        cursor_oracle.close()
    if conn_oracle.__getstate__:
        conn_oracle.close()
    if not cursor_pg.closed:
        cursor_pg.close()
    if not conn_pg.closed:
        conn_pg.close()

    print(f"Tabela {nome_tabela} Copiada Corretamente!")


def copiar_biprod(max_data_migrada):
    # Query para buscar dados no Oracle
    oracle_query_dt = f"SELECT * FROM BI_IDADE where DTSERIE > '{max_data_migrada}'"
    
    # Copiar dados de Oracle para PostgreSQL
    columns, rows = fetch_data_from_oracle(oracle_query_dt)
    
    # Inserir dados copiados no postgres
    insert_data_into_postgres(postgres_conn_str, 'BI_IDADE', columns, rows)
    print(f"Dados Lidos lidos e escritos com sucesso!")


# Configurações das variáveis de sistema para conexão
# Ou crie as variáveis diretamente no sistema ou configure as variáveis abaixo
#os.environ['ORACLE_USR'] = 'usuario_oracle'
#os.environ['ORACLE_IPUSER'] = 'IP/USUARIO_oracle'
#os.environ['ORACLE_PSW'] = 'senha_oracle'

#os.environ['POSTGRES_DB'] = 'nome_banco_posgres'
#os.environ['POSTGRES_USR'] = 'usuario_postgres
#os.environ['POSTGRES_PSW'] = 'senha_postgres'
#os.environ['POSTGRES_IP']  = 'ip_postgres'
#os.environ['POSTGRES_PORT'] = 'port_postgres'

usro = os.environ['ORACLE_USR'] 
ipusero = os.environ['ORACLE_IPUSER'] 
pswo = os.environ['ORACLE_PSW'] 

#dbp= 'bitsi' #os.environ['POSTGRES_DB'] 
#usrp = 'bitsi' #os.environ['POSTGRES_USR']
#pswp = 'bitsi' #os.environ['POSTGRES_PSW']

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

oracledb.init_oracle_client(lib_dir=os.getenv("ORACLE_HOME") )


#Query para buscar última data migrada
#max_data_migrada = read_last_data_postgres("BI_IDADE","DTSERIE")
#print (max_data_migrada)
#copiar_biprod(max_data_migrada)

#print(f"Chamando função Copiar BI_SD3_MEDIA!")
#copiar_tabela_oracle_para_postgres('BI_SD3_MEDIA')


#SELECT count(serie) as n FROM bi_idade where dtserie > '20081230' and dtserie < '20130101'
#SELECT count(serie) as n FROM bi_idade where dtserie > '20130101' and dtserie < '20200101'
#SELECT count(serie) as n FROM bi_idade where dtserie > '20200101' and dtserie < '20240101'



print(f"Chamando função Copiar BI_IDADE!")
copiar_tabela_oracle_para_postgres('BI_IDADE',False,False,"DTSERIE","'20081230'","'20130101'")
copiar_tabela_oracle_para_postgres('BI_IDADE',False,False,"DTSERIE","'20130101'","'20200101'")
copiar_tabela_oracle_para_postgres('BI_IDADE',False,False,"DTSERIE","'20200101'","'20240101'")

#copiar_tabela_oracle_para_postgres('BI_IDADE',False,False,"DTSERIE","min","")

#print(f"Chamando função Copiar BI_SB2!")
#copiar_tabela_oracle_para_postgres('BI_SB2',"")

print(f"Tabelas Copiadas Corretamente!")