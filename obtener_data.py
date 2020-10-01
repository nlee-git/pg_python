import simplejson as json
import psycopg2 as psy
import pandas as pd
import pandas.io.sql as psql
from datetime import datetime

def get_data_from_db(db_params):
    con = psy.connect(host=db_params['host'],
                      port=db_params['port'],
                      dbname=db_params['dbname'],
                      user=db_params['user'],
                      password=db_params['password'])
    query = "select version(),pid, datname, usename , application_name, client_addr, state, query_start, query from pg_catalog.pg_stat_activity limit 10;"
    resultado = psql.read_sql(query, con)
    
    df = pd.DataFrame(resultado,columns=['version','pid', 'datname', 'usename' , 'application_name', 'client_addr', 'state', 'query_start', 'query'])
    now = datetime.now()
    df.to_feather(f"/home/mint/proyecto/pg_python/data_servers/{now:{db_params['cliente']}%Y-%m-%d %H.%M.%S.feather}")
    
with open('/home/mint/proyecto/pg_python/secret_conexion_2.json') as conex_json:
    parametros = json.load(conex_json)

for dbdata in parametros:
    get_data_from_db(dbdata)