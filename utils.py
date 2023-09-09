from os.path import expanduser, exists
from datetime import datetime
from pprint import pprint
import pandas as pd 
import sqlalchemy as sa 
import psycopg2 
import hashlib
import vecs
import yaml
from sqlalchemy import dialects


'''
    SECRETS
'''
def open_secret(secret_name):
    try:
        # comment: 
        obj = open(secret_name)
    except Exception as e:
        home = expanduser('~')
        obj = open(f"{home}/.secrets/{secret_name}", 'r')
    else:
        # comment: 
        raise("Could not locate the relevant secret file")
    return obj


def get_secret_dict(secret_name, keys, separator=':'):
    """
    Purpose: one
    """
    with open_secret(secret_name=secret_name) as file:
        values = file.read().strip().split(separator)
        secret_dict = dict(zip(keys,values))
    return secret_dict
    
# end def

'''
    CONNECTIONS
'''
def sql_engine(secret_key='aws_rds', dbtype='mariadb'):

    params = ['host','port','database','user','password']
    params_dict = get_secret_dict(secret_key, params) #dict(zip(params,values))
    url_template = '{user}:{password}@{host}:{port}/{database}'

    if dbtype == 'mariadb':
        url_template = f'mariadb+pymysql://{url_template}'
    # elif dbtype == 'supabase':
    #     url_template = f'postgresql://postgres:{password}@{host}:5432/postgres'
    else:
        url_template = f'postgresql://{url_template}'
    url = url_template.format(**params_dict)

    engine = sa.create_engine(url)
    return engine 


def sql_connection(secret_key='aws_rds'):

    params = ['host','port','dbname','user','password']
    params_dict = get_secret_dict(secret_key, params) 

    conn_template = "host={host} port={port} dbname={dbname} user={user} password={password}"
    conn_params = conn_template.format(**params_dict) 

    return psycopg2.connect(conn_params)

'''
    DATA WRITES
'''
def write_data(secret_key, dbtype, schema_name, table_name, data, dtype={}, params ={}):
    
    sql_params = {
        'schema': schema_name,
        'chunksize': 100,
        'index': None,
        'if_exists': 'append',
        'method': 'multi'
        }
    
    if 'embedding' in data.columns:
        dtype.update({'embedding': dialects.postgresql.JSONB})
    
    sql_params.update(params)

    data.to_sql(name=table_name, 
                con=sql_engine(secret_key=secret_key, dbtype=dbtype),
                dtype=dtype,
                **sql_params
            )


def write_vecsdata(secret_key):
    params = ['host','port','database','user','password']
    params_dict = get_secret_dict(secret_key, params)

    url_template = 'postgresql://{user}:{password}@{host}:{port}/{database}'
    url = url_template.format(**params_dict)

    vx = vecs.create_client(url)
    docs = vx.get_or_create_collection(
                name="chunks", 
                dimension=3
            )

def get_data(query, secret_key, dbtype):

    return pd.read_sql_query(query, sql_engine(secret_key, dbtype))


'''
    UTILS
'''
def hash_file_contents(fp, how='md5'):
    """
    Purpose: 
    """

    if not exists(fp):
        return None

    with open(fp, 'rb') as f:
        return hashlib.md5(f.read()).digest()
    
def hash_file_str(s, how='md5'):
    if s is None:
        return None
    return hashlib.md5(s.encode()).hexdigest()[:6]

def get_now_timestamp():
    """
    Purpose: 
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def print_section(title):
    """
    Purpose: 
    """
    separator = '='*100
    s = f"""\n{separator}\n\t\t{title}\n{separator}"""
    return s
    
# end def

# end def
    
# end def