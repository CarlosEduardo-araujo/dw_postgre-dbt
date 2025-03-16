#imports
import yfinance as yf
import pandas as pd 
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os 

load_dotenv()


DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(DATABASE_URL)

def salvar_no_postgres(df, schema = 'public'):
    df.to_sql('commodities', engine, if_exists='replace', schema=schema, index=True, index_label='Date')


# DB_THRE = os.getenv('DB_THREADS_PROD')
# DB_TYPE = os.getenv('DB_TYPE_PROD')
# DBT_PRO = os.getenv('DBT_PROFILES_DIR')

#imports das variaveis de ambientes 

lista_commodities = ['CL=F', 'GC=F', 'SI=F']

def buscar_dados_commodities(simbolo, periodo='5d', intervalo='1d'):
    ticker = yf.Ticker(simbolo)
    dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados


def buscar_todos_dados_commodities(commodities):
    todos_os_dados = []
    for simbolo in commodities:
        dados = buscar_dados_commodities(simbolo)
        todos_os_dados.append(dados)
    return pd.concat(todos_os_dados)


if __name__ == '__main__': 
    dados_concatenados = buscar_todos_dados_commodities(lista_commodities)
    salvar_no_postgres(dados_concatenados, schema=DB_SCHEMA)


    



#pegar a cotação dos arquivos


# concatenar os meus ativos(1.. 2.. .3..) -> (1)


# salvar no banco de dados 

