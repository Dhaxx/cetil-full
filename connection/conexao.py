import fdb
import pyodbc
import os
from dotenv import load_dotenv
import inspect

load_dotenv(dotenv_path='C:\\Conversao\\CETIL\\connection\\.env')
ENTIDADE = os.environ['ENTIDADE'].upper()

def _conexoes():
    global conexao_fdb
    try:
        conexao_fdb = fdb.connect(
            dsn=os.environ['FDB_DSN'],
            user=os.environ['FDB_USER'],
            password=os.environ['FDB_PWD'],
            no_db_triggers=1,
            charset='WIN1252')
        
        conexao_sqls = pyodbc.connect(
            "DRIVER={SQL Server};"
            f"SERVER={os.environ['SQLS_SRVR']};"
            f"UID={os.environ['SQLS_USER']};"
            f"PWD={os.environ['SQLS_PWD']};"
            f"PORT={os.environ['SQLS_PORT']};"
            # "Trusted_Connection=yes"
        )

        cur_fdb, cur_sqls = _cursors(conexao_fdb, conexao_sqls)

        return cur_fdb, cur_sqls
    except Exception as e:
        print("Erro ao estabelecer conexão: ", e)
        return None, None

def commit():
    global conexao_fdb
    try:
        function_name = inspect.currentframe().f_back.f_code.co_name
        conexao_fdb.commit()
        print(f'{function_name} Commited') if function_name != 'limpa_tabela' else ...
    except Exception as e:
        print("Erro ao commitar: ", e)
        conexao_fdb.rollback()

def _cursors(cnx_fdb, cnx_sqls):
    try:
        cur_fdb = cnx_fdb.cursor()
        cur_Sqls = cnx_sqls.cursor()
        return cur_fdb, cur_Sqls
    except Exception as e:
        print("Erro ao obter cursores :", e)
        return None, None
    
CUR_FDB, CUR_SQLS = _conexoes()
    
def fetchallmap(query):
    CUR_SQLS.execute(query)
    colunas = [coluna[0] for coluna in CUR_SQLS.description]
    rows = CUR_SQLS.fetchall()
    result = []
    [result.append(dict(zip(colunas, row))) for row in rows]
    return result

EXERCICIO = int(CUR_FDB.execute('select mexer from cadcli').fetchone()[0])