import fdb
import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

def _conexoes():
    try:
        conexao_fdb = fdb.connect(
            dsn=os.environ['FDB_DSN'],
            user=os.environ['FDB_USER'],
            password=os.environ['FDB_PWD'],
            host=os.environ['FDB_HOST'],
            no_db_triggers=1,
            charset='WIN1252')
        
        pyodbc.setDecimalSeparator(',')
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

def commit(cnx_fdb):
    try:
        cnx_fdb.commit()
        print('Commited')
    except Exception as e:
        print("Erro ao commitar: ", e)
        cnx_fdb.rollback()

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