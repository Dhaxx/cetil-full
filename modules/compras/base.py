from connection import commit, CUR_FDB, CUR_SQLS, fetchallmap
from utils import limpa_tabela

def Cadunimedida():
    limpa_tabela("cadunimedida")

    rows = fetchallmap("SELECT * FROM UnidadeMedida")

    [CUR_FDB.execute("insert into cadunimedida(sigla, descricao) values (?, ?)", (row['sgUnidadeMedida'], row['dsUnidadeMedida'])) for row in rows]
    
    commit()

def cadgrupo():
    pass

