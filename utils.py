from connection import commit, CUR_FDB, CUR_SQLS, fetchallmap

EMPRESA = CUR_FDB.execute("select empresa from cadcli").fetchone()[0]

def limpa_tabela(tabela):
    try:
        CUR_FDB.execute(f"delete from {tabela}")
        commit()
    except Exception as e:
        print(f"Erro ao limpar a tabela {tabela}: {e}")