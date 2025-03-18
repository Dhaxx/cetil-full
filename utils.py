from connection import commit, CUR_FDB, fetchallmap

global EMPRESA
EMPRESA = None
EMPRESA = CUR_FDB.execute("select empresa from cadcli").fetchone()[0] if EMPRESA is None else EMPRESA

def limpa_tabela(*tabelas):
    try:
        for tabela in tabelas:
            CUR_FDB.execute(f"delete from {tabela}")
            commit()
    except Exception as e:
        print(f"Erro ao limpar a tabela {tabela}: {e}")

global dict_fornecedores
dict_fornecedores = {}
for codif, nome in CUR_FDB.execute('select codif, nome from desfor').fetchall():
    dict_fornecedores[codif] = {'nome': nome}

def cria_campo(tabela, campo):
    try:
        CUR_FDB.execute(f'alter table {tabela} add {campo} varchar(50)')
        commit()
    except Exception as e:
        print(f"Erro ao inserir coluna '{campo.upper()}' - tabela '{tabela.upper()}': {e}")
    return

global dict_produtos
dict_produtos = {}

def armazena_produtos():
    if len(dict_produtos) < 1:
        for k, v in CUR_FDB.execute('select codreduz, cadpro from desfor').fetchall():
            dict_produtos[k] = v 
    else: print('itens já estão armazenados!')

def limpa_patrimonio():
    tabelas = ('pt_movbem','pt_cadpat','pt_cadbai','pt_cadpats','pt_cadpatd','pt_cadsit','pt_cadpatg','pt_cadtip')
    for tabela in tabelas:
        limpa_tabela(tabela)