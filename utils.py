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

def armazena_fornecedores():
    for insmf, codif, nome, codant in CUR_FDB.execute("select replace(replace(replace(insmf,'.',''),'/',''),'-',''), codif, nome, codant from desfor").fetchall():
        dict_fornecedores[insmf] = {'codif': codif, 'nome': nome, 'codant': codant}

def cria_campo(tabela, campo):
    try:
        CUR_FDB.execute(f'alter table {tabela} add {campo} varchar(50)')
        commit()
    except:
        pass
    return

global dict_produtos
dict_produtos = {}

def armazena_produtos():
    for k, v in CUR_FDB.execute('select cast(codreduz as varchar(50)), cadpro from cadest').fetchall():
        dict_produtos[k] = v 

armazena_produtos() if len(dict_produtos) == 0 else ...

def limpa_patrimonio():
    tabelas = ('pt_movbem','pt_cadpat','pt_cadbai','pt_cadpats','pt_cadpatd','pt_cadsit','pt_cadpatg','pt_cadtip')
    for tabela in tabelas:
        limpa_tabela(tabela)

def cpf_cnpj(doc):
    documento = str(doc)

    if(len(documento) == 11):
        return {'tipo': '02', 'doc_nro': (documento[0:3] + '.' + documento[3: 6] + '.' + documento[6: 9] + '-' + documento[9: 11])}
    elif(len(documento) == 14):
        return {'tipo': '01', 'doc_nro': (documento[0:2] + '.' + documento[2: 5] + '.' + documento[5: 8] + '/' + documento[8:12] + '-' + documento[12:14])}
    else:
        return {'tipo': '03', 'doc_nro': documento}
    
def obter_codif_por_nome(nome_busca):
    for insmf, dados in dict_fornecedores.items():
        if dados.get("nome") == nome_busca:
            return dict_fornecedores[insmf]['codif']  # Retorna a primeira chave encontrada
    return None  # Retorna None se não encontrar

def obter_codif_por_codant(codant):
    for insmf, dados in dict_fornecedores.items():
        if dados.get("codant") == str(codant):
            return dict_fornecedores[insmf]['codif']  # Retorna a primeira chave encontrada
    return None  # Retorna None se não encontrar

def limpa_tabelas_compras():
    tabelas = [
        "licitcomiss",
        "regpreco_saldo_ant",
        "regpreco",
        "regprecohis",
        "regprecodoc",
        "cadpro",
        "cadpro_final",
        "cadpro_lance",
        "cadpro_proposta",
        "cadpro_status",
        "cadprolic_detalhe",
        "cadprolic",
        "prolic",
        "prolics",
        "cadlotelic",
        "cadlic_sessao",
        "cadlic",
        "membros",
        "comissao",
        "icadorc",
        "cadorc",
        "icadped",
        "fcadped",
        "cadped",
        "icadreq",
        "requi",
        "cadlote",
        "cadest",
        "cadsubgr",
        "cadgrupo",
        "centrocusto",
        "destino",
        "cadunimedida"
    ]

    for tabela in tabelas:
        limpa_tabela(tabela)
    commit()

EXERCICIO = CUR_FDB.execute('SELECT mexer FROM CADCLI c ').fetchone()[0]

import unicodedata

def decode_to_win1252(input_str: str) -> str:
    # Remove caracteres que não podem ser representados em WIN1252
    input_str = ''.join(c for c in input_str if c in bytes(range(256)).decode('windows-1252', errors='ignore'))
    
    # Normaliza caracteres acentuados para versões compatíveis
    input_str = unicodedata.normalize("NFKD", input_str).encode("ascii", "ignore").decode("ascii")
    
    return input_str.strip()
