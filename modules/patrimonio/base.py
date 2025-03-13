from connection import commit, CUR_FDB, CUR_SQLS, fetchallmap
from utils import EMPRESA, limpa_tabela

def pt_cadtip():
    limpa_tabela("cadtip")

    query = CUR_SQLS.execute("""
    select
            distinct
            cdClasseReduzido as cdClassificacao,
            dsClassificacao
        from
            BORA_PATRI.dbo.classificacao c
        where
            (nrNivel >= 2
            or (nrnivel = 1
            and exists(
            select
                1
            from
                BORA_PATRI.dbo.item i
            where
                i.cdclassificacao = c.cdclassereduzido)))
        order by
            cdClasseReduzido
    """)

    insert = CUR_FDB.prep("insert into pt_cadtip(codigo_tip, empresa_tip, descricao_tip) values(?,?,?)")

    rows = fetchallmap(query)

    for row in rows:
        insert(row['cdClassificacao'], '01', row['dsClassificacao'])
    commit()

def pt_cadpatg():
    limpa_tabela("pt_cadpatg")

    values = (
        (1, EMPRESA, 'Bens Móveis'),
        (2, EMPRESA, 'Bens Imóveis'),
    )

    CUR_SQLS.executemany("insert into pt_cadpatg(codigo_gru,empresa_gru,nogru_gru) values(?,?,?)", values)

    commit()

def pt_cadsit():
    limpa_tabela("pt_cadsit")

    query = CUR_SQLS.execute("select cdEstadoConser, dsEstadoConser from estadoconservacao")

    insert = CUR_FDB.prep("insert into pt_cadsit(codigo_sit, empresa_sit, descricao_sit) values(?,?,?)")

    rows = fetchallmap(query)

    [insert(row['cdSituacao'], '01', row['dsSituacao']) for row in rows]
        
    commit()

def setores():
    limpa_tabela("pt_cadpats")
    limpa_tabela("pt_cadpatd")
    
    query = CUR_SQLS.execute("""
        select            
            cdlocalreduzido ,
            nrnivel ,
            localizacao.dslocalizacao
        from
            localizacao
        where ininativa = 0
            order by
                nrnivel
    """)

    insert = CUR_FDB.prep("insert into pt_cadpats(codigo_set, empresa_set, descricao_set) values(?,?,?)")

    CUR_FDB.execute("insert into pt_cadpatd(codigo_des,empresa_des,nauni_des) select 1, empresa, clnt1 from cadcli")
    commit()

    insert_subunidade = CUR_FDB.prep("insert into pt_cadpats(codigo_set,empresa_set,codigo_des_set, noset_set) values (?,?,?,?)")

    subunidades = []

    [subunidades.append((row['cdlocalreduzido'], EMPRESA, 1, row['dslocalizacao'])) for row in fetchallmap(query)]

    CUR_FDB.executemany(insert_subunidade, subunidades)
    commit()

def pt_cadbai():
    limpa_tabela("pt_cadbai")

    CUR_FDB.execute("insert into pt_cadbai(codigo_bai, empresa_bai, descricao_bai) values (0,%d,'Diversos')" % EMPRESA)
    commit()

    rows = fetchallmap(CUR_SQLS.execute("select cdBairro, dsBairro from bairro"))

    insert = CUR_FDB.prep("insert into pt_cadbai(codigo_bai, empresa_bai, descricao_bai) values (?,?,?)")

    [insert(row['cdBairro'], EMPRESA, row['dsBairro']) for row in rows]
    commit()
    