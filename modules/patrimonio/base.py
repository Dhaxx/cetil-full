from connection import commit, CUR_FDB, fetchallmap
from utils import EMPRESA, limpa_tabela

def pt_cadtip():
    limpa_tabela("pt_cadtip")

    rows = fetchallmap("""
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

    for row in rows:
        CUR_FDB.execute(insert, (row['cdClassificacao'], EMPRESA, row['dsClassificacao']))
    commit()

def pt_cadpatg():
    limpa_tabela("pt_cadpatg")

    values = (
        (1, EMPRESA, 'Bens Móveis'),
        (2, EMPRESA, 'Bens Imóveis'),
    )

    CUR_FDB.executemany("insert into pt_cadpatg(codigo_gru,empresa_gru,nogru_gru) values(?,?,?)", (values))

    commit()

def pt_cadsit():
    limpa_tabela("pt_cadsit")

    rows = fetchallmap("select cdEstadoConser, dsEstadoConser from BORA_PATRI.dbo.estadoconservacao")

    insert = CUR_FDB.prep("insert into pt_cadsit(codigo_sit, empresa_sit, descricao_sit) values(?,?,?)")

    [CUR_FDB.execute(insert, ((row['cdEstadoConser'], EMPRESA, row['dsEstadoConser']))) for row in rows]
        
    commit()

def setores():
    limpa_tabela("pt_cadpats")
    limpa_tabela("pt_cadpatd")
    
    rows = fetchallmap("""
        select distinct         
            cdlocalreduzido ,
            nrnivel ,
            localizacao.dslocalizacao
        from
            BORA_PATRI.dbo.localizacao
        where ininativa = 0
            order by
                nrnivel
    """)

    CUR_FDB.execute("insert into pt_cadpatd(codigo_des,empresa_des,nauni_des) select 1, empresa, clnt1 from cadcli")
    commit()

    insert_subunidade = CUR_FDB.prep("insert into pt_cadpats(codigo_set,empresa_set,codigo_des_set, noset_set) values (?,?,?,?)")

    subunidades = []
    cdlocalreduzido_set = set()

    for row in rows:
        cdlocalreduzido = row['cdlocalreduzido']
        if cdlocalreduzido not in cdlocalreduzido_set:
            subunidades.append((cdlocalreduzido, EMPRESA, 1, row['dslocalizacao']))
            cdlocalreduzido_set.add(cdlocalreduzido)

    CUR_FDB.executemany(insert_subunidade, subunidades)
    commit()

def pt_cadbai():
    limpa_tabela("pt_cadbai")

    CUR_FDB.execute("insert into pt_cadbai(codigo_bai, empresa_bai, descricao_bai) values (0,%d,'Diversos')" % EMPRESA)
    commit()

    rows = fetchallmap("select cdtpbaixa, dstpbaixa from BORA_PATRI.dbo.tipobaixa")

    insert = CUR_FDB.prep("insert into pt_cadbai(codigo_bai, empresa_bai, descricao_bai) values (?,?,?)")

    [CUR_FDB.execute(insert, ((row['cdtpbaixa'], EMPRESA, row['dstpbaixa']))) for row in rows]
    commit()