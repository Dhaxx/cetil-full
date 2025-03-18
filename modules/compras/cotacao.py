from connection import commit, CUR_FDB, fetchallmap
from utils import limpa_tabela, EMPRESA, dict_produtos, dict_fornecedores

def itens():
    limpa_tabela(('icadorc', 'cadorc'))

    query = """
    select
        p.nrpesquisa,
        p.dtanopesquisa,
        coalesce(p.nrprocesso,
        0) as nrprocesso,
        p.dtanoprocesso,
        p.dspesquisa,
        p.dtexpedicao,
        p.justificativa_contratacao,
        i.lote,
        i.nritem,
        i.cdmaterial,
        i.qtitempesquisa,
        f.dscondicaopagamento,
        coalesce(l.dslocalentrega + ' - Compl.: ' + l.dscomplemento,
        '') as local,
        (
        select
            max(lote)
        from
            itempesquisa c
        where
            c.dtanopesquisa = p.dtanopesquisa
            and c.nrpesquisa = p.nrpesquisa) as qtdlote,
        o.cdOrgaoReduzido
    from
        processopesquisa p
    inner join itempesquisa i on
        i.dtanopesquisa = p.dtanopesquisa
        and i.nrpesquisa = p.nrpesquisa
    left join condicaopagamento f on
        f.cdcondicaopagamento = p.cdcondicaopagamento
    left join localentrega l on
        l.cdlocalentrega = p.cdlocalentrega
    left join orgaopesquisa o on
        o.dtAnoPesquisa = p.dtAnoPesquisa
        and o.nrPesquisa = p.nrPesquisa
    """

    cabecalhos = fetchallmap(f"""
    select distinct
        right(replicate('0', 5)+cast(nrpesquisa as varchar),5)+'/'+cast(dtanopesquisa % 2000 as varchar) numorc,
        right(replicate('0', 5)+cast(nrpesquisa as varchar),5) num,
        qr.dtanopesquisa,
        qr.dtexpedicao,
        cast(qr.dspesquisa as nvarchar(max)) dspesquisa,
        qr.justificativa_contratacao,
        qr.cdOrgaoReduzido,
        qr.nrpesquisa,
        right(replicate('0',6)+cast(qr.nrprocesso as varchar),6)+'/'+cast(coalesce(dtanoprocesso,0) % 2000 as varchar) proclic,
        concat(qr.nrprocesso, qr.dtanoprocesso%2000)
    from ({query}) as qr
    """)

    insert = CUR_FDB.prep("""
    insert
            into
            cadorc(numorc,
            num,
            ano,
            dtorc,
            descr,
            obs,
            codccusto,
            status,
            liberado,
            id_cadorc,
            empresa,
            proclic,
            numlic,
            registropreco,
            condpgto,
            local_entrega)
        values(?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,
        ?,?)
    """)

    itens = fetchallmap(f"""
    select
        distinct
        right(replicate('0',
        5)+ cast(nrpesquisa as varchar),
        5)+ '/' + cast(dtanopesquisa % 2000 as varchar) numorc,
        qr.nritem,
        qr.qtdlote,
        qr.lote,
        qr.qtitempesquisa,
        qr.cdmaterial,
        qr.cdOrgaoReduzido,
        qr.nrpesquisa from ({query})
    """)

    insert_itens = CUR_FDB.prep("""
    insert
        into
        icadorc(numorc,
        item,
        itemorc,
        valor,
        codccusto,
        cadpro,
        qtd,
        id_cadorc)
    values(?,?,?,?,?,?,?,?)
    """)

    cotacao_atual = 0

    for cabecalho in cabecalhos:
        if cabecalho['nrpesquisa'] == cotacao_anterior:
            cotacao_atual = cabecalho['nrpesquisa']
            numorc = cabecalho['numorc']
            num = cabecalho['num']
            ano = cabecalho['dtanopesquisa']
            dtorc = cabecalho['dtexpedicao']
            descr = cabecalho['dspesquisa'].title()
            obs = cabecalho['justificativa_contratacao'].capitalize()
            codccusto = cabecalho['cdOrgaoReduzido']
            status = 'EC'
            liberado = 'S'
            idcadorc = cabecalho['nrpesquisa']
            empresa = EMPRESA
            if numlic != 0:
                proclic = cabecalho['proclic']
                numlic = cabecalho['numlic']
            else:
                proclic = None
                numlic = None
            registropreco = "N"
            condpgto = cabecalho['dscondicaopagamento'][:30]
            local = cabecalho['local'][:60]

            CUR_FDB.execute(insert, (cotacao_atual
            , numorc
            , num
            , ano
            , dtorc
            , descr
            , obs
            , codccusto
            , status
            , liberado
            , idcadorc
            , empresa
            , proclic
            , numlic
            , proclic
            , numlic
            , registropreco
            , condpgto
            , local))

            cotacao_anterior = idcadorc
    commit()

    for item in itens:
        nritem = item.nritem if item.qtdlote == 1 else str(item.lote) + str(item.nritem) 

        if item.lote > 9 or item.nritem > 99:
            nritem = str(item.lote)  + '0' + str(item.nritem)
        
        itemorc = nritem
        valor = 0
        cadpro = dict_produtos[item['cdmaterial']]
        numorc = item['numorc']
        codccusto = item['cdOrgaoReduzido']
        qtd = item['qtitempesquisa']
        idcadorc = item['nrpesquisa']

        CUR_FDB.execute(insert, (numorc, nritem, itemorc, valor, codccusto, cadpro, qtd, idcadorc))
    commit()

def fcadorc():
    limpa_tabela('fcadorc')

    rows = fetchallmap("""
    select
        distinct nrpesquisa,
        dtanopesquisa,
        p.cdfornecedor,
        f.nmfornecedor,
        f.nrcgccpf
    from
        propostapesquisa p
    inner join fornecedor f on
        f.cdfornecedor = p.cdfornecedor
    where
        nrpesquisa > 0
    order by
        nrpesquisa
    """)

    insert = CUR_FDB.prep("insert into fcadorc(numorc,codif,nome,valorc,id_cadorc) values(?,?,?,?,?)")

    for row in rows:
        numorc = row['numorc']
        codif = dict_fornecedores[row['cdfornecedor']]
        nome = row['nmfornecedor'][:70].title()
        valorc = 0
        id_cadorc = row['nrpesquisa']

        CUR_FDB.execute(insert, (numorc, codif, nome, valorc, id_cadorc))
    commit()

def vcadorc():
    limpa_tabela('vcadorc')

    rows = fetchallmap("""
    select
        nrpesquisa,
        dtanopesquisa,
        p.cdfornecedor,
        lote,
        nritemproposta,
        vlcotacaoproposta,
        qtitemproposta,
        invencedor,
        (
        select
            top 1 g.cdfornecedor
        from
            propostapesquisa g
        where
            g.nrpesquisa = p.nrpesquisa
            and g.dtanopesquisa = p.dtanopesquisa
            and g.lote = p.lote
            and g.nritemproposta = p.nritemproposta
            and g.vlcotacaoproposta > 0
            order by g.vlcotacaoproposta ) as ganhador,
        (
        select
            top 1 g.vlcotacaoproposta
        from
            propostapesquisa g
        where
            g.nrpesquisa = p.nrpesquisa
            and g.dtanopesquisa = p.dtanopesquisa
            and g.lote = p.lote
            and g.nritemproposta = p.nritemproposta
            and g.vlcotacaoproposta > 0
            order by g.vlcotacaoproposta ) as valorvencedor,
        f.nmfornecedor,
        f.nrcgccpf,
        (
        select
            max(lote)
        from
            itempesquisa c
        where
            c.dtanopesquisa = p.dtanopesquisa
            and c.nrpesquisa = p.nrpesquisa) as qtdlote
    from
        propostapesquisa p
    inner join fornecedor f on
        f.cdfornecedor = p.cdfornecedor
    where
        nrpesquisa > 0 
    """)

    insert = CUR_FDB.prep("insert into vcadorc(numorc,codif,vlruni,vlrtot,item,id_cadorc,classe,ganhou,vlrganhou) values(?,?,?,?,?,?,?,?,?)")

    for row in rows:
        numorc = f'{row.nrpesquisa:05}' + '/' + str(row.dtanopesquisa)[2:4]
        codif = dict_fornecedores(row['cdfornecedor'])
        vlruni = row['vlcotacaoproposta']
        vlrtot = row['vlcotacaoproposta'] * row['qtitemproposta']
        item = row['nritemproposta']

        if row['qtdlote'] > 1:
            item = str(row['lote']) + str(row['nritemproposta'])
        
        if row['lote'] > 9 or row['nritemproposta'] > 99:
            item = str(row['lote'])  + '0' + str(row['nritemproposta'])

        id_cadorc = row['nrpesquisa']
        classe = 'GL'
        ganhou = dict_fornecedores(row['ganhador'])
        vlrganhou = row['valorvencedor']

        CUR_FDB.execute(insert, (numorc, codif, vlruni, vlrtot, item, id_cadorc, classe, ganhou, vlrganhou))
    commit()

    CUR_FDB.execute("update fcadorc f set f.valorc = (select sum(v.vlrtot) from vcadorc v where f.codif = v.codif and f.id_cadorc = v.id_cadorc)")
    CUR_FDB.execute("update icadorc_cot c set c.codif = (select first 1 v.ganhou from vcadorc v where v.id_cadorc = c.id_cadorc and v.item = c.item)")
    CUR_FDB.execute("""
    MERGE INTO icadorc_cot c
    USING (
        SELECT
            v.numorc,
            v.item,
            SUM(v.VLRUNI) AS total_vlruni,
            COUNT(v.CODIF) AS total_itens
        FROM
            vcadorc v
        WHERE
            v.VLRUNI > 0
        GROUP BY
            v.numorc,
            v.item
    ) v
    ON (c.numorc = v.numorc AND c.item = v.item)
    WHEN MATCHED THEN
        UPDATE SET
            c.valunt = CASE
                WHEN v.total_itens > 0 THEN v.total_vlruni / v.total_itens
                ELSE c.valunt -- Mantém o valor atual se a divisão for inválida
            END;
    """)
    CUR_FDB.execute("update icadorc_cot set valtot = valunt * qtd")
    CUR_FDB.execute("update icadorc_cot set tipo = 'M' , flg_aceito = 'S'")
    CUR_FDB.execute("update icadorc_cot set valunt = 0 where valunt is null")
    CUR_FDB.execute("update icadorc_cot t set t.valtot = qtd * valunt where valtot is null")
    commit()