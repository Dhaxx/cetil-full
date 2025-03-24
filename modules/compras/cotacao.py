from connection import commit, CUR_FDB, fetchallmap, ENTIDADE
from utils import limpa_tabela, EMPRESA, dict_produtos, dict_fornecedores, cria_campo, obter_codif_por_nome, obter_codif_por_codant

def itens():
    limpa_tabela('vcadorc', 'fcadorc', 'icadorc', 'cadorc')

    query = f"""
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
            {ENTIDADE}_COMPRAS.dbo.itempesquisa c
        where
            c.dtanopesquisa = p.dtanopesquisa
            and c.nrpesquisa = p.nrpesquisa) as qtdlote,
        o.cdOrgaoReduzido
    from
        {ENTIDADE}_COMPRAS.dbo.processopesquisa p
    left join {ENTIDADE}_COMPRAS.dbo.itempesquisa i on
        i.dtanopesquisa = p.dtanopesquisa
        and i.nrpesquisa = p.nrpesquisa
    left join {ENTIDADE}_COMPRAS.dbo.condicaopagamento f on
        f.cdcondicaopagamento = p.cdcondicaopagamento
    left join {ENTIDADE}_COMPRAS.dbo.localentrega l on
        l.cdlocalentrega = p.cdlocalentrega
    left join {ENTIDADE}_COMPRAS.dbo.orgaopesquisa o on
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
        concat(qr.nrpesquisa,substring(cast(dtanopesquisa as varchar),3,2)) nrpesquisa,
        right(replicate('0',6)+cast(qr.nrprocesso as varchar),6)+'/'+cast(coalesce(dtanoprocesso,0) % 2000 as varchar) proclic,
        concat(qr.nrprocesso, qr.dtanoprocesso%2000) numlic,
        cast(dscondicaopagamento as nvarchar(max)) dscondicaopagamento,
        local
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
    SELECT
        DISTINCT
        RIGHT(REPLICATE('0', 5) + CAST(nrpesquisa AS VARCHAR(5)), 5) + '/' + CAST(dtanopesquisa % 2000 AS VARCHAR(4)) AS numorc,
        qr.nritem,
        qr.qtdlote,
        qr.lote,
        qr.qtitempesquisa,
        qr.cdmaterial,
        qr.cdOrgaoReduzido,
        concat(qr.nrpesquisa,substring(cast(dtanopesquisa as varchar),3,2)) nrpesquisa from ({query}) as qr
        where qr.cdmaterial <> 0
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

    cotacao_anterior = 0

    for cabecalho in cabecalhos:
        if cabecalho['nrpesquisa'] != cotacao_anterior:
            numorc = cabecalho['numorc']
            num = cabecalho['num']
            ano = cabecalho['dtanopesquisa']
            dtorc = cabecalho['dtexpedicao']
            descr = cabecalho['dspesquisa'].title()
            obs = cabecalho['justificativa_contratacao'].capitalize() if cabecalho['justificativa_contratacao'] else None
            codccusto = 0
            status = 'EC'
            liberado = 'S'
            idcadorc = cabecalho['nrpesquisa']
            empresa = EMPRESA
            if cabecalho['numlic'] != 0:
                proclic = cabecalho['proclic']
                numlic = cabecalho['numlic']
            else:
                proclic = None
                numlic = None
            registropreco = "N"
            condpgto = cabecalho['dscondicaopagamento'][:30]
            local = cabecalho['local'][:60]

            CUR_FDB.execute(insert, (numorc
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
            , registropreco
            , condpgto
            , local))

            cotacao_anterior = idcadorc
    commit()

    for item in itens:
        nritem = item['nritem'] if item['qtdlote'] == 1 else str(item['lote']) + str(item['nritem']) 

        if item['lote'] > 9 or item['nritem'] > 99:
            nritem = str(item['lote'])  + '0' + str(item['nritem'])
        
        itemorc = nritem
        valor = 0
        cadpro = dict_produtos[str(item['cdmaterial'])]
        numorc = item['numorc']
        codccusto = item['cdOrgaoReduzido']
        qtd = item['qtitempesquisa']
        idcadorc = item['nrpesquisa']

        CUR_FDB.execute(insert_itens, (numorc, nritem, itemorc, valor, codccusto, cadpro, qtd, idcadorc))
    commit()

def fcadorc():
    limpa_tabela('fcadorc')
    cria_campo('fcadorc', 'insmf_ant')

    rows = fetchallmap(f"""
    select
        distinct
        right(replicate('0', 5)+cast(nrpesquisa as varchar),5)+'/'+cast(dtanopesquisa % 2000 as varchar) numorc,
        concat(nrpesquisa,substring(cast(dtanopesquisa as varchar),3,2)) nrpesquisa,
        dtanopesquisa,
        p.cdfornecedor,
        f.nmfornecedor,
        trim(CAST(CAST(nrcgccpf AS DECIMAL(14,0)) AS CHAR(14))) insmf
    from
        {ENTIDADE}_COMPRAS.dbo.propostapesquisa p
    inner join {ENTIDADE}_ALMOX.dbo.fornecedor f on
        f.cdfornecedor = p.cdfornecedor
    where
        nrpesquisa > 0
    order by
        nrpesquisa
    """)

    insert = CUR_FDB.prep("insert into fcadorc(numorc,codif,nome,valorc,id_cadorc,insmf_ant) values(?,?,?,?,?,?)")

    fornecedores_nao_identificados = set()

    for row in rows:
        numorc = row['numorc']
        insmf = row['insmf']

        if (len(insmf) > 11 and  len(insmf) < 14):
            insmf = insmf.zfill(14)
        elif len(insmf) < 11:
            insmf = insmf.zfill(11)

        codif = dict_fornecedores.get(insmf, 0)
        if codif == 0:
            codif = obter_codif_por_nome(row['nmfornecedor'])
        else:
            codif = codif['codif']

        nome = row['nmfornecedor'][:70].title()
        valorc = 0
        id_cadorc = row['nrpesquisa']

        CUR_FDB.execute(insert, (numorc, codif, nome, valorc, id_cadorc, insmf))
    commit()

def vcadorc():
    limpa_tabela('vcadorc')

    rows = fetchallmap(f"""
    select
        right(replicate('0', 5)+cast(nrpesquisa as varchar),5)+'/'+cast(dtanopesquisa % 2000 as varchar) numorc,
        concat(nrpesquisa,substring(cast(dtanopesquisa as varchar),3,2)) nrpesquisa,
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
            {ENTIDADE}_COMPRAS.dbo.propostapesquisa g
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
            {ENTIDADE}_COMPRAS.dbo.propostapesquisa g
        where
            g.nrpesquisa = p.nrpesquisa
            and g.dtanopesquisa = p.dtanopesquisa
            and g.lote = p.lote
            and g.nritemproposta = p.nritemproposta
            and g.vlcotacaoproposta > 0
            order by g.vlcotacaoproposta ) as valorvencedor,
        f.nmfornecedor,
        trim(CAST(CAST(f.nrcgccpf AS DECIMAL(14,0)) AS CHAR(14))) insmf,
        (
        select
            coalesce(max(lote),1)
        from
            {ENTIDADE}_COMPRAS.dbo.propostapesquisa c
        where
            c.dtanopesquisa = p.dtanopesquisa
            and c.nrpesquisa = p.nrpesquisa) as qtdlote
    from
        {ENTIDADE}_COMPRAS.dbo.propostapesquisa p
    inner join {ENTIDADE}_ALMOX.dbo.fornecedor f on
        f.cdfornecedor = p.cdfornecedor
    where
        nrpesquisa > 0 
    """)

    insert = CUR_FDB.prep("insert into vcadorc(numorc,codif,vlruni,vlrtot,item,id_cadorc,classe,ganhou,vlrganhou) values(?,?,?,?,?,?,?,?,?)")

    for row in rows:
        numorc = row['numorc']
        insmf = row['insmf']

        if (len(insmf) > 11 and  len(insmf) < 14):
            insmf = insmf.zfill(14)
        elif len(insmf) < 11:
            insmf = insmf.zfill(11)

        if insmf in dict_fornecedores:
            codif = dict_fornecedores[insmf]['codif']
        else:
            codif = obter_codif_por_nome(row['nmfornecedor'])

        vlruni = row['vlcotacaoproposta']
        vlrtot = row['vlcotacaoproposta'] * row['qtitemproposta']
        item = row['nritemproposta']

        if row['qtdlote'] > 1:
            item = str(row['lote']) + str(row['nritemproposta'])
        
        if row['lote'] > 9 or row['nritemproposta'] > 99:
            item = str(row['lote'])  + '0' + str(row['nritemproposta'])

        id_cadorc = row['nrpesquisa']
        classe = 'GL'
        ganhou = obter_codif_por_codant(row['ganhador'])
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