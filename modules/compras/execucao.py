from connection import commit, CUR_FDB, fetchallmap, ENTIDADE
from utils import limpa_tabela, EMPRESA, dict_produtos, dict_fornecedores, cria_campo, obter_codif_por_nome

def pedidos():
    cria_campo("cadped","conv_empenho")
    cria_campo("cadped","conv_pkemp")
    cria_campo("cadped","conv_empenho_ano")

    rows = fetchallmap(f"""
    select
        sum(vlcotacaoproposta) as vlcotacaoproposta,
        sum(vltotalitem) as vltotalitem,
        sum(qtdeitem) as qtdeitem,
        registro_preco,
        cod_registro,
        dtanoprocesso,
        nrprocesso,
        cdfornecedor,
        nrempenho,
        dtanoempenho,
        dsobjeto,
        nritem,
        lote,
        cddespesa,
        dtempenho,
        cdorgaoreduzido,
        nrempenhocp,
        dtanoempenhocp,
        nrinstrumentocontratual,
        dtanoinstrumentocontratual,
        dscondicaopagamento,
        nmorgao,
        nmfornecedor,
        nrcgccpf,
        maisdeumlote,
        cdmaterial,
        ID_ProcessoLicitatorio,
        CONCAT(ID_ProcessoLicitatorio, '-', nritem, '-', cdmaterial, '-', lote) codant
    from
        (
        select
            case
                when e.ehregistropreco = '0' then 'N'
                else 'S'
            end registro_preco,
            0 as cod_registro,
            e.dtanoprocesso,
            e.nrprocesso,
            e.cdfornecedor,
            e.nrempenho,
            e.dtanoempenho,
            l.dsobjeto,
            e.nritem,
            e.lote,
            e.cddespesa,
            e.dtempenho,
            e.qtdeitem,
            e.vlcotacaoproposta,
            e.vltotalitem,
            e.cdorgaoreduzido,
            e.nrempenhocp,
            e.dtanoempenhocp,
            e.nrinstrumentocontratual,
            e.dtanoinstrumentocontratual,
            cast(cp.dscondicaopagamento as varchar) as dscondicaopagamento,
            o.nmorgao,
            z.nmfornecedor,
            trim(CAST(CAST(z.nrcgccpf AS DECIMAL(14,0)) AS CHAR(14))) nrcgccpf,
            case
                when exists(
                select
                    1
                from
                    {ENTIDADE}_COMPRAS.dbo.proposta b
                where
                    e.dtanoprocesso = b.dtanoprocesso
                    and e.nrprocesso = b.nrprocesso
                    and e.lote <> b.lote) 
                                            then 'S'
                else 'N'
            end as maisdeumlote,
            coalesce((
            select
                top 1 i.cdmaterial
            from
                {ENTIDADE}_ALMOX.dbo.INTEGRAINSTRUMENTOPEDIDO i
            where
                i.nrprocesso = e.nrprocesso
                and i.dtanoprocesso = e.dtanoprocesso
                and i.cdtipoprocesso = e.cdtipoprocesso
                and i.lote = e.lote
                and i.nritem = e.nritem),
            (
            select
                top 1 i.cdmaterial
            from
                {ENTIDADE}_COMPRAS.dbo.ITEMOBJETO i
            where
                i.nrprocesso = e.nrprocesso
                and i.dtanoprocesso = e.dtanoprocesso
                and i.cdtipoprocesso = e.cdtipoprocesso
                and i.lote = e.lote
                and i.nritem = e.nritem)
                                            ) as cdmaterial,
        ID_ProcessoLicitatorio
        from
            {ENTIDADE}_COMPRAS.dbo.dataview_pedidoempenho e
        inner join {ENTIDADE}_COMPRAS.dbo.processolicitatorio as l on
            e.dtanoprocesso = l.dtanoprocesso
            and e.nrprocesso = l.nrprocesso
            and e.cdtipoprocesso = l.cdtipoprocesso
        left join {ENTIDADE}_COMPRAS.dbo.condicaopagamento as cp on
            cp.cdcondicaopagamento = l.cdcondicaopagamento
        left join {ENTIDADE}_COMPRAS.dbo.orgao as o on
            e.cdorgaoreduzido = o.cdorgaoreduzido
            and e.dtanoempenho = o.dtano
        inner join {ENTIDADE}_ALMOX.dbo.fornecedor z on
            z.cdfornecedor = e.cdfornecedor) as p
    where cdmaterial is not Null
    group by
        registro_preco,
        cod_registro,
        dtanoprocesso,
        nrprocesso,
        cdfornecedor,
        nrempenho,
        dtanoempenho,
        dsobjeto,
        nritem,
        lote,
        cddespesa,
        dtempenho,
        cdorgaoreduzido,
        nrempenhocp,
        dtanoempenhocp,
        nrinstrumentocontratual,
        dtanoinstrumentocontratual,
        dscondicaopagamento,
        nmorgao,
        nmfornecedor,
        nrcgccpf,
        maisdeumlote,
        cdmaterial,
        ID_ProcessoLicitatorio
    order by
        dtanoempenho,
        nrempenho""")

    insert_cabecalho = CUR_FDB.prep(
        """
        insert
            into
            cadped (
                numped
                , num
                , ano
                , codif
                , datped
                , ficha
                , codccusto
                , entrou
                , numlic
                , proclic
                , localentg
                , condpgto
                , prozoentrega
                , obs
                , id_cadped
                , empresa
                , aditamento
                , contrato
                , npedlicit
                , id_cadpedlicit 
                , conv_empenho
                , conv_empenho_ano               
            )
        values(?,?,?,?,?,
               ?,?,?,?,?,
               ?,?,?,?,?,
               ?,?,?,?,?,?,?)""")
    
    insert_itens = CUR_FDB.prep(
        """
        insert
            into
            icadped (
                numped
                , id_cadped
                , item
                , cadpro
                , codccusto
                , qtd
                , prcunt
                , prctot
                , ficha
                , desdobro
                , marca
            )
        values (?,?,?,?,?,?,?,?,?,?,?)""")
    
    pedido_atual = ''
    
    fichas = fetchallmap(f"""
        select
        codigo_despesa_principal as fichaprincipal,
        cddespesa as fichasecundaria
    from
        {ENTIDADE}_COMPRAS.dbo.despesa
    where
        dtano = 2025
        and codigo_despesa_principal is not null
    """)

    id_cadped = 0
    try:
        id_cadped = CUR_FDB.execute('select coalesce(id,0) from pk where tabela = cadped').fetchone()[0]
    except:
        pass

    itens_cadpro = {codant: (codccusto, item, cadpro, codif) for codant, codccusto, item, codif, cadpro in CUR_FDB.execute("SELECT cc.CODANT, co.CODCCUSTO, co.item, co.codif, co.cadpro FROM cadpro co JOIN cadprolic cc ON co.item = cc.item AND co.numlic = cc.numlic")}
    exercicio = CUR_FDB.execute('SELECT mexer FROM CADCLI c ').fetchone()[0]

    for row in rows:
        ficha = None
        if row['dtanoempenho'] == exercicio:
            ficha = next((x['ficha'] for x in fichas if x['fichasecundaria'] == row['cddespesa']), None)

        nroPedido = f'{row['nrempenho']:05}' + '/' + str(row['dtanoempenho'])[2:4]

        if nroPedido != pedido_atual:
            id_cadped += 1
            pedido_atual = nroPedido
            numped = nroPedido
            num = f'{row['nrempenho']:05}'
            ano = row['dtanoempenho']
            
            insmf = row['nrcgccpf']

            if (len(insmf) > 11 and  len(insmf) < 14):
                insmf = insmf.zfill(14)
            elif len(insmf) < 11:
                insmf = insmf.zfill(11)

            codif = dict_fornecedores.get(insmf, 0)
            if codif == 0:
                codif = obter_codif_por_nome(row['nmfornecedor'])
            else:
                codif = codif['codif']

            datped = row['dtempenho']
            entrou = 'N'
            numlic = row['ID_ProcessoLicitatorio']
            proclic = f'{row['nrprocesso']:06}' '/' + str(row['dtanoprocesso'])[2:4]
            localentg = None
            condpgto = row['dscondicaopagamento']
            prozoentrega = None
            obs = row['dsobjeto']
            aditamento = None
            contrato = None
            conv_empenho = row['nrempenhocp']
            conv_empenho_ano = row['dtanoempenhocp']
            codccusto = 0

            if row['nrinstrumentocontratual'] is not None:
                contrato = f'{row['nrinstrumentocontratual']}/{str(row['dtanoinstrumentocontratual'])[2:]}'
            npedlicit = None
            id_cadpedlicit = None

            try:
                CUR_FDB.execute(insert_cabecalho, (numped, num, ano, codif, datped
                                            , ficha, codccusto, entrou, numlic
                                            , proclic, localentg, condpgto, prozoentrega
                                            , obs, id_cadped, EMPRESA, aditamento
                                            , contrato, npedlicit, id_cadpedlicit, conv_empenho, conv_empenho_ano))
            except:
                print(f'Erro ao inserir pedido {numped}')
        
        codant = row['codant']        
        item_info = itens_cadpro.get(codant, 0)

        if item_info == 0:
            if row['lote'] > 9 or row['nritem'] > 99:
                item = str(row['lote'])  + '0' + str(row['nritem'])
            cadpro = dict_produtos[str(row['cdmaterial'])]
            codccusto = row['cdorgaoreduzido']
            marca = None
        else:
            item = item_info[1]
            cadpro = item_info[2]
            codccusto = item_info[0]
            marca = item_info[3]
        
        qtd = row['qtdeitem']
        prcunt = row['vlcotacaoproposta']
        prctot = row['vltotalitem']
        desdobro = None

        try:
            CUR_FDB.execute(insert_itens, (numped, id_cadped, item, cadpro, codccusto,
                                        qtd, prcunt, prctot,ficha,desdobro, marca))
        except:
            print(f'Erro ao inserir item {item} do pedido {numped}')
    commit()

    CUR_FDB.execute("""
    MERGE INTO cadped a 
    USING (SELECT max(codccusto) orgao, id_cadped FROM icadped GROUP BY id_cadped) b
    ON a.id_cadped = b.id_cadped
    WHEN MATCHED THEN UPDATE SET a.codccusto = b.orgao""")

    CUR_FDB.execute(
    """
    update
        cadped p
    set
        p.conv_pkemp = (
        select
            first 1 d.pkemp
        from
            despes d
        where
            d.id_nrempenho = p.conv_empenho
            and d.id_dtanoemissao = p.conv_empenho_ano
            and d.vadem > 0
                        )
    where
        p.npedlicit is null
    """)

    CUR_FDB.execute("MERGE INTO DESPES a USING (select c.numped, c.id_cadped, c.conv_pkemp from cadped c) b ON a.pkemp = b.conv_pkemp WHEN MATCHED THEN UPDATE SET a.id_cadped = b.id_cadped, a.numped = b.numped")

    CUR_FDB.execute("""
    update
        cadped p
    set
        p.codatualizacao_rp = 0
    where
        p.numlic is not null
        and exists(
        select
            1
        from
            cadlic c
        where
            c.numlic = p.numlic
            and c.registropreco = 'S')""")
    commit()


def autorizacao():
    rows = fetchallmap(f"""
    select
        l.nrinstrumentocontratual,
        l.dtanoinstrumentocontratual,
        l.tpinstrumentocontratual,
        l.nrprocesso,
        l.dtanoprocesso,
        l.cdfornecedor,
        l.dtinstrumentocontratual,
        l.dtentrega,
        i.lote,
        i.nritem,
        i.qtitem,
        i.valor_unitario,
        e.nr_empenho,
        e.dt_ano_empenho,
        z.dslocalentrega,
        z.dscomplemento,
        i.cdmaterial,
        case
            when exists(
            select
                1
            from
                {ENTIDADE}_COMPRAS.dbo.proposta b
            where
                l.dtanoprocesso = b.dtanoprocesso
                and l.nrprocesso = b.nrprocesso
                and i.lote <> b.lote) 
                                            then 'S'
            else 'N'
        end as maisdeumlote,
        CONCAT(ID_ProcessoLicitatorio, '-', nritem, '-', cdmaterial, '-', lote) codant
    from
        {ENTIDADE}_COMPRAS.dbo.instrumentocontratual l
    inner join {ENTIDADE}_COMPRAS.dbo.iteminstrumentocontratual i on
        l.nrinstrumentocontratual = i.nrinstrumentocontratual
        and l.dtanoinstrumentocontratual = i.dtanoinstrumentocontratual
        and l.tpinstrumentocontratual = i.tpinstrumentocontratual
    inner join {ENTIDADE}_COMPRAS.dbo.instrumento_contratual_empenho e on
        e.nr_instrumento_contratual = l.nrinstrumentocontratual
        and e.dt_ano_instrumento_contratual = l.dtanoinstrumentocontratual
        and e.tp_instrumento_contratual = l.tpinstrumentocontratual
        and i.fk_instrumento_contratual_empenho = e.pkid
    left join {ENTIDADE}_COMPRAS.dbo.localentrega z on
        z.cdlocalentrega = l.cdlocalentrega
    where
        e.nr_empenho <> 0
        and dt_ano_empenho <> 0
    order by
        l.nrinstrumentocontratual,
        e.dt_ano_empenho,
        e.nr_empenho""")

    id_cadped = CUR_FDB.execute("SELECT COALESCE(MAX(ID_CADPED),0) FROM CADPED").fetchone()[0]

    numero = CUR_FDB.execute("SELECT CAST(COALESCE(MAX(NUM),0) AS INTEGER) FROM CADPED").fetchone()[0]

    lista_pedidos = CUR_FDB.execute("select * from cadped where id_cadpedlicit is null").fetchallmap()

    insert_cabecalho = CUR_FDB.prep("""
    insert
        into
        cadped (
            numped
            , num
            , ano
            , codif
            , datped
            , codccusto
            , entrou
            , numlic
            , proclic
            , localentg
            , condpgto
            , prozoentrega
            , obs
            , id_cadped
            , empresa
            , aditamento
            , contrato
            , npedlicit
            , id_cadpedlicit                
        )
    values(?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?)""")
    
    insert_itens = CUR_FDB.prep("""
    insert
        into
        icadped (
            numped
            , id_cadped
            , item
            , cadpro
            , codccusto
            , qtd
            , prcunt
            , prctot
            , ficha
            , desdobro
            , marca
        )
    values (?,?,?,?,?,
            ?,?,?,?,?,?)""")
    
    pedido_atual = ""
    itens_cadpro = {codant: (codccusto, item, cadpro, codif) for codant, codccusto, item, codif, cadpro in CUR_FDB.execute("SELECT cc.CODANT, co.CODCCUSTO, co.item, co.codif, co.cadpro FROM cadpro co JOIN cadprolic cc ON co.item = cc.item AND co.numlic = cc.numlic")}
    
    for row in rows:
        numero_composto = f'{row['nrinstrumentocontratual']}_{row['dtanoinstrumentocontratual']}_{row['tpinstrumentocontratual']}_{row['nr_empenho']}'
        if numero_composto != pedido_atual:
            pedido_atual = numero_composto
            numero += 1
            num = f'{numero:05}'
            numped = f'{num}/{str(row['dtanoprocesso'])[2:]}'
            ano = row['dtanoprocesso']

            npedlicit = f'{row['nr_empenho']:05}/{str(row['dt_ano_empenho'])[2:]}'
            try:
                pedido = next(x for x in lista_pedidos if x['numped'] == npedlicit)
            except:
                print(f'pedido: {npedlicit} - Não encontrado!')

            id_cadpedlicit = pedido['id_cadped']
            codif = pedido['codif']
            condpgto = pedido['condpgto']
            prozoentrega = pedido['prozoentrega']
            obs = pedido['obs']
            numlic = pedido['numlic']
            proclic = pedido['proclic']
            contrato = pedido['contrato']
            datped = row['dtinstrumentocontratual']
            entrou = 'N'
            codccusto = pedido['codccusto']

            localentg = row['dslocalentrega']
            id_cadped += 1
            aditamento = None

            CUR_FDB.execute(insert_cabecalho, (numped, num, ano, codif, datped
                                            , codccusto, entrou, numlic
                                            , proclic, localentg, condpgto, prozoentrega
                                            , obs, id_cadped, EMPRESA, aditamento
                                            , contrato, npedlicit, id_cadpedlicit))
            
        codant = row['codant']        
        item_info = itens_cadpro.get(codant, 0)

        if item_info == 0:
            if row['lote'] > 9 or row['nritem'] > 99:
                item = str(row['lote'])  + '0' + str(row['nritem'])
            cadpro = dict_produtos[str(row['cdmaterial'])]
            codccusto = 0
            marca = None
        else:
            item = item_info[1]
            cadpro = item_info[2]
            codccusto = item_info[0]
            marca = item_info[3]

        qtd = row['qtitem']
        prcunt = row['valor_unitario']
        prctot = row['qtitem']*row['valor_unitario']
        ficha = None
        desdobro = None

        try:
            CUR_FDB.execute(insert_itens, (numped, id_cadped, item, cadpro, codccusto, qtd, prcunt, prctot, ficha, desdobro, marca))
        except:
            print(f'Erro ao inserir item {item} do subpedido {numped}')
    commit()