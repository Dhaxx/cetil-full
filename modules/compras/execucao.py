from connection import commit, CUR_FDB, fetchallmap, ENTIDADE, EXERCICIO, CUR_SQLS
from utils import limpa_tabela, EMPRESA, dict_produtos, dict_fornecedores, cria_campo, obter_codif_por_nome

def pedidos():
    limpa_tabela('icadped', 'cadped')
    cria_campo("cadped","conv_empenho")
    cria_campo("cadped","conv_pkemp")
    cria_campo("cadped","conv_empenho_ano")
    cria_campo("icadped","codant")

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
                , item_licit
                , cadpro_licit
                , codant
            )
        values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    
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

    CUR_SQLS.execute(f"""
    SELECT
        SUM(vlcotacaoproposta) AS vlcotacaoproposta,
        SUM(vltotalitem) AS vltotalitem,
        SUM(qtdeitem) AS qtdeitem,
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
        coalesce(cast(cdmaterial as varchar), CASE WHEN FK_TABELA_PRECO IS NOT NULL THEN CONCAT('TBL', CAST(fk_tabela_preco AS VARCHAR)) ELSE CAST(cdmaterial AS VARCHAR) END) cdmaterial,
        ID_ProcessoLicitatorio,
        CONCAT(ID_ProcessoLicitatorio, '-', nritem, '-', 
            CASE 
                WHEN FK_TABELA_PRECO IS NOT NULL THEN CONCAT('TBL', CAST(fk_tabela_preco AS VARCHAR))
                ELSE CAST(cdmaterial AS VARCHAR) 
            END, '-', lote
        ) AS codant,
        preco_por_tabela
    FROM
        (
        SELECT
            CASE
                WHEN e.ehregistropreco = '0' THEN 'N'
                ELSE 'S'
            END AS registro_preco,
            FK_TABELA_PRECO,
            0 AS cod_registro,
            e.dtanoprocesso,
            e.nrprocesso,
            e.cdfornecedor,
            e.nrempenho,
            e.dtanoempenho,
            l.dsobjeto,
            e.nritem,
            coalesce(nr_lote, lote) lote,
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
            CAST(cp.dscondicaopagamento AS VARCHAR) AS dscondicaopagamento,
            o.nmorgao,
            z.nmfornecedor,
            TRIM(CAST(CAST(z.nrcgccpf AS DECIMAL(14, 0)) AS CHAR(14))) AS nrcgccpf,
            CASE
                WHEN EXISTS (
                SELECT
                    1
                FROM
                    {ENTIDADE}_COMPRAS.dbo.proposta b
                WHERE
                    e.dtanoprocesso = b.dtanoprocesso
                    AND e.nrprocesso = b.nrprocesso
                    AND e.lote <> b.lote
                ) THEN 'S'
                ELSE 'N'
            END AS maisdeumlote,
            COALESCE (
                (
            SELECT
                TOP 1 i.cdmaterial
            FROM
                {ENTIDADE}_ALMOX.dbo.INTEGRAINSTRUMENTOPEDIDO i
            WHERE
                i.nrprocesso = e.nrprocesso
                AND i.dtanoprocesso = e.dtanoprocesso
                AND i.cdtipoprocesso = e.cdtipoprocesso
                AND i.lote = e.lote
                AND i.nritem = e.nritem),
            (
            SELECT
                TOP 1 i.cdmaterial
            FROM
                {ENTIDADE}_COMPRAS.dbo.ITEMOBJETO i
            WHERE
                i.nrprocesso = e.nrprocesso
                AND i.dtanoprocesso = e.dtanoprocesso
                AND i.cdtipoprocesso = e.cdtipoprocesso
                AND i.lote = e.lote
                AND i.nritem = e.nritem)
            ) AS cdmaterial,
            ID_ProcessoLicitatorio,
            case when t.fk_tabela_preco is not null then 'S' else 'N' end preco_por_tabela
        FROM
            {ENTIDADE}_COMPRAS.dbo.dataview_pedidoempenho e
        INNER JOIN {ENTIDADE}_COMPRAS.dbo.processolicitatorio l 
            ON
            e.dtanoprocesso = l.dtanoprocesso
            AND e.nrprocesso = l.nrprocesso
            AND e.cdtipoprocesso = l.cdtipoprocesso
        LEFT JOIN {ENTIDADE}_COMPRAS.dbo.condicaopagamento cp 
            ON
            cp.cdcondicaopagamento = l.cdcondicaopagamento
        LEFT JOIN {ENTIDADE}_COMPRAS.dbo.orgao o 
            ON
            e.cdorgaoreduzido = o.cdorgaoreduzido
            AND e.dtanoempenho = o.dtano
        INNER JOIN {ENTIDADE}_ALMOX.dbo.fornecedor z 
            ON
            z.cdfornecedor = e.cdfornecedor
        LEFT JOIN (
            SELECT
                pkid,
                fk_tabela_preco,
                FK_PROCESSO_LICITATORIO,
                nr_lote
            FROM
                {ENTIDADE}_COMPRAS.dbo.TABELA_PRECO_PROCESSO
        ) t 
        ON
            t.FK_PROCESSO_LICITATORIO = l.pkid
        WHERE e.dtanoempenho >= {int(EXERCICIO)-5}
    ) AS p
    GROUP BY
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
        FK_TABELA_PRECO,
        preco_por_tabela
    ORDER BY
        dtanoempenho,
        nrempenho""")

    id_cadped = 0
    try:
        id_cadped = CUR_FDB.execute('select coalesce(id,0) from pk where tabela = cadped').fetchone()[0]
    except:
        pass

    itens_cadpro = {codant: (codccusto, item, cadpro, codif) for codant, codccusto, item, codif, cadpro in CUR_FDB.execute("SELECT CODANT, CODCCUSTO, item, codif, cadpro FROM cadpro co where subem = 1")}
    exercicio = CUR_FDB.execute('SELECT mexer FROM CADCLI c ').fetchone()[0]
    item = 0

    for row in CUR_SQLS:
        ficha = None
        if row.dtanoempenho == int(exercicio):
            ficha = next((x['fichaprincipal'] for x in fichas if x['fichasecundaria'] == row.cddespesa), None)

        nroPedido = f'{row.nrempenho:05}' + '/' + str(row.dtanoempenho)[2:4]

        if nroPedido != pedido_atual:
            id_cadped += 1
            pedido_atual = nroPedido
            numped = nroPedido
            num = f'{row.nrempenho:05}'
            ano = row.dtanoempenho
            item = 0
            
            insmf = row.nrcgccpf

            if (len(insmf) > 11 and  len(insmf) < 14):
                insmf = insmf.zfill(14)
            elif len(insmf) < 11:
                insmf = insmf.zfill(11)

            codif = dict_fornecedores.get(insmf, 0)
            if codif == 0:
                codif = obter_codif_por_nome(row.nmfornecedor)
            else:
                codif = codif['codif']

            datped = row.dtempenho
            entrou = 'N'
            numlic = row.ID_ProcessoLicitatorio
            proclic = f'{row.nrprocesso:06}' '/' + str(row.dtanoprocesso)[2:4]
            localentg = None
            condpgto = row.dscondicaopagamento
            prozoentrega = None
            obs = row.dsobjeto
            aditamento = None
            contrato = None
            conv_empenho = row.nrempenhocp
            conv_empenho_ano = row.dtanoempenhocp
            codccusto = 0

            if row.nrinstrumentocontratual is not None:
                contrato = f'{row.nrinstrumentocontratual}/{str(row.dtanoinstrumentocontratual)[2:]}'
            npedlicit = None
            id_cadpedlicit = None

            try:
                CUR_FDB.execute(insert_cabecalho, (numped, num, ano, codif, datped
                                            , ficha, codccusto, entrou, numlic
                                            , proclic, localentg, condpgto, prozoentrega
                                            , obs, id_cadped, EMPRESA, aditamento
                                            , contrato, npedlicit, id_cadpedlicit, conv_empenho, conv_empenho_ano))
            except Exception as e:
                print(f'Erro ao inserir pedido {numped}')
        
        codant = row.codant        
        item_info = itens_cadpro.get(codant, 0)
        item_licit = None
        cadpro_licit = None
        cadpro = dict_produtos[str(row.cdmaterial)]
        item += 1

        try:
            if item_info == 0:
                codccusto = row.cdorgaoreduzido
                marca = None
                if row.preco_por_tabela == 'S':
                    item_licit = row.nritem
                    cadpro_licit = item_info[2]
            else:
                cadpro = item_info[2]
                codccusto = item_info[0]
                marca = item_info[3]
                if row.preco_por_tabela == 'S':
                    item_licit = item_info[1]
                    cadpro_licit = item_info[2]
            
            qtd = row.qtdeitem
            prcunt = row.vlcotacaoproposta
            prctot = row.vltotalitem
            desdobro = None

            CUR_FDB.execute(insert_itens, (numped, id_cadped, item, cadpro, codccusto,
                                        qtd, prcunt, prctot,ficha,desdobro, marca, item_licit, cadpro_licit, codant))
        except Exception as e:
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

    # CUR_FDB.execute("""
    # EXECUTE BLOCK 
    # AS 
    # DECLARE VARIABLE ID_CADPED INTEGER;
    # DECLARE VARIABLE PKEMP INTEGER;
    # DECLARE VARIABLE cadpro_tabela VARCHAR(50);
    # DECLARE VARIABLE codccusto INTEGER;
    # BEGIN
    #     FOR 
    #         SELECT b.ID_CADPED, a.pkemp 
    #         FROM despesitem a 
    #         JOIN despes b ON a.pkemp = b.pkemp 
    #         WHERE EXISTS (SELECT 1 FROM icadped c WHERE c.cadpro LIKE '999%' AND c.id_cadped = b.id_cadped) 
    #         INTO :id_cadped, :pkemp
    #     DO
    #     BEGIN
    #         -- Buscar os valores máximos de CADPRO e CODCCUSTO para o ID_CADPED
    #         SELECT MAX(CADPRO), MAX(codccusto) 
    #         FROM icadped 
    #         WHERE id_cadped = :id_cadped 
    #         INTO :cadpro_tabela, :codccusto;

    #         -- Remover os registros antigos
    #         DELETE FROM icadped WHERE id_cadped = :id_cadped;

    #         -- Inserir os novos registros
    #         INSERT INTO icadped (numped, id_cadped, item, cadpro, codccusto, qtd, prcunt, prctot)
    #         SELECT 0, :id_cadped, item, :cadpro_tabela, :codccusto, vltotal/vlunit, vlunit, vltotal
    #         FROM despesitem 
    #         WHERE pkemp = :pkemp and vlunit <> 0;
    #     END
    #     UPDATE ICADPED A SET A.NUMPED = (SELECT B.NUMPED FROM CADPED B WHERE B.ID_CADPED = A.ID_CADPED) where A.numped = '0';
    # END
    # """)
    # commit()

    CUR_FDB.execute("ALTER TRIGGER TAUP_ICADPED INACTIVE")
    commit()

    CUR_FDB.execute("UPDATE icadped SET item = item * 1000 where cadpro not starting '999.'")
    commit()

    CUR_FDB.execute("""MERGE INTO icadped a USING (SELECT max(item) item, codant FROM cadpro WHERE cadpro NOT STARTING '999.' AND subem = 1 GROUP BY 2) b
        ON a.codant = b.codant AND a.item <> b.item
        WHEN MATCHED THEN UPDATE SET a.item = b.item""")
    commit()


def autorizacao():
    CUR_SQLS.execute(f"""
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
        CONCAT(ID_ProcessoLicitatorio, '-', nritem, '-', 
                CASE 
                    WHEN t.FK_TABELA_PRECO IS NOT NULL THEN CONCAT('TBL', CAST(t.fk_tabela_preco AS VARCHAR))
                    ELSE CAST(cdmaterial AS VARCHAR) 
                END, '-', coalesce(nr_lote, lote)
            ) AS codant,
            e.CD_ORGAO_REDUZIDO
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
    LEFT JOIN (
        SELECT
            pkid,
            fk_tabela_preco,
            FK_PROCESSO_LICITATORIO,
            nr_lote
        FROM
            {ENTIDADE}_COMPRAS.dbo.TABELA_PRECO_PROCESSO
            ) t 
            ON
                t.FK_PROCESSO_LICITATORIO = l.ID_PROCESSOLICITATORIO
        and t.nr_lote = i.Lote
    where
        e.nr_empenho <> 0
        and dt_ano_empenho >= {int(EXERCICIO)-5}
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
            , item_licit
            , cadpro_licit
        )
    values (?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    pedido_atual = ""
    itens_cadpro = {codant: (codccusto, item, cadpro, codif) for codant, codccusto, item, codif, cadpro in CUR_FDB.execute("SELECT cc.CODANT, co.CODCCUSTO, co.item, co.codif, co.cadpro FROM cadpro co JOIN cadprolic cc ON co.item = cc.item AND co.numlic = cc.numlic")}
    
    for row in CUR_SQLS:
        numero_composto = f'{row.nrinstrumentocontratual}_{row.dtanoinstrumentocontratual}_{row.tpinstrumentocontratual}_{row.nr_empenho}'
        if numero_composto != pedido_atual:
            item = 0
            pedido_atual = numero_composto
            numero += 1
            num = f'{numero:05}'
            numped = f'{num}/{str(row.dtanoprocesso)[2:]}'
            ano = row.dtanoprocesso

            npedlicit = f'{row.nr_empenho:05}/{str(row.dt_ano_empenho)[2:]}'
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
            datped = row.dtinstrumentocontratual
            entrou = 'N'
            codccusto = pedido['codccusto']

            localentg = row.dslocalentrega
            id_cadped += 1
            aditamento = None

            CUR_FDB.execute(insert_cabecalho, (numped, num, ano, codif, datped
                                            , codccusto, entrou, numlic
                                            , proclic, localentg, condpgto, prozoentrega
                                            , obs, id_cadped, EMPRESA, aditamento
                                            , contrato, npedlicit, id_cadpedlicit))
            
        try:
            item += 1
            codant = row.codant        
            item_info = itens_cadpro.get(codant, 0)
            cadpro = dict_produtos[str(row.cdmaterial)]
            ehtabela = codant.split('-')[2]

            if 'TBL' in ehtabela and item_info != 0:
                item_licit = item_info[1]
                cadpro_licit = item_info[2]
            else: 
                item_licit = None
                cadpro_licit = None

            qtd = row.qtitem
            prcunt = row.valor_unitario
            prctot = row.qtitem*row.valor_unitario
            ficha = None
            desdobro = None


            CUR_FDB.execute(insert_itens, (numped, id_cadped, item, cadpro, codccusto, qtd, prcunt, prctot, ficha, desdobro, item_licit, cadpro_licit))
        except:
            print(f'Erro ao inserir item {item} do subpedido {numped}')
    commit()