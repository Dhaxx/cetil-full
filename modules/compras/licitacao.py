from connection import commit, CUR_FDB, fetchallmap, ENTIDADE
from utils import limpa_tabela, EMPRESA, dict_produtos, dict_fornecedores, obter_codif_por_nome, cria_campo

def cadlic():
    limpa_tabela('cadlic')

    insert = CUR_FDB.prep("""
    insert
        into
        cadlic (
            empresa
            , numlic
            , proclic
            , numero
            , ano
            , comp
            , licnova
            , liberacompra                
            , discr
            , detalhe
            , registropreco
            , microempresa
            , numpro
            , discr7                
            , datae
            , processo_data
            , horabe
            , horreal
            , tipopubl
            , dtadj
            , dtenc
            , dthom                
            , local
            , modlic
            , licit
            , anomod
            , codmod
            , codtce
        )
    values (?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?)
    """)

    modalidades = {
        '100': {"modalidade": "DISPENSA ELETRÔNICA", "sigla": "DE01", "indice": 11}, #Dispensa Eletrônica
        '520': {"modalidade": "DISPENSA", "sigla": "DIS1", "indice": 1}, # Chamamento Público
        '410': {"modalidade": "CONCORRÊNCIA", "sigla": "CE01", "indice": 4}, # Concorrência
        '430': {"modalidade": "CONVITE", "sigla": "CONV", "indice": 2}, # Convite
        '450': {"modalidade": "LEILÃO", "sigla": "LEIL", "indice": 6}, # Credenciamento
        '480': {"modalidade": "PREGÃO", "sigla": "PP01", "indice": 8}, # Pregão
        '48-1': {"modalidade": "PREGÃO ELETRÔNICO", "sigla": "PE01", "indice": 9}, # P
        '420': {"modalidade": "TOMADA DE PREÇOS", "sigla": "TOM3", "indice": 3}, # Tomada de preços,
    }

    rows = fetchallmap(f"""
    select 
		Concat(nrProcesso, replace(Concat(cdTipoProcesso,cdtipoModalidade,inPregaoEletronico),'-',''), dtAnoProcesso%2000) numlic,
		Concat(format(nrProcesso, '000000'),'/',format(dtAnoProcesso%2000, '00')) proclic,
		format(nrProcesso, '000000') numero,
		dtanoprocesso ano,
		comp = 3,
		licnova = 1,
		liberacompra = 'S',
		discr = daObjeto,
		detalhe = dsObjeto,
		case when inRegistroPreco = -1 then 'S' else 'N' end registropreco,
		microempresa = 0,
		numpro = nrModalidade,
		CASE 
		    WHEN CAST(dsCriterio AS VARCHAR(MAX)) = 'Menor preço - Global' THEN 'Menor Preco Global' 
		    ELSE 'Menor Preco Unitario' 
		END AS discr7,
		dtExpedicao datae,
		format(coalesce(dtexpedicao, '00:00'), 'HH:mm') horabe,
		dtHomologacao,
		Concat(cdTipoProcesso,cdtipoModalidade,inPregaoEletronico) tipo_processo,
		dacondicaopagamento
    from
        {ENTIDADE}_COMPRAS.dbo.processolicitatorio
    left join {ENTIDADE}_COMPRAS.dbo.condicaopagamento on
        condicaopagamento.cdcondicaopagamento = processolicitatorio.cdcondicaopagamento
    order by
        dtanoprocesso,
        nrprocesso
    """)

    for row in rows:
        empresa = EMPRESA
        numlic = row['numlic']
        proclic = row['proclic']
        numero = row['numero']
        ano = row['ano']
        comp = row['comp']
        licnova = row['licnova']
        liberacompra = row['liberacompra']
        discr = row['discr']
        detalhe = row['detalhe']
        registropreco = row['registropreco']
        microempresa = row['microempresa']
        numpro = row['numpro']
        discr7 = row['discr7']
        datae = row['datae']
        processo_data = row['datae']
        horabe = row['horabe']
        horreal = row['horabe']
        tipopubl='Outros'
        dtadj = row['dtHomologacao']
        dtenc = row['dtHomologacao']
        dthom = row['dtHomologacao']
        local = None
        
        modalidade = modalidades.get(row['tipo_processo'], modalidades['520'])
        
        modlic = modalidade['sigla']
        licit = modalidade['modalidade']
        anomod = row['ano']
        codmod = modalidade['indice']
        codtce = None
        
        CUR_FDB.execute(insert, (empresa, numlic, proclic, numero, ano,
                  comp, licnova, liberacompra, discr[:1024], detalhe,
                  registropreco, microempresa, numpro, discr7, datae,
                  processo_data, horabe, horreal, tipopubl, dtadj, dtenc, dthom,
                  local, modlic, licit, anomod, codmod, codtce))
    commit()

    CUR_FDB.execute("""
    INSERT INTO CADLIC_SESSAO (NUMLIC, SESSAO, DTREAL, HORREAL, COMP, DTENC, HORENC, SESSAOPARA, MOTIVO) 
	SELECT L.NUMLIC, CAST(1 AS INTEGER), L.DTREAL, L.HORREAL, L.COMP, L.DTENC, L.HORENC, CAST('T' AS VARCHAR(1)), CAST('O' AS VARCHAR(1)) FROM CADLIC L 
	WHERE numlic not in (SELECT FIRST 1 S.NUMLIC FROM CADLIC_SESSAO S WHERE S.NUMLIC = L.NUMLIC)
    """)
    commit()

def cadlotelic():
    limpa_tabela('cadlotelic')

    rows = fetchallmap(f"""
    SELECT
        DISTINCT 
        CONCAT(a.nrProcesso, 
            REPLACE(CONCAT(p.cdTipoProcesso, p.cdtipoModalidade, p.inPregaoEletronico), '-', ''), 
            p.dtAnoProcesso % 2000) AS identificador,
        lote,
        nmlote,
        p.cdTipoProcesso
        -- Adicionado para evitar erro no ORDER BY
    FROM
        {ENTIDADE}_COMPRAS.dbo.itemobjeto a
    JOIN {ENTIDADE}_COMPRAS.dbo.processolicitatorio p 
        ON
        p.dtanoprocesso = a.dtanoprocesso
        AND p.nrprocesso = a.nrprocesso
        AND p.cdtipoprocesso = a.cdtipoprocesso
    WHERE
        lote > 0
    GROUP BY
        a.dtanoprocesso,
        a.nrprocesso,
        lote,
        nmlote,
        p.cdTipoProcesso,
        p.cdtipoModalidade,
        p.inPregaoEletronico,
        p.dtAnoProcesso
    ORDER BY
        p.cdTipoProcesso;
    """)

    insert = CUR_FDB.prep("insert into cadlotelic(numlic,lotelic,descr,reduz,microempresa,tlance) values(?,?,?,?,?,?)")

    for row in rows:
        numlic = row['identificador']
        lotelic = f'{row['lote']:08}'
        descr = row['nmlote']
        reduz = 'N'
        microempresa = 'N'
        tlance = '$'

        try:
            CUR_FDB.execute(insert, (numlic, lotelic, descr, reduz, microempresa, tlance))
        except:
            continue
    commit()

def prolic_prolics():
    limpa_tabela('prolics', 'prolic')

    insert_prolics = CUR_FDB.prep('insert into prolics(sessao,numlic,codif, habilitado, status, cpf,representante) values (?,?,?,?,?,?,?)')
    insert_prolic = CUR_FDB.prep('insert into prolic(numlic, codif, nome, status) values (?,?,?,?)')

    rows = fetchallmap(f'''
    SELECT DISTINCT  
        CONCAT(a.nrProcesso, 
            REPLACE(CONCAT(p.cdTipoProcesso, p.cdtipoModalidade, p.inPregaoEletronico), '-', ''), 
            p.dtAnoProcesso % 2000) AS identificador,
        a.nrprocesso,  -- Adicionado para evitar erro no ORDER BY
        p.cdTipoProcesso,  -- Adicionado
        p.cdtipoModalidade,  -- Adicionado
        p.inPregaoEletronico,  -- Adicionado
        a.cdfornecedor,
        fornecedor.nmfornecedor,
        CASE
            WHEN (l.cdtipoprocesso = 4 AND l.cdtipomodalidade IN (5, 8)) THEN 'S'
            ELSE NULL
        END AS lance,
        trim(CAST(CAST(fornecedor.nrcgccpf AS DECIMAL(14,0)) AS CHAR(14))) insmf
    FROM {ENTIDADE}_COMPRAS.dbo.proposta a
    JOIN {ENTIDADE}_COMPRAS.dbo.processolicitatorio p 
        ON p.dtanoprocesso = a.dtanoprocesso
        AND p.nrprocesso = a.nrprocesso
        AND p.cdtipoprocesso = a.cdtipoprocesso
    INNER JOIN {ENTIDADE}_COMPRAS.dbo.processolicitatorio l 
        ON a.dtanoprocesso = l.dtanoprocesso
        AND a.nrprocesso = l.nrprocesso
        AND a.cdtipoprocesso = l.cdtipoprocesso
    INNER JOIN {ENTIDADE}_ALMOX.dbo.fornecedor 
        ON a.cdfornecedor = fornecedor.cdfornecedor
    ORDER BY
        a.nrprocesso,
        p.cdTipoProcesso,
        p.cdtipoModalidade,
        p.inPregaoEletronico;
    ''')

    for row in rows:
        sessao = 1
        numlic = row['identificador']
        insmf = row['insmf']

        if (len(insmf) > 11 and  len(insmf) < 14):
            insmf = insmf.zfill(14)
        elif len(insmf) < 11:
            insmf = insmf.zfill(11)

        if insmf in dict_fornecedores:
            codif = dict_fornecedores[insmf]['codif']
        else:
            codif = obter_codif_por_nome(row['nmfornecedor'])
        habilitado = 'S'
        status = 'S'
        cpf = row['insmf']
        nome = row['nmfornecedor'][:40].title()
        representante = None

        CUR_FDB.execute(insert_prolics,(sessao,numlic,codif, habilitado, status, cpf, representante))
        CUR_FDB.execute(insert_prolic,(numlic, codif, nome, status))
    commit()

def cadprolic():
    limpa_tabela('cadprolic_detalhe')
    limpa_tabela('cadprolic')
    cria_campo('cadprolic', 'codant')

    rows = fetchallmap(f"""
    SELECT
        coalesce(b.cdOrgaoReduzido,
        0) cdOrgaoReduzido,
        a.dtanoprocesso,
        a.nrprocesso,
        a.nritem,
        a.lote,
        a.cdmaterial,
        a.vlcotacaoitem,
        SUM(a.qtitemobjeto) AS qtitemobjeto,
        CONCAT(a.nrProcesso, 
            REPLACE(CONCAT(p.cdTipoProcesso, p.cdtipoModalidade, p.inPregaoEletronico), '-', ''), 
            p.dtAnoProcesso % 2000) AS identificador,
        CASE
            WHEN EXISTS (
            SELECT
                1
            FROM
                {ENTIDADE}_COMPRAS.dbo.itemobjeto b
            WHERE
                a.dtanoprocesso = b.dtanoprocesso
                AND a.nrprocesso = b.nrprocesso
                AND b.lote <> a.lote
            ) THEN 'S'
            ELSE 'N'
        END AS maisdeumlote,
        (
        SELECT
            COUNT(*)
        FROM
            {ENTIDADE}_COMPRAS.dbo.itemobjeto d
        WHERE
            d.nrprocesso = a.nrprocesso
            AND d.dtanoprocesso = a.dtanoprocesso
            AND d.lote = a.lote
            AND d.nritem = a.nritem
            AND a.cdmaterial = d.cdmaterial
            AND d.vlcotacaoitem <> a.vlcotacaoitem
        ) AS duplicado,
        -- Nova coluna nritem com ROW_NUMBER()
        ROW_NUMBER() OVER (
            PARTITION BY CONCAT(b.nrprocesso, SUBSTRING(CAST(b.dtanoprocesso AS VARCHAR), 3, 2))
    ORDER BY
        b.nritem,
        b.lote,
        b.cdmaterial
        ) AS nritem_novo,
        -- Nova coluna codant
        CONCAT(
            coalesce(CONCAT(b.nrprocesso, SUBSTRING(CAST(b.dtanoprocesso AS VARCHAR), 3, 2)), 0), 
            '-', coalesce(a.nritem, 0),
            '-', coalesce(a.lote, 0), 
            '-', coalesce(b.cdOrgaoReduzido, 0)
        ) AS codant
    FROM
        {ENTIDADE}_COMPRAS.dbo.itemobjeto a
    JOIN {ENTIDADE}_COMPRAS.dbo.processolicitatorio p 
        ON
        p.dtanoprocesso = a.dtanoprocesso
        AND p.nrprocesso = a.nrprocesso
        AND p.cdtipoprocesso = a.cdtipoprocesso
    LEFT JOIN (
        SELECT
            DISTINCT
            RIGHT(REPLICATE('0',
            5) + CAST(nrpesquisa AS VARCHAR),
            5) + '/' + 
            CAST(dtanopesquisa % 2000 AS VARCHAR) AS numorc,
            qr.nritem,
            qr.lote,
            qr.cdmaterial,
            qr.cdOrgaoReduzido,
            qr.nrprocesso,
            qr.dtanoprocesso,
            qr.cdTipoProcesso
        FROM
            (
            SELECT
                p.nrpesquisa,
                p.dtanopesquisa,
                COALESCE(p.nrprocesso,
                0) AS nrprocesso,
                p.dtanoprocesso,
                p.dspesquisa,
                p.dtexpedicao,
                p.justificativa_contratacao,
                i.lote,
                i.nritem,
                i.cdmaterial,
                i.qtitempesquisa,
                f.dscondicaopagamento,
                COALESCE(l.dslocalentrega + ' - Compl.: ' + l.dscomplemento,
                '') AS local,
                (
                SELECT
                    MAX(lote)
                FROM
                    {ENTIDADE}_COMPRAS.dbo.itempesquisa c
                WHERE
                    c.dtanopesquisa = p.dtanopesquisa
                    AND c.nrpesquisa = p.nrpesquisa
                ) AS qtdlote,
                o.cdOrgaoReduzido,
                p.cdTipoProcesso
            FROM
                {ENTIDADE}_COMPRAS.dbo.processopesquisa p
            INNER JOIN {ENTIDADE}_COMPRAS.dbo.itempesquisa i 
                ON
                i.dtanopesquisa = p.dtanopesquisa
                AND i.nrpesquisa = p.nrpesquisa
            LEFT JOIN {ENTIDADE}_COMPRAS.dbo.condicaopagamento f 
                ON
                f.cdcondicaopagamento = p.cdcondicaopagamento
            LEFT JOIN {ENTIDADE}_COMPRAS.dbo.localentrega l 
                ON
                l.cdlocalentrega = p.cdlocalentrega
            LEFT JOIN {ENTIDADE}_COMPRAS.dbo.orgaopesquisa o 
                ON
                o.dtAnoPesquisa = p.dtAnoPesquisa
                AND o.nrPesquisa = p.nrPesquisa
        ) AS qr
    ) b 
    ON
        a.nrprocesso = b.nrprocesso
        AND a.dtAnoProcesso = b.dtanoprocesso
        AND a.nritem = b.nritem
        AND a.Lote = b.lote
        AND a.cdMaterial = b.cdmaterial
        AND a.cdTipoProcesso = b.cdTipoProcesso
    WHERE
        a.nritem <> 0
    GROUP BY
        a.dtanoprocesso,
        a.nrprocesso,
        a.lote,
        a.nritem,
        a.cdmaterial,
        a.vlcotacaoitem,
        b.cdOrgaoReduzido,
        p.cdTipoProcesso,
        p.cdtipoModalidade,
        p.inPregaoEletronico,
        p.dtAnoProcesso,
        b.nrprocesso,
        -- Adicionado para a partição do ROW_NUMBER
        b.dtanoprocesso,
        b.nritem,
        b.lote,
        b.cdmaterial,
        b.cdOrgaoReduzido
    HAVING
        SUM(qtitemobjeto) > 0
    ORDER BY
        a.dtanoprocesso,
        a.nrprocesso,
        a.lote,
        a.nritem,
        a.vlcotacaoitem DESC;
    """)

    insert_itens = CUR_FDB.prep("""
    insert
        into
        cadprolic(
            item
            , item_mask
            , itemorc
            , cadpro
            , quan1
            , vamed1
            , vatomed1
            , codccusto
            , ficha
            , reduz
            , ordnumorc
            , numlic
            , id_cadorc
            , lotelic
            , item_lote
            , codant
        )
    values (?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?)
    """)

    insert_detalhe = CUR_FDB.prep(
    """
    insert
        into
        cadprolic_detalhe (
            numlic
            , item
            , ordnumorc
            , numorc
            , itemorc
            , cadpro
            , quan1
            , vamed1
            , vatomed1
            , codccusto
            , ficha
            , item_cadprolic
            , id_cadorc
        )
    values (?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?)""")
    
    insert_cadpro_status = CUR_FDB.prep(
        """
        insert
            into
            cadpro_status (
                sessao
                , itemp
                , telafinal
                , aceito
                , lotelic
                , numlic
            )
        values (?,?,?,?,?,?)
        """)
    
    for row in rows:
        if row['vlcotacaoitem'] == 0 and row['duplicado'] == 1:
            continue
        
        item = row['nritem_novo']

        item_mask = item
        itemorc = None
        cadpro = dict_produtos[str(row['cdmaterial'])]
        quan1 = row['qtitemobjeto']
        vamed1 = row['vlcotacaoitem']
        vatomed1 = row['vlcotacaoitem']*row['qtitemobjeto']
        codccusto = row['cdOrgaoReduzido']
        ficha = None
        reduz = 'N'
        ordnumorc = None
        numlic = row['identificador']
        id_cadorc = None
        lotelic = f'{row['lote']:08}' if row['lote'] > 0 else None
        item_lote = item
        numorc = None
        item_cadprolic = item

        sessao = 1
        itemp = item
        telafinal = 'I_ENCERRAMENTO'
        aceito = 'S'
        codant = row['codant']

        CUR_FDB.execute(insert_itens,(item , item_mask , itemorc , cadpro , quan1
                , vamed1 , vatomed1 , codccusto , ficha , reduz , ordnumorc
                , numlic , id_cadorc , lotelic , item_lote, codant))
        
        CUR_FDB.execute(insert_detalhe,(numlic , item , ordnumorc , numorc , itemorc
                , cadpro , quan1 , vamed1 , vatomed1 , codccusto , ficha
                , item_cadprolic , id_cadorc))
        
        CUR_FDB.execute(insert_cadpro_status,(sessao , itemp , telafinal , aceito
                , lotelic , numlic))
    commit()

def proposta():
    limpa_tabela('cadpro_proposta')
    insert_cadpro_proposta = CUR_FDB.prep("""
    insert
        into
        cadpro_proposta (
            sessao
            , codif
            , item
            , itemp
            , quan1
            , vaun1
            , vato1
            , numlic
            , status
            , subem
            , marca
            , itemlance                
            , lotelic
        )
    values (?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?)""")
    
    insert_cadpro_final = CUR_FDB.prep("""
    insert
        into
        cadpro_final(
            numlic
            , ult_sessao
            , codif
            , itemp
            , vaunf
            , vatof
            , status
            , subem
        )
    values (?,?,?,?,?,
            ?,?,?)""")
    
    insert_cadpro = CUR_FDB.prep("""
    insert
    into
    cadpro(
        codif
        , cadpro
        , quan1
        , vaun1
        , vato1
        , subem
        , status
        , item
        , codccusto
        , numlic
        , ult_sessao
        , itemp
        , qtdadt
        , vaunadt
        , vatoadt
        , tpcontrole_saldo
    ) values(?,?,?,?,?,?,?,
    ?,?,?,?,?,?,?,?,?)""")
    
    rows = fetchallmap(f"""
    select
        a.dtanoprocesso,
        a.nrprocesso,
        a.cdfornecedor,
        fornecedor.nmfornecedor,
        a.invencedor,
        trim(CAST(CAST(fornecedor.nrcgccpf AS DECIMAL(14, 0)) AS CHAR(14))) insmf,
        CONCAT(a.nrProcesso, 
            REPLACE(CONCAT(p.cdTipoProcesso, p.cdtipoModalidade, p.inPregaoEletronico), '-', ''), 
            p.dtAnoProcesso % 2000) AS identificador,
        i.cdmaterial,
        case
            when exists(
            select
                1
            from
                {ENTIDADE}_COMPRAS.dbo.itemobjeto b
            where
                a.dtanoprocesso = b.dtanoprocesso
                and a.nrprocesso = b.nrprocesso
                and b.lote <> a.lote) 
                        then 'S'
            else 'N'
        end as maisdeumlote,
        CONCAT(
                coalesce(CONCAT(a.nrprocesso, SUBSTRING(CAST(i.dtanoprocesso AS VARCHAR), 3, 2)), 0), 
                '-', coalesce(i.nritem, 0),
                '-', coalesce(a.lote, 0), 
                '-', coalesce(o.cdOrgaoReduzido, 0)
            ) AS codant,
        a.lote,
        a.qtitemproposta,
        a.vlcotacaoproposta,
        coalesce(
                    (
        select
            top 1 cp.vlcotacaoproposta
        from
            {ENTIDADE}_COMPRAS.dbo.CLASSIFICACAOPROPOSTA cp
        where
            a.dtanoprocesso = cp.dtanoprocesso
            and a.cdtipoprocesso = cp.cdtipoprocesso
            and a.nrprocesso = cp.nrprocesso
            and a.lote = cp.lote
            and a.nritemproposta = cp.nritem
            and i.cdmaterial = cp.cdmaterial
            and a.cdfornecedor = cp.cdfornecedor
            and cp.tpregistro = 'R'
        order by
            ordem desc),
        a.vlcotacaoproposta) valoraditado,
        coalesce(o.cdOrgaoReduzido, 0) cdOrgaoReduzido
    from
        {ENTIDADE}_COMPRAS.dbo.proposta a
    join {ENTIDADE}_COMPRAS.dbo.processolicitatorio p on
        p.dtanoprocesso = a.dtanoprocesso
        and p.nrprocesso = a.nrprocesso
        and p.cdtipoprocesso = a.cdtipoprocesso
    inner join {ENTIDADE}_COMPRAS.dbo.itemobjeto i on
        a.dtanoprocesso = i.dtanoprocesso
        and a.cdtipoprocesso = i.cdtipoprocesso
        and a.nrprocesso = i.nrprocesso
        and a.lote = i.lote
        and a.nritemproposta = i.nritem
    inner join {ENTIDADE}_ALMOX.dbo.fornecedor on
        a.cdfornecedor = fornecedor.cdfornecedor
    left join (
        select
            dtAnoProcesso,
            cdTipoProcesso,
            nrProcesso,
            cdOrgaoReduzido
        from
            {ENTIDADE}_COMPRAS.dbo.ORGAOPROCESSO) o on
            o.dtAnoProcesso = a.dtAnoProcesso
        and
            o.cdTipoProcesso = a.cdTipoProcesso
        and
            o.nrProcesso = a.nrProcesso
    """)

    itens_processo = {codant : item for codant, item in CUR_FDB.execute('select codant, item from cadprolic').fetchall()}

    for row in rows:
        sessao = 1
        insmf = row['insmf']

        if (len(insmf) > 11 and  len(insmf) < 14):
            insmf = insmf.zfill(14)
        elif len(insmf) < 11:
            insmf = insmf.zfill(11)

        if insmf in dict_fornecedores:
            codif = dict_fornecedores[insmf]['codif']
        else:
            codif = obter_codif_por_nome(row['nmfornecedor'])
        
        item = itens_processo[row['codant']]
        itemp = item
        quan1 = row['qtitemproposta']
        vaun1 = row['vlcotacaoproposta']
        vato1 = quan1*vaun1
        numlic = row['identificador']
        status = 'C'
        subem = 1 if row['invencedor'] == 'S' else 0
        marca = None
        itemlance = 'S'

        lotelic = f'{row.lote:08}' if row.lote > 0 else None

        ult_sessao = 1
        vaunf = vaun1
        vatof = vato1

        cadpro = dict_produtos[row['cdmaterial']]
        codccusto = row['cdOrgaoReduzido']                                             
        qtdadt = quan1
        vaunadt = row['valoraditado']
        vatoadt =  row['valoraditado'] * row['qtitemproposta']
        tpcontrole_saldo = "Q"

        CUR_FDB.execute(insert_cadpro_proposta,( sessao , codif , item , itemp , quan1
                , vaun1 , vato1 , numlic , status , subem , marca , itemlance, lotelic))

        CUR_FDB.execute(insert_cadpro_final,( numlic , ult_sessao , codif , itemp , vaunf
                , vatof , status , subem))

        CUR_FDB.execute(insert_cadpro,(codif , cadpro , quan1 , vaun1 , vato1 , subem , status
                , item , codccusto , numlic , ult_sessao , itemp , qtdadt , vaunadt , vatoadt
                , tpcontrole_saldo))
    commit()

    limpa_tabela('cadprolic_detalhe_fic')

    CUR_FDB.execute(
        """
        insert
            into
            CADPROLIC_DETALHE_FIC (NUMLIC,
            ITEM,
            CODIGO,
            CODCCUSTO,
            FICHA,
            ELEMENTO,
            DESDOBRO,
            BALCO_TCE,
            QTDMED,
            VALORMED,
            QTD,
            VALOR,
            QTDADT,
            VALORADT,
            EVGRUPO,
            EVCODIGO,
            VINGRUPO,
            VINCODIGO,
            SUBDESDOBRO,
            TIPO)
        select
            LID.NUMLIC,
            LID.ITEM,
            coalesce(ICF.CODIGO,
            -1),
            LID.CODCCUSTO,
            ICF.FICHA,
            ICF.ELEMENTO,
            ICF.DESDOBRO,
            ICF.BALCO_TCE,
            coalesce(ICF.QTD,
            LID.QUAN1),
            coalesce(ICF.VALOR,
            LID.VATOMED1),
            coalesce(ICF.QTD,
            CP.QUAN1,
            LID.QUAN1),
            cast(coalesce((coalesce(ICF.QTD,
            CP.QUAN1,
            LID.QUAN1) * CP.VAUN1),
            0) as numeric(18,
            5)),
            coalesce(ICF.QTD,
            CP.QTDADT,
            LID.QUAN1),
            cast(coalesce((coalesce(ICF.QTD,
            CP.QTDADT,
            LID.QUAN1) * CP.VAUNADT),
            0) as numeric(18,
            5)),
            ICF.EVGRUPO,
            ICF.EVCODIGO,
            ICF.VINGRUPO,
            ICF.VINCODIGO,
            ICF.SUBDESDOBRO,
            cast('C' as VARCHAR(1))
        from
            CADPROLIC_DETALHE LID
        left join ICADORC_FIC ICF on
            ICF.ID_CADORC = LID.ID_CADORC
            and ICF.ITEM = LID.ITEMORC
        left join CADPRO CP on
            CP.NUMLIC = LID.NUMLIC
            and CP.ITEM = LID.ITEM
            and (coalesce(CP.STATUS,
            'C') = 'C')
            and CP.SUBEM = 1
        where
            (not exists(
            select
                first 1 X.NUMLIC,
                X.ITEM
            from
                CADPROLIC_DETALHE_FIC X
            where
                X.NUMLIC = LID.NUMLIC
                and X.ITEM = LID.ITEM
                and X.CODIGO = coalesce(ICF.CODIGO,
                1)));""")
    commit()

    CUR_FDB.execute("""
    EXECUTE BLOCK AS  
        BEGIN  
        INSERT INTO REGPRECODOC (NUMLIC, CODATUALIZACAO, DTPRAZO, ULTIMA)  
        SELECT DISTINCT A.NUMLIC, 0, DATEADD(1 YEAR TO A.DTHOM), 'S'  
        FROM CADLIC A WHERE A.REGISTROPRECO = 'S' AND A.DTHOM IS NOT NULL  
        AND NOT EXISTS(SELECT 1 FROM REGPRECODOC X  
        WHERE X.NUMLIC = A.NUMLIC);  

        INSERT INTO REGPRECO (COD, DTPRAZO, NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, QTDENT, SUBEM, STATUS, ULTIMA)  
        SELECT B.ITEM, DATEADD(1 YEAR TO A.DTHOM), B.NUMLIC, B.CODIF, B.CADPRO, B.CODCCUSTO, B.ITEM, 0, B.QUAN1, B.VAUN1, B.VATO1, 0, B.SUBEM, B.STATUS, 'S'  
        FROM CADLIC A INNER JOIN CADPRO B ON (A.NUMLIC = B.NUMLIC) WHERE A.REGISTROPRECO = 'S' AND A.DTHOM IS NOT NULL  
        AND NOT EXISTS(SELECT 1 FROM REGPRECO X  
        WHERE X.NUMLIC = B.NUMLIC AND X.CODIF = B.CODIF AND X.CADPRO = B.CADPRO AND X.CODCCUSTO = B.CODCCUSTO AND X.ITEM = B.ITEM);  

        INSERT INTO REGPRECOHIS (NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, SUBEM, STATUS, MOTIVO, MARCA, NUMORC, ULTIMA)  
        SELECT B.NUMLIC, B.CODIF, B.CADPRO, B.CODCCUSTO, B.ITEM, 0, B.QUAN1, B.VAUN1, B.VATO1, B.SUBEM, B.STATUS, B.MOTIVO, B.MARCA, B.NUMORC, 'S'  
        FROM CADLIC A INNER JOIN CADPRO B ON (A.NUMLIC = B.NUMLIC) WHERE A.REGISTROPRECO = 'S' AND A.DTHOM IS NOT NULL  
        AND NOT EXISTS(SELECT 1 FROM REGPRECOHIS X  
        WHERE X.NUMLIC = B.NUMLIC AND X.CODIF = B.CODIF AND X.CADPRO = B.CADPRO AND X.CODCCUSTO = B.CODCCUSTO AND X.ITEM = B.ITEM);  
    END;
    """)
    commit()

    CUR_FDB.execute(
        """
        update
            CADLIC C
        set
            DISCR10 = lpad(extract(day
        from
            dthom),
            2,
            '0') || '/' || lpad( extract(month
        from
            dthom),
            2,
            '0') || '/' 
        || cast(extract(year
        from
            dthom)+ 1 as varchar(4))
        where
            REGISTROPRECO = 'S'
    """)
    commit()