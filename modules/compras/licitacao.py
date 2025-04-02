from connection import commit, CUR_FDB, fetchallmap, ENTIDADE
from utils import limpa_tabela, EMPRESA, dict_produtos, dict_fornecedores, obter_codif_por_nome, cria_campo, obter_codif_por_codant

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
            , dtreal
            , dtpropostaini
            , dtpropostafim
            , dtenv
        )
    values (?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?)""")

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
		pkid numlic,
		Concat(format(nrProcesso, '000000'),'/',format(dtAnoProcesso%2000, '00')) proclic,
		format(nrProcesso, '000000') numero,
		dtanoprocesso ano,
		comp = 3,
		licnova = 1,
		liberacompra = 'S',
		discr = daObjeto,
		detalhe = dsObjeto,
		case when inRegistroPreco = -1 then 'S' else 'N' end registropreco,
		microempresa = 2,
		numpro = nrModalidade,
		CASE 
		    WHEN CAST(dsCriterio AS VARCHAR(MAX)) = 'Menor preço - Global' THEN 'Menor Preco Global' 
		    ELSE 'Menor Preco Unitario' 
		END AS discr7,
		dtExpedicao datae,
		format(coalesce(dtexpedicao, '00:00'), 'HH:mm') horabe,
		dtHomologacao,
		Concat(cdTipoProcesso,cdtipoModalidade,inPregaoEletronico) tipo_processo,
		dacondicaopagamento,
        dtRespostaSolicitacao dtreal,
        dtInicioEnvioProposta,
        dtTerminoRecebimento,
        dtJulgamentoProposta
    from
        {ENTIDADE}_COMPRAS.dbo.processolicitatorio
    left join {ENTIDADE}_COMPRAS.dbo.condicaopagamento on
        condicaopagamento.cdcondicaopagamento = processolicitatorio.cdcondicaopagamento
    where cdUnidadeGestora = 0
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

        dtreal = row['dtreal']
        dtpropostaini = row['dtInicioEnvioProposta']
        dtpropostafim = row['dtTerminoRecebimento']
        dtenv = row['dtJulgamentoProposta']
        
        CUR_FDB.execute(insert, (empresa, numlic, proclic, numero, ano,
                  comp, licnova, liberacompra, discr[:1024], detalhe,
                  registropreco, microempresa, numpro, discr7, datae,
                  processo_data, horabe, horreal, tipopubl, dtadj, dtenc, dthom,
                  local, modlic, licit, anomod, codmod, codtce, dtreal, dtpropostaini, dtpropostafim, dtenv))
    commit()

    CUR_FDB.execute("""
    INSERT INTO CADLIC_SESSAO (NUMLIC, SESSAO, DTREAL, HORREAL, COMP, DTENC, HORENC, SESSAOPARA, MOTIVO) 
	SELECT L.NUMLIC, CAST(1 AS INTEGER), L.DTREAL, L.HORREAL, L.COMP, L.DTENC, L.HORENC, CAST('T' AS VARCHAR(1)), CAST('O' AS VARCHAR(1)) FROM CADLIC L 
	WHERE numlic not in (SELECT FIRST 1 S.NUMLIC FROM CADLIC_SESSAO S WHERE S.NUMLIC = L.NUMLIC)
    """)
    commit()

    CUR_FDB.execute("""
    MERGE INTO CADORC A USING (SELECT numlic, proclic FROM cadlic) B
    ON A.PROCLIC = B.PROCLIC 
    WHEN MATCHED THEN UPDATE SET A.numlic = B.numlic;
    """)
    commit()

    CUR_FDB.execute("""
    MERGE INTO CADLIC A USING (SELECT id_cadorc, NUMORC, NUMLIC FROM CADORC) B
    ON A.NUMLIC = B.NUMLIC 
    WHEN MATCHED THEN UPDATE SET A.id_cadorc = B.id_cadorc, A.numorc = B.numorc
    """)
    commit()

def cadlotelic():
    limpa_tabela('cadlotelic')

    rows = fetchallmap(f"""
    SELECT
        DISTINCT 
            p.pkid identificador,
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
    left join {ENTIDADE}_COMPRAS.dbo.TABELA_PRECO_PROCESSO tpp
            on
        tpp.FK_PROCESSO_LICITATORIO = p.PKID
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
        p.dtAnoProcesso,
        p.pkid
    union all
    select
        distinct tpp.FK_PROCESSO_LICITATORIO,
        nr_lote,
        tpp.NM_LOTE,
        cd_tipo_processo
    from
        {ENTIDADE}_COMPRAS.dbo.TABELA_PRECO_PROCESSO tpp
    ORDER BY
        p.cdTipoProcesso
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
        p.pkid AS identificador,
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
    limpa_tabela('cadpro_status', 'cadprolic_detalhe', 'cadprolic')
    CUR_FDB.execute('ALTER TRIGGER TBIU_CADPRO_STATUS inactive')
    commit()
    cria_campo('cadprolic', 'codant')

    rows = fetchallmap(f"""
    SELECT
        x.nrprocesso,
        x.dtanoprocesso,
        x.cdtipoProcesso,
        x.PKID,
        x.codant,
        x.cdmaterial,
        x.lote,
        coalesce(x.codccusto,
        0) codccusto,
        coalesce(x.qtitemObjeto,
        1) qtitemObjeto,
        coalesce(x.vlCotacaoItem,
        0) vlCotacaoItem,
        ROW_NUMBER() OVER (PARTITION BY x.PKID
    ORDER BY
        x.PKID) AS nritem
    FROM
        (
        SELECT
            DISTINCT 
                            nrprocesso,
            dtanoprocesso,
            cdtipoProcesso,
            PKID,
            nritem,
            CONCAT(PKID, '-', nritem, '-', cdmaterial, '-', lote) AS codant,
            cast(cdmaterial as varchar) cdmaterial,
            lote,
            cdOrgaoReduzido codccusto,
            qtdeItem qtitemObjeto,
            vlCotacaoItem
        FROM
            (
            SELECT
                p.nrprocesso,
                c.dtanoprocesso,
                c.cdtipoProcesso,
                p.PKID,
                cdFornecedor,
                c.Lote,
                c.nritem,
                case
                    when c.FK_TABELA_PRECO_PROCESSO is not null then concat('TBL', cast(fk_tabela_preco as varchar))
                    else cast(c.cdMaterial as varchar)
                end as cdMaterial,
                --coalesce(t.fk_tabela_preco , c.cdMaterial) cdMaterial,
                vlCotacaoProposta,
                nrClassificacao,
                i2.qtdeItem,
                coalesce(i2.VALOR_ESTIMADO,
                0) vlCotacaoItem,
                d.cdOrgaoReduzido
            FROM
                {ENTIDADE}_COMPRAS.dbo.classificacaoproposta c
            JOIN {ENTIDADE}_COMPRAS.dbo.PROCESSOLICITATORIO p 
                                    ON
                c.nrProcesso = p.nrProcesso
                AND c.dtAnoProcesso = p.dtAnoProcesso
                AND c.cdTipoProcesso = p.cdTipoProcesso
            left JOIN {ENTIDADE}_COMPRAS.dbo.ITEMOBJETO i 
                                ON
                i.ID_PROCESSOLICITATORIO = p.PKID
                and CONCAT(i.ID_PROCESSOLICITATORIO, '-', i.nritem, '-', i.cdmaterial, '-', i.lote) = CONCAT(c.FK_PROCESSO_LICITATORIO, '-', c.nritem, '-', c.cdmaterial, '-', c.lote)
            left JOIN {ENTIDADE}_COMPRAS.dbo.ITEMDESPESA i2 
                                    ON
                i2.nrProcesso = p.nrProcesso
                and i2.dtAnoProcesso = p.dtAnoProcesso
                and i2.cdTipoProcesso = p.cdTipoProcesso
                and i2.Lote = i.Lote
                and i2.nrItem = i.nrItem
            left join {ENTIDADE}_COMPRAS.dbo.DESPESA d
                                    ON
                i2.dtAnoExercicio = d.dtAno
                and i2.cdDespesa = d.cdDespesa
            left join (
                select
                    pkid,
                    fk_tabela_preco,
                    vl_estimado
                from
                    {ENTIDADE}_COMPRAS.dbo.TABELA_PRECO_PROCESSO tpp) t on
                t.pkid = c.FK_TABELA_PRECO_PROCESSO) AS qr
        ) AS x""")

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
            ?)""")

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
        item = row['nritem']
        item_mask = item
        itemorc = None
        cadpro = dict_produtos[str(row['cdmaterial'])]
        quan1 = row['qtitemObjeto']
        vamed1 = row['vlCotacaoItem']
        vatomed1 = row['vlCotacaoItem']*row['qtitemObjeto']
        codccusto = row['codccusto']
        ficha = None
        reduz = 'N'
        ordnumorc = None
        numlic = row['PKID']
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
    limpa_tabela('cadpro','regpreco','regprecodoc','regprecohis','cadprolic_detalhe_fic','cadpro_final','cadpro_proposta')
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
    SELECT
        p.nrprocesso,
        c.dtanoprocesso,
        c.cdtipoProcesso,
        p.PKID,
        f.insmf,
        c.cdFornecedor,
        c.Lote,
        c.nritem,
        case
            when c.FK_TABELA_PRECO_PROCESSO is not null then concat('TBL', cast(fk_tabela_preco as varchar))
            else cast(c.cdMaterial as varchar)
        end as cdMaterial,
        vlCotacaoProposta,
        nrClassificacao,
        coalesce(i.qtitemObjeto,
        1) qtitemObjeto,
        i.vlCotacaoItem,
        CONCAT(p.PKID, '-', c.nritem, '-', case
                        when c.FK_TABELA_PRECO_PROCESSO is not null then concat('TBL', cast(fk_tabela_preco as varchar))
                        else cast(c.cdMaterial as varchar)
                    end, '-', c.lote) AS codant,
        coalesce(
                        (
        select
            top 1 cp.vlcotacaoproposta
        from
            {ENTIDADE}_COMPRAS.dbo.CLASSIFICACAOPROPOSTA cp
        where
            c.dtanoprocesso = cp.dtanoprocesso
            and c.cdtipoprocesso = cp.cdtipoprocesso
            and c.nrprocesso = cp.nrprocesso
            and c.lote = cp.lote
            and c.nritem = cp.nritem
            and c.cdmaterial = cp.cdmaterial
            and c.cdfornecedor = cp.cdfornecedor
            and cp.tpregistro = 'R'
        order by
            ordem desc),
        c.vlcotacaoproposta) valoraditado
    FROM
        {ENTIDADE}_COMPRAS.dbo.classificacaoproposta c
    JOIN {ENTIDADE}_COMPRAS.dbo.PROCESSOLICITATORIO p 
                ON
        c.nrProcesso = p.nrProcesso
        AND c.dtAnoProcesso = p.dtAnoProcesso
        AND c.cdTipoProcesso = p.cdTipoProcesso
    left JOIN {ENTIDADE}_COMPRAS.dbo.ITEMOBJETO i 
                ON
        i.ID_PROCESSOLICITATORIO = p.PKID
        and CONCAT(i.ID_PROCESSOLICITATORIO, '-', i.nritem, '-', i.cdmaterial, '-', i.lote) = CONCAT(c.FK_PROCESSO_LICITATORIO, '-', c.nritem, '-', c.cdmaterial, '-', c.lote)
    JOIN (
        select
            trim(CAST(CAST(f.nrcgccpf AS DECIMAL(14, 0)) AS CHAR(14))) insmf,
            f.cdFornecedor
        from
            {ENTIDADE}_ALMOX.dbo.Fornecedor f) f on
        f.cdFornecedor = c.cdFornecedor
    left join (
        select
            pkid,
            fk_tabela_preco
        from
            {ENTIDADE}_COMPRAS.dbo.TABELA_PRECO_PROCESSO) t on
        t.pkid = c.FK_TABELA_PRECO_PROCESSO""")

    itens_processo = {codant : (item, codccusto) for codant, item, codccusto in CUR_FDB.execute('select codant, item, codccusto from cadprolic').fetchall()}

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
            codif = obter_codif_por_codant(row['cdFornecedor'])
        
        itens_info = itens_processo.get(str(row['codant']).strip(), 0)

        if itens_info.__class__ == tuple:
            item = itens_info[0]
            codccusto = itens_info[1] 
        else:
            item = 0
            codccusto = 0
        itemp = item
        quan1 = row['qtitemObjeto']
        vaun1 = row['vlCotacaoProposta']
        vato1 = quan1*vaun1
        numlic = row['PKID']
        status = 'C'
        subem = row['nrClassificacao']
        marca = None
        itemlance = 'S'

        lotelic = f'{row['Lote']:08}' if row['Lote'] > 0 else None

        ult_sessao = 1
        vaunf = vaun1
        vatof = vato1

        cadpro = dict_produtos[str(row['cdMaterial'])]
                                        
        qtdadt = quan1
        vaunadt = row['valoraditado']
        vatoadt =  row['valoraditado'] * row['qtitemObjeto']
        tpcontrole_saldo = "Q"

        try:
            CUR_FDB.execute(insert_cadpro_proposta,( sessao , codif , item , itemp , quan1
                    , vaun1 , vato1 , numlic , status , subem , marca , itemlance, lotelic))

            CUR_FDB.execute(insert_cadpro_final,( numlic , ult_sessao , codif , itemp , vaunf
                    , vatof , status , subem))

            CUR_FDB.execute(insert_cadpro,(codif , cadpro , quan1 , vaun1 , vato1 , subem , status
                    , item , codccusto , numlic , ult_sessao , itemp , qtdadt , vaunadt , vatoadt
                    , tpcontrole_saldo))
        except:
            continue
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