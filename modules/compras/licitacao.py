from connection import commit, CUR_FDB, fetchallmap, ENTIDADE
from utils import limpa_tabela, EMPRESA, dict_produtos, dict_fornecedores

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
        '100': {"modalidade": "DISPENSA ELETRÔNICA", "sigla": "DISPE", "indice": 11}, #Dispensa Eletrônica
        '520': {"modalidade": "DISPENSA", "sigla": "DISP", "indice": 1}, # Chamamento Público
        '410': {"modalidade": "CONCORRÊNCIA", "sigla": "CONC", "indice": 4}, # Concorrência
        '430': {"modalidade": "CONVITE", "sigla": "CONV", "indice": 2}, # Convite
        '450': {"modalidade": "LEILÃO", "sigla": "LEIL", "indice": 6}, # Credenciamento
        '480': {"modalidade": "PREGÃO", "sigla": "PP", "indice": 8}, # Pregão
        '48-1': {"modalidade": "PREGÃO ELETRÔNICO", "sigla": "PE", "indice": 9}, # P
        '420': {"modalidade": "TOMADA DE PREÇOS", "sigla": "TP", "indice": 3}, # Tomada de preços,
    }

    rows = fetchallmap(f"""
    select 
		Concat(nrProcesso, dtAnoProcesso%2000) numlic,
		Concat(format(nrProcesso, '000000'), dtAnoProcesso%2000) proclic,
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
        
        modalidade = modalidades.get(row['tipo_processo'], ['520'])
        
        modlic = modalidade['sigla']
        licit = modalidade['modalidade']
        anomod = row['dtAnoProcesso']
        codmod = row['indice']
        codtce = None
        
        CUR_FDB.execute(insert, (empresa, numlic, proclic, numero, ano,
                  comp, licnova, liberacompra, discr[:1024], detalhe,
                  registropreco, microempresa, numpro, discr7, datae,
                  processo_data, horabe, horreal, tipopubl, dtadj, dtenc, dthom,
                  local, modlic, licit, anomod, codmod,codtce))
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
    select
        a.dtanoprocesso,
        a.nrprocesso,
        lote,
        nmlote
    from
        {ENTIDADE}_COMPRAS.dbo.itemobjeto a
        join {ENTIDADE}_COMPRAS.dbo.processolicitatorio p on p.dtanoprocesso = a.dtanoprocesso 
        and p.nrprocesso  = a.nrprocesso and p.cdtipoprocesso  = a.cdtipoprocesso
    where lote > 0
    group by
        a.dtanoprocesso,
        a.nrprocesso,
        lote,
        nmlote
    order by
        a.dtanoprocesso,
        a.nrprocesso,
        lote,
        nmlote
    """)

    insert = CUR_FDB.prep("insert into cadlotelic(numlic,lotelic,descr,reduz,microempresa,tlance) values(?,?,?,?,?,?)")

    for row in rows:
        numlic = f'{row['nrprocesso']}{row['dtanoprocesso']%2000}'
        lotelic = f'{row['lote']:08}'
        descr = row['nmlote']
        reduz = 'N'
        microempresa = 'N'
        tlance = '$'

        CUR_FDB.execute(insert, (numlic, lotelic, descr, reduz, microempresa, tlance))
    commit()

def prolic_prolics():
    pass

def cadprolic():
    limpa_tabela('cadprolic_detalhe')
    limpa_tabela('cadprolic')

    rows = fetchallmap(f"""
    select
        b.cdOrgaoReduzido,
        a.dtanoprocesso,
        a.nrprocesso,
        a.lote,
        a.nritem,
        a.cdmaterial,
        a.vlcotacaoitem,
        sum(a.qtitemobjeto) as qtitemobjeto,
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
        (
        select
            count(*)
        from
            {ENTIDADE}_COMPRAS.dbo.itemobjeto d
        where
            d.nrprocesso = a.nrprocesso
            and d.dtanoprocesso = a.dtanoprocesso
            and d.lote = a.lote
            and d.nritem = a.nritem
            and a.cdmaterial = d.cdmaterial
            and d.vlcotacaoitem <> a.vlcotacaoitem) as duplicado
    from
        {ENTIDADE}_COMPRAS.dbo.itemobjeto a
    join {ENTIDADE}_COMPRAS.dbo.processolicitatorio p on
        p.dtanoprocesso = a.dtanoprocesso
        and p.nrprocesso = a.nrprocesso
        and p.cdtipoprocesso = a.cdtipoprocesso
    left join (
        select
            distinct
            right(replicate('0',
            5)+ cast(nrpesquisa as varchar),
            5)+ '/' + cast(dtanopesquisa % 2000 as varchar) numorc,
            qr.nritem,
            qr.lote,
            qr.cdmaterial,
            qr.cdOrgaoReduzido,
            qr.nrprocesso,
            qr.dtanoprocesso,
            cdTipoProcesso
        from
            (
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
                o.cdOrgaoReduzido,
                p.cdTipoProcesso
            from
                {ENTIDADE}_COMPRAS.dbo.processopesquisa p
            inner join {ENTIDADE}_COMPRAS.dbo.itempesquisa i on
                i.dtanopesquisa = p.dtanopesquisa
                and i.nrpesquisa = p.nrpesquisa
            left join {ENTIDADE}_COMPRAS.dbo.condicaopagamento f on
                f.cdcondicaopagamento = p.cdcondicaopagamento
            left join {ENTIDADE}_COMPRAS.dbo.localentrega l on
                l.cdlocalentrega = p.cdlocalentrega
            left join {ENTIDADE}_COMPRAS.dbo.orgaopesquisa o on
                o.dtAnoPesquisa = p.dtAnoPesquisa
                and o.nrPesquisa = p.nrPesquisa) as qr) b on
        a.nrprocesso = b.nrprocesso
        and a.dtAnoProcesso = b.dtanoprocesso
        and a.nritem = b.nritem
        and a.Lote = b.lote
        and a.cdMaterial = b.cdmaterial
        and a.cdTipoProcesso = b.cdTipoProcesso
    where
        a.nritem <> 0
    group by
        a.dtanoprocesso,
        a.nrprocesso,
        a.lote,
        a.nritem,
        a.cdmaterial,
        a.vlcotacaoitem,
        b.cdOrgaoReduzido
    having
        sum(qtitemobjeto) > 0
    order by
        a.dtanoprocesso,
        a.nrprocesso,
        a.lote,
        a.nritem,
        a.vlcotacaoitem desc
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
        )
    values (?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?)
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
        
        item = row['nritem'] if row['maisdeumlote'] == 'N' else f'{row['lote']}{row['nritem']}'
        
        if row['lote'] > 9 or row['nritem']:
            item = f'{row['lote']}0{row['nritem']}'

        item_mask = item
        itemorc = None
        cadpro = dict_produtos[row['cdmaterial']]
        quan1 = row['qtitemobjeto']
        vamed1 = row['vlcotacaoitem']
        vatomed1 = row['vlcotacaoitem']*row['qtitemobjeto']
        codccusto = row['cdOrgaoReduzido']
        ficha = None
        reduz = 'N'
        ordnumorc = None
        numlic = f'{row['nrprocesso']}{row['dtanoprocesso']%2000}'
        id_cadorc = None
        lotelic = f'{row['lote']:08}' if row['lote'] > 0 else None
        item_lote = item
        numorc = None
        item_cadprolic = item

        sessao = 1
        itemp = item
        telafinal = 'I_ENCERRAMENTO'
        aceito = 'S'

        CUR_FDB.execute(insert_itens,(item , item_mask , itemorc , cadpro , quan1
                , vamed1 , vatomed1 , codccusto , ficha , reduz , ordnumorc
                , numlic , id_cadorc , lotelic , item_lote))
        
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
    
    """)
