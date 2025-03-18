from connection import commit, CUR_FDB, fetchallmap
from utils import limpa_tabela, EMPRESA, dict_produtos, dict_fornecedores

def pedidos():
    pass

def autorizacao():
    rows = fetchallmap("""
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
                    proposta b
            where
                    l.dtanoprocesso = b.dtanoprocesso
                and l.nrprocesso = b.nrprocesso
                and i.lote <> b.lote) 
                                    then 'S'
            else 'N'
        end as maisdeumlote
    from
        instrumentocontratual l
    inner join iteminstrumentocontratual i on
        l.nrinstrumentocontratual = i.nrinstrumentocontratual
        and l.dtanoinstrumentocontratual = i.dtanoinstrumentocontratual
        and l.tpinstrumentocontratual = i.tpinstrumentocontratual
    inner join instrumento_contratual_empenho e on
        e.nr_instrumento_contratual = l.nrinstrumentocontratual
        and e.dt_ano_instrumento_contratual = l.dtanoinstrumentocontratual
        and e.tp_instrumento_contratual = l.tpinstrumentocontratual
        and i.fk_instrumento_contratual_empenho = e.pkid
    left join localentrega z on
        z.cdlocalentrega = l.cdlocalentrega
    where
        e.nr_empenho <> 0
        and dt_ano_empenho <> 0
    order by
        l.nrinstrumentocontratual,
        e.dt_ano_empenho,
        e.nr_empenho
    """)

    id_cadped = CUR_FDB.execute("SELECT COALESCE(MAX(ID_CADPED),0) FROM CADPED").fetchone()[0]

    numero = CUR_FDB.execute("SELECT CAST(COALESCE(MAX(NUM),0) AS INTEGER) FROM CADPED WHERE ANO = 2025 ")

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
        )
    values (?,?,?,?,?,
            ?,?,?,?,?)""")
    
    pedido_atual = ""
    
    for row in rows:
        numero_composto = f'{row['nrinstrumentocontratual']}-{row['dtanoinstrumentocontratual']}-{row['tpinstrumentocontratual']}-{row['nr_empenho']}'
        codccusto = None
        if numero_composto != pedido_atual:
            pedido_atual = numero_composto
            numero += 1
            num = f'{numero:05}'
            numped = f'{num}/{row['dtanoprocesso']%2000}'
            ano = row['dtanoprocesso']

            npedlicit = f'{row['nr_empenho']}/{row['dt_ano_empenho']%2000}'
            pedido = next(x for x in lista_pedidos if x['numped'] == npedlicit)
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

            localentg = row['dslocalentrega']
            id_cadped += 1
            aditamento = None

            CUR_FDB.execute(insert_cabecalho, (numped, num, ano, codif, datped
                                            , codccusto, entrou, numlic
                                            , proclic, localentg, condpgto, prozoentrega
                                            , obs, id_cadped, EMPRESA, aditamento
                                            , contrato, npedlicit, id_cadpedlicit))
            
        item = row['nritem'] if row['maisdeumlote'] == 'N' else f'{row['lote']}{row['nritem']}'

        if row['lote'] > 9 or row['nritem'] > 99:
            item = f'{row['lote']}0{row['nritem']}'
        cadpro = dict_produtos[row['cdmaterial']]
        qtd = row['qtitem']
        prcunt = row['valor_unitario']
        prctot = row['qtitem']*row['valor_unitario']

        CUR_FDB.execute(insert_itens, (numped, id_cadped, item, cadpro, codccusto, qtd, prcunt, prctot))
    commit()

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
    
    CUR_FDB.execute("""
    insert
        into
        fcadped(numped,
        pkemp,
        ficha,
        valor,
        categoria,
        grupo,
        modalidade,
        elemento,
        desdobro,
        codfcadped,
        id_cadped)               
    select
        i.numped,
        p.conv_pkemp,
        i.ficha,
        sum(prctot),
        categoria,
        grupo,
        modalidade,
        elemento,
        desdobro,
        0,
        i.id_cadped
    from
        cadped p
    inner join icadped i on
        i.id_cadped = p.id_cadped
    where
        i.ficha is not null
    group by
        numped,
        p.conv_pkemp,
        ficha,
        categoria,
        grupo,
        modalidade,
        elemento,
        desdobro,
        i.id_cadped""")

    CUR_FDB.execute("update despes d set d.id_cadped = (select f.id_cadped from fcadped f where f.pkemp  = d.pkemp)")

    CUR_FDB.execute("update despes d set d.numped = (select f.numped from fcadped f where f.pkemp  = d.pkemp)")

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