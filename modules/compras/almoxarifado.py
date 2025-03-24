from connection import commit, CUR_FDB, fetchallmap
from utils import limpa_tabela, EMPRESA, dict_produtos, dict_fornecedores

requi = CUR_FDB.prep("""
insert
    into
    requi(
        empresa
        , id_requi
        , requi
        , num
        , ano
        , dtlan
        , datae
        , dtpag
        , entr
        , said                
        , comp
        , nempg_requi
        , codif
        , obs                
        , anoemp_requi
        , id_cadped
        , docum
        , responsa 
        , recebe 
    )
values (?,gen_id(id_requi,1),?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,?,
        ?,?,?)
""")

icadreq = CUR_FDB.prep("""
insert
    into
    icadreq(
        empresa
        , id_requi
        , requi
        , item
        , cadpro
        , quan1
        , quan2
        , vaun1
        , vaun2
        , vato1
        , vato2
        , lote
        , codccusto
        , destino                
    )
values (?,gen_id(id_requi),?,?,?,
        ?,?,?,?,?,
        ?,?,?,?)""")

def saldo_inicial():
    limpa_tabela(('icadreq where requi = 00000/25','requi where requi = 00000/25'))

    rows = fetchallmap("""
    select
        *
    from
        (
        select
            p.cdmaterial,
            (
            select
                sum(m.qtdeentrada - m.qtdesaida)
            from
                MOVIMENTO M
            where
                m.CDMATERIAL = p.cdmaterial
                and m.cdoperacao <> 12
                and year(DTMOVIMENTO) < 2025 
            ) as quantidade,
            (
            select
                top 1 
            sldvalor
            from
                MOVIMENTO M
            where
                m.CDMATERIAL = p.cdmaterial
                and year(DTMOVIMENTO) < 2025
                    and m.cdoperacao <> 12
                order by
                    NRLANCAMENTO desc,
                    DTMOVIMENTO desc) as valor
        from
            {ENTIDADE}_ALMOX.dbo.PRODUTO P
        where
            p.tpmaterial <> 2) as q
    where
        quantidade > 0
    order by
        cdmaterial""")
    
    item = 0
    existe_lote = CUR_FDB.execute("select count(*) from lote_entidade").fetchone()[0]
    if existe_lote == 0:
        CUR_FDB.execute("insert into lote_entidade(empresa,cadpro, lote) select (select empresa from cadcli) , cadpro, '000000000' from cadest")
    
    mudou_requi = True

    for row in rows:
        if mudou_requi:
            num = '00000'
            ano = 2025
            dtlan = '31.12.2024'
            datae = dtlan
            dtpag = None
            entr = 'S'
            said = 'N'
            comp = 'P'
            nempg = None
            codif = None
            obs = None
            anoemp = None
            id_cadped = None
            docum = None
            responsa = None
            recebe = None

            CUR_FDB.execute(requi, (EMPRESA, requi, num, ano, 
                                dtlan, datae, dtpag, entr, said, comp, nempg, codif, obs, anoemp, 
                                id_cadped, docum, responsa, recebe))
            mudou_requi = False
        item += 1
        cadpro = dict_produtos[row['cdmaterial']]
        quan1 = row['quantidade']
        quan2 = 0
        vaun1 = row['valor'] / row['quantidade']
        vaun2 = 0
        vato1 = row['valor']
        vato2 = 0
        lote = '000000000'
        codccusto = 0
        destino = '000000001'

        CUR_FDB.execute(icadreq, (EMPRESA, requi, item, cadpro, quan1, quan2, vaun1, vaun2, vato1, vato2, lote, codccusto, destino))
    commit()
        
def movimento():
    limpa_tabela(('icadreq where requi <> 00000/25','requi where requi <> 00000/25'))

    rows = fetchallmap("""
    select
        case
            when cdoperacao = 7
            or cdoperacao = 8 then 'E'
            when cdoperacao = 1
            or cdoperacao = 2
            or cdoperacao = 9 then 'S'
            else 'X'
        end as operacao,
        cdoperacao,
        dtmovimento,	
        nrdocumento,
        inaplicacaoimediata ,
        qtdeentrada ,
        qtdesaida ,
        vlcustomedio,
        vlcustoitem,
        vlprecounitario,
        vlcustomediocontabilizado ,
        cdalmoxarifado,
        cdmaterial,
        cdfornecedor,
        nrnota,
        nrserie,
        dtemissao,
        cdorgaoreduzido
    from
        {ENTIDADE}_COMPRAS.dbo.movimento
    where
        year(dtmovimento) = 2025
        and cdoperacao > 0
    order by
        dtmovimento,
        nrdocumento ,
        cdoperacao
    """)

    item = 0
    numero = CUR_FDB.execute("select cast(coalesce(max(num),0) as integer) from requi").fetchone()[0]
    requi_atual = ''

    for row in rows:
        if row['operacao'] == 'X':
            exit('Operação inválida')

        requisicao_composta = str(row['dtmovimento']) + '-' + str(row['nrdocumento']) + '-' + str(row['cdoperacao'])

        if requi_atual != requisicao_composta:
            requi_atual = requisicao_composta
            item = 0
            numero += 1
            num = f'{numero:05}'
            requi = f'{num}/25'
            ano = 2025
            dtlan = row['dtmovimento']
            if row['operacao'] == 'E':
                datae = dtlan
                dtpag = None
                entr = 'S'
                said = 'N'
                vaun1 = row['vlcustoitem']
                vaun2 = 0
                vato1 = row['vlcustoitem']*row['qtdeentrada']
                vato2 = 0
            else:
                datae = None
                dtpag = dtlan
                entr = 'N'
                said = 'S'
                vaun2 = row['vlcustoitem']
                vaun1= 0
                vato2 = row['vlcustoitem']*row['qtdeentrada']
                vato1 = 0
            comp = 'P'
            nempg = None
            codif = None if row['cdfornecedor'] == 0 else dict_fornecedores[row['cdfornecedor']]
            obs = None
            anoemp = None
            id_cadped = None
            docum = row['nrdocumento']
            responsa = None
            recebe = None
            dtemissaonf = None
            entr_said = 'N'
            nfe_serie = row['nrserie']
            nfe_danfe = row['nrnota']

            CUR_FDB.execute(requi, (EMPRESA, requi, num, ano, 
                            dtlan, datae, dtpag, entr, said, comp, nempg, codif, obs, anoemp, 
                            id_cadped, docum, responsa, recebe, dtemissaonf , entr_said , nfe_serie , nfe_danfe))
        item += 1
        cadpro = dict_produtos[row['cdmaterial']]
        quan1 = row['qtdeentrada']
        quan2 = row['qtdesaida']
        lote = '000000000'
        codccusto = row['cdorgaoreduzido']
        destino = '00000001'

        CUR_FDB.execute(icadreq, (EMPRESA, requi, item, cadpro, quan1, quan2, vaun1, 
                        vaun2, vato1, vato2, lote, codccusto, destino))
    commit()