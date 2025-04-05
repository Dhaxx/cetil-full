from connection import commit, CUR_FDB, fetchallmap, ENTIDADE
from utils import limpa_tabela, EMPRESA, dict_produtos, dict_fornecedores, obter_codif_por_codant
exercicio = CUR_FDB.execute("select mexer from cadcli").fetchone()[0]

insert_requi = CUR_FDB.prep("""
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
        , dtemissaonf 
        , entr_said 
        , nfe_serie 
        , nfe_danfe
    )
values (?,?,?,?,?,
        ?,?,?,?,?,?,
        ?,?,?,?,?,?,
        ?,?,?,?,?,?)
""")

insert_icadreq = CUR_FDB.prep("""
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
values (?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?)""")

def saldo_inicial():
    limpa_tabela("icadreq where requi = '00000/25'","requi where requi = '00000/25'")
    CUR_FDB.execute('ALTER TRIGGER TAU_ESTOQUE inactive')
    commit()

    id_requi = CUR_FDB.execute("select coalesce(max(id_requi),0) from requi").fetchone()[0]

    rows = fetchallmap(f"""
    WITH UltimoMovimento AS (
    SELECT 
        m.cdmaterial,
        m.cdAlmoxarifado,
        m.qtdeentrada,
        m.sldQuantidade,
        m.sldvalor,
        m.dtmovimento,
        m.nrlancamento,
        ROW_NUMBER() OVER (
            PARTITION BY m.cdmaterial, m.cdAlmoxarifado 
            ORDER BY m.dtmovimento DESC, m.nrlancamento DESC
        ) AS rn
    FROM 
        {ENTIDADE}_ALMOX.dbo.MOVIMENTO m
    where year(dtmovimento) < {exercicio}
    )
    SELECT 
        cdmaterial,
        cdAlmoxarifado,
        sldQuantidade,
        sldvalor,
        dtmovimento,
        nrlancamento
    FROM 
        UltimoMovimento
    WHERE 
        rn = 1 and sldQuantidade > 0""")
    
    item = 0
    existe_lote = CUR_FDB.execute("select count(*) from lote_entidade").fetchone()[0]
    if existe_lote == 0:
        CUR_FDB.execute("insert into lote_entidade(empresa,cadpro, lote) select (select empresa from cadcli) , cadpro, '000000000' from cadest")
    
    mudou_requi = True

    for row in rows:
        if mudou_requi:
            id_requi += 1
            num = '00000'
            ano = exercicio
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
            requi = f'{num}/{str(int(exercicio))[2:]}'

            CUR_FDB.execute(insert_requi, (EMPRESA, id_requi, requi, num, ano, 
                                dtlan, datae, dtpag, entr, said, comp, nempg, codif, obs, anoemp, 
                                id_cadped, docum, responsa, recebe, None, None, None, None))
            mudou_requi = False
            commit()
        item += 1
        cadpro = dict_produtos[str(row['cdmaterial'])]
        quan1 = row['sldQuantidade']
        quan2 = 0
        vaun1 = row['sldvalor'] / row['sldQuantidade']
        vaun2 = 0
        vato1 = row['sldvalor']
        vato2 = 0
        lote = '000000000'
        codccusto = 0
        destino = str(row['cdAlmoxarifado']).zfill(9)

        CUR_FDB.execute(insert_icadreq, (EMPRESA, id_requi, requi, item, cadpro, quan1, quan2, vaun1, vaun2, vato1, vato2, lote, codccusto, destino))
        commit()
        
def movimento():
    limpa_tabela("icadreq where requi <> '00000/25'", "requi where requi <> '00000/25'")

    rows = fetchallmap(f"""
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
        cast(cdalmoxarifado as varchar) cdalmoxarifado,
        cdmaterial,
        cdfornecedor,
        nrnota,
        nrserie,
        dtemissao,
        cdorgaoreduzido
    from
        {ENTIDADE}_ALMOX.dbo.movimento
    where
        year(dtmovimento) = {exercicio}
        and cdoperacao not in (0,12)
    order by
        dtmovimento,
        operacao,
        nrdocumento
    """)

    item = 0
    numero = CUR_FDB.execute("select cast(coalesce(max(num),0) as integer) from requi").fetchone()[0]
    requi_atual = ''
    id_requi = CUR_FDB.execute("select coalesce(max(id_requi),0) from requi").fetchone()[0]

    for row in rows:
        if row['operacao'] == 'X':
            continue

        requisicao_composta = str(row['dtmovimento']) + '-' + str(row['nrdocumento']) + '-' + str(row['cdoperacao'])

        if requi_atual != requisicao_composta:
            requi_atual = requisicao_composta
            id_requi += 1
            item = 0
            numero += 1
            num = f'{numero:05}'
            requi = f'{num}/25'
            ano = exercicio
            dtlan = row['dtmovimento']
            if row['operacao'] == 'E':
                datae = dtlan
                dtpag = None
                entr = 'S'
                said = 'N'
            else:
                datae = None
                dtpag = dtlan
                entr = 'N'
                said = 'S'
            comp = 'P'
            nempg = None
            codif = None if row['cdfornecedor'] == 0 else obter_codif_por_codant(row['cdfornecedor'])
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

            CUR_FDB.execute(insert_requi, (EMPRESA, id_requi, requi, num, ano, 
                            dtlan, datae, dtpag, entr, said, comp, nempg, codif, obs, anoemp, 
                            id_cadped, docum, responsa, recebe, dtemissaonf , entr_said , nfe_serie , nfe_danfe))
            commit()
        item += 1
        cadpro = dict_produtos[str(row['cdmaterial'])]
        if row['operacao'] == 'E':
            vaun1 = row['vlcustoitem']
            vaun2 = 0
            vato1 = row['vlcustoitem']*row['qtdeentrada']
            vato2 = 0
        else:
            vaun2 = row['vlcustoitem']
            vaun1= 0
            vato2 = row['vlcustoitem']*row['qtdesaida']
            vato1 = 0
        quan1 = row['qtdeentrada']
        quan2 = row['qtdesaida']
        lote = '000000000'
        codccusto = row['cdorgaoreduzido']
        destino = row['cdalmoxarifado'].zfill(9)

        CUR_FDB.execute(insert_icadreq, (EMPRESA, id_requi, requi, item, cadpro, quan1, quan2, vaun1, 
                        vaun2, vato1, vato2, lote, codccusto, destino))
        commit()