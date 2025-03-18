from connection import commit, CUR_FDB, fetchallmap
from utils import limpa_tabela, cria_campo, EMPRESA, dict_produtos

def cadunimedida():
    limpa_tabela("cadunimedida")

    rows = fetchallmap("SELECT * FROM UnidadeMedida")

    [CUR_FDB.execute("insert into cadunimedida(sigla, descricao) values (?, ?)", (row['sgUnidadeMedida'], row['dsUnidadeMedida'])) for row in rows]
    
    commit()

def cadgrupo():
    limpa_tabela(('cadsubgr','cadgrupo'))
    
    cria_campo('cadgrupo', 'conv_id')
    cria_campo('cadgrupo', 'conv_tipo')

    rows = fetchallmap("""
    select
            cdclassereduzido,
            '1' as tipo,
            dsclassificacao
        from
            classificacao
        union all
        select
            cdclassereduzido,
            '2' ,
            dsclassificacao
        from
            classificacao_bens
        union all
        select
            cdclassereduzido,
            '3' ,
            dsclassificacao
        from
            classificacao_obras
        union            
    select
        cdclassereduzido ,
        '3',
        'NAO CADASTRADO'
    from
        produto p
    where
        not exists(
        select
            1
        from
            classificacao_obras o
        where
            o.cdclassereduzido = p.cdclassereduzido)
        and tpmaterial = 3
    """)

    for i, row in enumerate(rows):
        i += 1
        grupo = f'{i:03}'
        tipo = row['tipo']
        nome = row['dsclassificacao'].title()[:45]
        ocultar = 'N'
        id = row['cdclassereduzido']

        CUR_FDB.execute("INSERT INTO CadGrupo(grupo, nome, ocultar, conv_id, conv_tipo) VALUES (?, ?, ?, ?, ?)", (grupo,nome,ocultar,id,tipo))
    commit()

def cadest():
    limpa_tabela('cadest')

    rows = fetchallmap("""
    select
            'T' + cast(tpmaterial as varchar) + 'C' + cast(cdclassereduzido as varchar) as grupo,
            p.cdmaterial,
            p.cdclassereduzido ,
            tpmaterial ,
            case
                p.tpmaterial when 3 then 'S'
                else 'P'
            end as tipo,
            case p.tpmaterial when 2 then 'P'
                else 'C' end as usopro,
            p.dsmaterial ,
            p.damaterial ,                    
            u.sgunidademedida as siglas                    
        from
            produto p
        left join unidademedida u on
            u.cdunidademedida = p.cdunidademedida
        order by p.TPMATERIAL , p.cdclassereduzido 
    """)

    grupos = CUR_FDB.execute("select grupo,nome, cast(conv_id as integer) as id, cast(conv_tipo as integer) as tipo from cadgrupo").fetchallmap()
    codigo_int = 0
    subgrupos_set = {x for x in CUR_FDB.execute("select grupo||.||subgrupo from cadsubgr").fetchall()}

    for row in rows:
        if grupo_atual != row['grupo']:
            codigo_int = 0
            grupo_atual = row['grupo']
            subgrupo_int = 0

            localiza_grupo = next((x for x in grupos if x['id'] == row['cdclassereduzido'] and x['tipo'] == row['tpmaterial']), None) 

            grupo = localiza_grupo['grupo']
            nome_grupo = localiza_grupo['nome']
        codigo_int += 1

        if codigo_int % 1000:
            codigo_int = 1
            subgrupo_int += 1
            subgrupo = f'{subgrupo_int:03}'

            if grupo+'.'+subgrupo not in subgrupos_set:
                CUR_FDB.execute("insert into cadsubgr(grupo, subgrupo,nome,ocultar) values(?,?,?,?)", (grupo,subgrupo,nome_grupo,'N'))
                subgrupos_set.append(grupo+'.'+subgrupo)
        
        codigo = f'codigo_int:03'
        cadpro = f'{grupo}.{subgrupo}.{codigo}'
        disc1 = row['dsmaterial'].title()
        discr1 = row['dsmaterial'].title()
        tipopro = row['tipo']
        usopro = row['usopro']
        unid1 = row['siglas']
        codreduz = row['cdmaterial']

        CUR_FDB.execute("INSERT INTO Cadest(cadpro, grupo, subgrupo, codreduz,codigo, disc1, discr1, tipopro, usopro, unid1) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                    (cadpro,grupo,subgrupo,codreduz,codigo,disc1,discr1,tipopro,usopro,unid1))  
        dict_produtos[codreduz] = cadpro
    commit()

def centro_custo():
    limpa_tabela('centrocusto')

    rows = fetchallmap("""select cdlocalreduzido, dslocalizacao, dsresponsavel from localizacao l union select top 1 0 ,'Conversão', '' from localizacao""")

    local = CUR_FDB.execute("select first 1 poder, orgao, unidade from desorc where empresa = (select empresa from cadcli)").fetchonemap()

    for row in rows:
        poder = local['poder']
        orgao = local['orgao']
        unidade = local['unidade']
        destino = '000000001'
        ccusto = "001"
        descr = row.dslocalizacao.title()
        codccusto = row.cdlocalreduzido
        empresa = EMPRESA
        ocultar = "N"

        CUR_FDB.execute("INSERT INTO centrocusto(poder, orgao, unidade, destino, ccusto, descr, codccusto, empresa, ocultar) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (poder,orgao,unidade,destino,ccusto,descr,codccusto,empresa,ocultar))
    commit()

def destino():
    limpa_tabela('destino')

    rows = fetchallmap("""
    select
            cdalmoxarifado ,
            dsalmoxarifado
        from
            almoxarifado
        union 
        select
            top 1 0,
            'Conversão'
        from
            almoxarifado
    """)

    for row in rows:
        cod = f'{row.cdalmoxarifado:09}'
        desti = row.dsalmoxarifado.title()
        empresa = EMPRESA

        CUR_FDB.execute("INSERT INTO destino(cod, desti, empresa) VALUES (?, ?, ?)", (cod,desti,empresa))
    commit()