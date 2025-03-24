from connection import commit, CUR_FDB, fetchallmap, ENTIDADE
from utils import limpa_tabela, cria_campo, EMPRESA, dict_produtos, cpf_cnpj, dict_fornecedores, armazena_fornecedores

def cadunimedida():
    limpa_tabela("cadunimedida")

    rows = fetchallmap(F"SELECT * FROM {ENTIDADE}_ALMOX.dbo.UnidadeMedida")

    [CUR_FDB.execute("insert into cadunimedida(sigla, descricao) values (?, ?)", (row['sgUnidadeMedida'], row['dsUnidadeMedida'])) for row in rows]
    
    commit()

def cadgrupo():
    limpa_tabela('cadsubgr','cadgrupo')
    
    cria_campo('cadgrupo', 'conv_id')
    cria_campo('cadgrupo', 'conv_tipo')

    rows = fetchallmap(f"""
    select
            cdclassereduzido,
            '1' as tipo,
            dsclassificacao
        from
            {ENTIDADE}_ALMOX.dbo.classificacao
        union all
        select
            cdclassereduzido,
            '2' ,
            dsclassificacao
        from
            {ENTIDADE}_ALMOX.dbo.classificacao_bens
        union all
        select
            cdclassereduzido,
            '3' ,
            dsclassificacao
        from
            {ENTIDADE}_ALMOX.dbo.classificacao_obras
        union            
    select
        cdclassereduzido ,
        '3',
        'NAO CADASTRADO'
    from
        {ENTIDADE}_ALMOX.dbo.produto p
    where
        not exists(
        select
            1
        from
            {ENTIDADE}_ALMOX.dbo.classificacao_obras o
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
    CUR_FDB.execute("insert into cadsubgr(grupo,subgrupo,nome,ocultar) select grupo, '000', nome, ocultar from cadgrupo")
    commit()

def cadest():
    limpa_tabela('cadest')

    rows = fetchallmap(f"""
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
            {ENTIDADE}_ALMOX.dbo.produto p
        left join {ENTIDADE}_ALMOX.dbo.unidademedida u on
            u.cdunidademedida = p.cdunidademedida
        order by p.TPMATERIAL , p.cdclassereduzido 
    """)

    grupos = CUR_FDB.execute("select grupo,nome, cast(conv_id as integer) as id, cast(conv_tipo as integer) as tipo from cadgrupo").fetchallmap()
    codigo_int = 0
    subgrupos_set = {x for x in CUR_FDB.execute("select grupo||'.'||subgrupo from cadsubgr").fetchall()}
    grupo_atual = ''

    for row in rows:
        if grupo_atual != row['grupo']:
            codigo_int = 0
            grupo_atual = row['grupo']

            localiza_grupo = next((x for x in grupos if x['id'] == row['cdclassereduzido'] and x['tipo'] == row['tpmaterial']), None) 

            grupo = localiza_grupo['grupo']
            nome_grupo = localiza_grupo['nome']
        codigo_int += 1

        if codigo_int > 999:
            subgrupo = f'{codigo_int//1000}'.zfill(3)
            codigo = str(codigo_int)[-3:]

            if grupo+'.'+subgrupo not in subgrupos_set:
                CUR_FDB.execute("insert into cadsubgr(grupo, subgrupo,nome,ocultar) values(?,?,?,?)", (grupo,subgrupo,nome_grupo,'N'))
                subgrupos_set.add(grupo+'.'+subgrupo)
        else: 
            subgrupo = '000'
            codigo = f'{codigo_int}'.zfill(3)
        
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

    rows = fetchallmap(f"""WITH CTE AS (
        SELECT
            cdlocalreduzido,
            dslocalizacao,
            dsresponsavel,
            ROW_NUMBER() OVER (PARTITION BY cdlocalreduzido ORDER BY (SELECT NULL)) AS rn
        FROM
            {ENTIDADE}_ALMOX.dbo.localizacao
    )
    SELECT
        cdlocalreduzido,
        dslocalizacao,
        dsresponsavel
    FROM
        CTE
    WHERE
        rn = 1
    UNION
    SELECT
        TOP 1 0,
        'Conversão',
        ''
    FROM
        {ENTIDADE}_ALMOX.dbo.localizacao;""")

    local = CUR_FDB.execute("select first 1 poder, orgao, unidade from desorc where empresa = (select empresa from cadcli)").fetchonemap()

    for row in rows:
        poder = local['poder']
        orgao = local['orgao']
        unidade = local['unidade']
        destino = '000000001'
        ccusto = "001"
        descr = row['dslocalizacao'].title()
        codccusto = row['cdlocalreduzido']
        empresa = EMPRESA
        ocultar = "N"

        CUR_FDB.execute("INSERT INTO centrocusto(poder, orgao, unidade, destino, ccusto, descr, codccusto, empresa, ocultar) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (poder,orgao,unidade,destino,ccusto,descr,codccusto,empresa,ocultar))
    commit()

def destino():
    limpa_tabela('destino')

    rows = fetchallmap(f"""
    select
            cdalmoxarifado ,
            dsalmoxarifado
        from
            {ENTIDADE}_ALMOX.dbo.almoxarifado
        union 
        select
            top 1 0,
            'Conversão'
        from
            {ENTIDADE}_ALMOX.dbo.almoxarifado
    """)

    for row in rows:
        cod = f'{row['cdalmoxarifado']:09}'
        desti = row['dsalmoxarifado'].title()
        empresa = EMPRESA

        CUR_FDB.execute("INSERT INTO destino(cod, desti, empresa) VALUES (?, ?, ?)", (cod,desti,empresa))
    commit()

def fornecedores():
    cria_campo("desfor","modulo")
    cria_campo("desfor", "codant")
    limpa_tabela("desfor where modulo = 'COMPRAS'")
    CUR_FDB.execute("update desfor set codant = null")

    rows = fetchallmap(f"""
    select
            distinct 
                unidadefederacao.sguf,
            municipio.nmmunicipio,
            fornecedor.nmfantasia,
            fornecedor.nmfornecedor,
            fornecedor.cdfornecedor,
            fornecedor.inpessoafisicajuridica,
            fornecedor.nrcgccpf,
            fornecedor.nrinscricaoestadual,
            fornecedor.nrinscricaomunicipal,
            logradouro.nmlogradouro,
            enderecofornecedor.cdcep,
            enderecofornecedor.dsendereco,
            enderecofornecedor.nmbairro,
            enderecofornecedor.nrimovel,
            coalesce(enderecofornecedor.dscomplementologradouro,'') as dscomplementologradouro,
            contacorrentefornecedor.cdbanco,
            contacorrentefornecedor.cdagencia,
            contacorrentefornecedor.dgagencia,
            contacorrentefornecedor.cdcontacorrente,
            contacorrentefornecedor.dgcontacorrente
        from
            {ENTIDADE}_ALMOX.dbo.fornecedor
        left join {ENTIDADE}_ALMOX.dbo.enderecofornecedor on
            enderecofornecedor.cdfornecedor = fornecedor.cdfornecedor
        left join {ENTIDADE}_ALMOX.dbo.municipio on
            enderecofornecedor.cdpais = municipio.cdpais
            and enderecofornecedor.cduf = municipio.cduf
            and enderecofornecedor.cdmunicipio = municipio.cdmunicipio
        left join {ENTIDADE}_ALMOX.dbo.unidadefederacao on
            enderecofornecedor.cdpais = unidadefederacao.cdpais
            and enderecofornecedor.cduf = unidadefederacao.cduf
        left join {ENTIDADE}_ALMOX.dbo.logradouro              
                on
            enderecofornecedor.cdpais = logradouro.cdpais
            and enderecofornecedor.cduf = logradouro.cduf
            and enderecofornecedor.cdmunicipio = logradouro.cdmunicipio
            and enderecofornecedor.cdlogradouro = logradouro.cdlogradouro
        left join {ENTIDADE}_ALMOX.dbo.contacorrentefornecedor on
            fornecedor.cdfornecedor = contacorrentefornecedor.cdfornecedor
        order by
            cdfornecedor, dsendereco desc
    """)

    codif_max  = int(CUR_FDB.execute("select max(codif) from desfor").fetchone()[0])
    lstFornecedores = CUR_FDB.execute("SELECT CODIF , AUDESP_NUMERO, nome FROM DESFOR WHERE (INSMF IS NOT NULL) ").fetchallmap()

    insert = CUR_FDB.prep(
    """
    insert
        into
        desfor(codif,
        nome,
        nom_fant,
        ender,
        compl,
        cepci,
        uf,
        codant,
        cep,
        codtip,
        insmf,
        inscim,
        inest,
        banco,
        agenc,
        conta,
        modulo)
    values(?,?,?,?,?,?,
    ?,?,?,?,?,?,?,?,?,
    ?,?)""")

    update = CUR_FDB.prep("update desfor set codant = ? where codif = ?")

    incluidos = []

    for row in rows:
        cnpj = str(int(row['nrcgccpf']))

        if len(cnpj) == 12:
            cnpj = '00' + cnpj
        elif len(cnpj) == 13:
            cnpj = '0' + cnpj
        
        if len(cnpj) == 10:
            cnpj = '0' + cnpj

        encontra = next((x for x in lstFornecedores if x['audesp_numero'] == cnpj), None)

        if encontra is None:
            encontra = next((x for x in lstFornecedores if x['nome'] == row['nmfornecedor'][:50]), None)

        if encontra is None:

            if row['cdfornecedor'] in incluidos:
                continue

            incluidos.append(row['cdfornecedor'])
            codif_max += 1
            codif= codif_max
            nome=  row['nmfornecedor'][:50]
            nom_fant= row['nmfantasia'][:50] 
            ender= row['dsendereco']
            compl= row['dscomplementologradouro'][:20]
            cepci= row['nmmunicipio'] 
            uf= row['sguf']
            codant= row['cdfornecedor'] 
            cep= row['cdcep'] 
        
            dados = cpf_cnpj(cnpj)
            codtip= dados["tipo"] 
            insmf= dados["doc_nro"] 
        
            inscim= row['nrinscricaomunicipal'] 
            inest= row['nrinscricaoestadual']
            banco= row['cdbanco'] 
            agenc= row['cdagencia'] 
            conta= row['cdcontacorrente']
            modulo= 'COMPRAS'

            CUR_FDB.execute(insert, (codif, nome, nom_fant, ender, compl, cepci, uf, 
                    codant, cep, codtip, insmf, inscim, inest, banco, agenc, conta, modulo))

        else:
            CUR_FDB.execute(update, (row['cdfornecedor'], encontra['codif'])) 

    CUR_FDB.connection.commit()

    dict_fornecedores.clear()
    armazena_fornecedores()