from connection import commit, CUR_FDB, fetchallmap, ENTIDADE
from utils import limpa_tabela, EMPRESA, dict_produtos, dict_fornecedores, obter_codif_por_codant
exercicio = CUR_FDB.execute("select mexer from cadcli").fetchone()[0]

def motoristas():
    limpa_tabela('motor')
    insert = CUR_FDB.prep(
        """
        insert
            into
            motor(cod,
            nome,
            cnh,
            dtvenccnh,
            categcnh,
            secretaria,
            telefone_celular)
        values(?,?,?,?,?,?,?)""")
    
    rows = fetchallmap(f"select * from {ENTIDADE}_FROTAS.dbo.motorista")

    for row in rows:
        cod = int(row['CdFuncionario'])
        nome = row['Nmfuncionario']
        cnh = str(int(row['NrCarteiraHabilitacao']))
        dtvenccnh = row['DtValidadeHabilitacao']
        categcnh = row['CdCategoriaHabilitacao']
        secretaria  = row['DsLogradouro']
        telefone_celular = row['NrFoneMotorista'][:15]
        CUR_FDB.execute(insert, (cod, nome, cnh, dtvenccnh, categcnh, secretaria, telefone_celular))
    commit()

def veiculos():
    limpa_tabela('veiculo')
    CUR_FDB.execute('update centrocusto set placa = Null where placa is not null')
    commit()

    insert = CUR_FDB.prep("""
    insert
        into
        veiculo(sequencia,
        placa,
        modelo,
        ano,
        anomod,
        renavam,
        chassi,
        combustivel,
        kminicial,
        kmatual,
        tipo_marcador,
        tipo_media,
        deslocamento_tce)
    values(?,?,?,?,?,?,
    ?,?,?,?,?,?,?)""")

    update = CUR_FDB.prep("update centrocusto set placa = ? where codccusto = ?")

    insert_custo = CUR_FDB.prep("""
    insert
            into
            centrocusto(poder,
            orgao,
            unidade,
            destino,
            ccusto,
            descr,
            placa,
            codccusto,
            empresa,
            ocultar)
        values(?,?,?,?,?,
        ?,?,?,?,?)""")
    
    rows = fetchallmap(f"""
    select
            p.descricao_unica as combustivel ,
            t.dstpveiculo as tipo ,
            replace(replace(v.nrplacaveiculo, '-', ''), ' ', '') as placa ,
            cdveiculo ,
            dtanofabricacao ,
            dtanomodelo ,
            nrrenavan ,
            nrchassi ,
            nrmarcadoratual ,
            nrmarcadorinicial
    from
        {ENTIDADE}_FROTAS.dbo.veiculo v
    left join {ENTIDADE}_FROTAS.dbo.tipoveiculo t on
        t.cdtpveiculo = v.cdtpveiculo
    left join {ENTIDADE}_ALMOX.dbo.produto p on
        p.cdmaterial = v.cdcombustivel
    where nrplacaveiculo <> ''
    order by
        dtanofabricacao desc
    """)

    custos = CUR_FDB.execute("select codccusto, upper(replace(descr,' ','')) as descricao from centrocusto c where lower(descr) like '%placa%'").fetchallmap()
    max_custo = CUR_FDB.execute("select max(codccusto) from centrocusto").fetchone()[0]

    for row in rows:
        sequencia = row['cdveiculo']
        placa = row['placa']
        modelo = row['dtanomodelo']
        ano = row['dtanofabricacao']
        anomod = row['dtanomodelo']
        renavam = row['nrrenavan']
        chassi = row['nrchassi']
        combustivel = row['combustivel'][:1] if row['combustivel'] is not None else None
        kminicial = row['nrmarcadorinicial']
        kmatual = row['nrmarcadoratual']
        tipo_marcador = "Hodômetro"
        tipo_media = 'Km/L'
        deslocamento_tce = '01 - Quilômetros'

        CUR_FDB.execute(insert, (sequencia, placa, modelo, ano, anomod, renavam, chassi, 
                        combustivel, kminicial, kmatual, tipo_marcador, tipo_media, deslocamento_tce))
    
        codccusto = next((x for x in custos if veiculos.placa in x['descricao']), None)

        if codccusto is not None:
            CUR_FDB.execute(update, (placa, codccusto['codccusto']))
        else:
            poder = '02'
            orgao = '01'
            unidade = '01'
            destino = '000000001'
            ccusto = '001'
            descr = row['placa']
            placa = row['placa']
            max_custo += 1
            codccusto = max_custo    
            ocultar = 'N'

            CUR_FDB.execute(insert_custo, (poder, orgao, unidade, destino, ccusto, descr, placa, 
                            codccusto, EMPRESA, ocultar))
    commit()

def abastecimento():
    requisicoes = CUR_FDB.execute("select id_requi from icadreq where placa is not null").fetchall()
    limpa_tabela('icadreq where placa is not null')
    limpa_tabela(f'requi where id_requi in ({list(requisicoes)})')

    rows = fetchallmap(
        f"""
        select
            m.cdveiculo ,
            replace(replace(v.nrplacaveiculo,'-',''),' ','') as placa,
            m.cditemmaterial ,
            m.nrsequencia ,
            m.nrdeordem ,
            m.cdfornecedormotorista ,
            m.nrnfdepartamento ,
            m.vlvalor ,
            m.nrmarcador ,
            m.qtmovimentada ,
            m.intpmaterial ,
            m.dshistorico ,
            m.dtmovimento
        from
            {ENTIDADE}_FROTAS.dbo.movtoveiculo m
        inner join {ENTIDADE}_FROTAS.dbo.veiculo v on 
            v.cdveiculo  = m.cdveiculo 
        where
            year(dtmovimento) = 2025  
        and cditemmaterial > 0       
        and v.nrplacaveiculo <> ''  
        order by
            dtmovimento,
            nrsequencia
        """)
    
    insert_cabecalhos = CUR_FDB.prep("""
    insert
        into
        requi(requi,
        num,
        ano,
        dtlan,
        datae,
        dtpag,
        codif,
        docum,
        entr,
        said,
        comp,
        tiposaida,
        codccusto,
        entr_said,
        id_requi,
        empresa)
    values(?,?,?,?,?,
    ?,?,?,?,?,?,?,?,?,
    ?,?)""")

    insert_itens = CUR_FDB.prep(
        """
        insert
            into
            icadreq(requi,
            item,
            cadpro,
            quan1,
            quan2,
            vaun1,
            vaun2,
            lote,
            vato1,
            vato2,
            placa,
            km,
            documento,
            codccusto,
            id_requi,
            empresa)
        values(?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,
        ?,?)""")
    
    id_requi = CUR_FDB.execute("select max(id_requi) from requi").fetchone()[0]
    numeracao = CUR_FDB.execute("select cast(max(num) as integer) from requi").fetchone()[0]
    movimento_anterior = None
    item = 0

    for abastecimento in rows:
        if movimento_anterior != abastecimento.dtmovimento:
            movimento_anterior = abastecimento.dtmovimento
            id_requi += 1
            numeracao += 1
            item = 0

            num = f'{numeracao:06}'
            requi = num + '/' + 25
            ano = exercicio
            dtlan = abastecimento['dtmovimento']
            datae = abastecimento['dtmovimento']
            dtpag = abastecimento['dtmovimento']
            codif = obter_codif_por_codant(abastecimento['cdfornecedormotorista']) if abastecimento['cdfornecedormotorista'] is not None else None
            docum = abastecimento['nrnfdepartamento']
            entr = 'S'
            said = 'S'
            comp = 'P'
            tiposaida = 'R'
            codccusto = None
            entr_said = 'S'

            CUR_FDB.execute(insert_cabecalhos, (requi, num, ano, dtlan, datae, dtpag, 
                    codif, docum, entr, said, comp, tiposaida, codccusto, entr_said, id_requi, EMPRESA))
        
        item += 1
        cadpro = dict_produtos(abastecimento['cditemmaterial'])
        quan1 = abastecimento['qtmovimentada']
        quan2 = abastecimento['qtmovimentada']
        vaun1 = abastecimento['vlvalor'] / abastecimento['qtmovimentada']
        vaun2 = vaun1
        lote = '000000000'
        vato1 = abastecimento['vlvalor']
        vato2 = vato1
        placa = abastecimento['placa']
        km = abastecimento['nrmarcador']
        documento = abastecimento['nrnfdepartamento']
     
        CUR_FDB.execute(insert_itens, (requi, item, cadpro, quan1, quan2, vaun1, vaun2, lote,
                vato1, vato2, placa, km, documento, codccusto, id_requi, EMPRESA))
        commit()     

    CUR_FDB.execute(
        """
        update
            icadreq q
        set
            codccusto = (
            select
                codccusto
            from
                centrocusto c
            where
                c.placa = q.placa)
        where
            q.placa is not null
        """
    )     
    commit()
    