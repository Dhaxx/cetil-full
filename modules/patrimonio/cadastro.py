from connection import commit, CUR_FDB, CUR_SQLS, fetchallmap
from utils import EMPRESA, limpa_tabela, exec_statment

def pt_cadpat():
    limpa_tabela('pt_cadpat')

    rows = fetchallmap("""
    select
            distinct item.cditem,
            case when item.nrplaca = '' then 'S/P ' + cast(item.cditem as varchar) else item.nrplaca end nrplaca,
            item.nrnotafiscal,
            item.cdtpingresso,
            item.cdlocalizacao as sub,
            item.cdclassificacao,
            estadoconservacao.cdestadoconser,
            item.dsreduzida,
            item.dtaquisicao,
            item.vlaquisicao,
            item.vlatual,
            cdfornecedor,
            item.dtultmovto,
            item.dsmodelo,
            item.dsmarca,
            item.dsnrserie,
            'M' as percentemp_pat,
            case
                when cdmetododepreciacao = 1 then 'V'
                else null
            end as dae_pat,
            item.vlresidual as valres_pat,
            item.vidautil * 12 as percenqtd_pat,
            coalesce(item.dtiniciodepreciacao,
            item.dtaquisicao ) as dtlan,
            substring(replace(c.cdnivelcontabil, '.', ''), 1, 9) as balco,
            case when substring(replace(c.cdnivelcontabil, '.', ''), 1, 4) = '1232' 
                then 2 else 1 end as grupo
        from
            item
        inner join localizacao on
            item.cdlocalizacao = localizacao.cdlocalreduzido
        inner join classificacao on
            item.cdclassificacao = classificacao.cdclassereduzido
        inner join estadoconservacao on
            item.cdestadoconser = estadoconservacao.cdestadoconser
        left join BORA_PPA.dbo.ppaplanoconta c on
            c.cdcontacontabil = item.cdcontabil
        left join itemempenhos on
            item.cditem = itemempenhos.cditem
        order by
            item.cditem
    """)

    insert = CUR_FDB.prep("""
    insert
    into
    pt_cadpat(
        codigo_pat
        , empresa_pat
        , codigo_gru_pat
        , chapa_pat
        , codigo_set_pat
        , codigo_set_atu_pat
        , nota_pat
        , orig_pat
        , obs_pat
        , codigo_sit_pat
        , discr_pat
        , datae_pat
        , dtlan_pat
        , dt_contabil
        , valaqu_pat
        , valatu_pat
        , valres_pat
        , dae_pat
        , percentemp_pat
        , percenqtd_pat
        , codigo_cpl_pat
        , codigo_tip_pat
        , codigo_for_pat
        , hash_sinc
        )
    values( ?,? ,? ,? ,?
        ,? ,? ,? ,? ,? ,?
        ,? ,? ,? ,? ,? ,?
        ,? ,? ,? ,? ,? ,?,?)
    """)

    tipos_bens = {1: "C", 3: "O", 4: "D", 8: "S", 9: "P", 13: "F"}

    for row in rows:
        codigo_pat = row['cditem']
        hash_sinc = row['cditem']
        empresa_pat = EMPRESA
        codigo_gru_pat = row['grupo']
        chapa_pat = row['nrplaca'][:6]
        codigo_set_pat = row['sub']
        codigo_set_atu_pat = row['sub']
        nota_pat = row['nrnotafiscal'][:10]
        orig_pat = tipos_bens.get(row['cdtpingresso'], default="I")
        obs_pat = row['dsItem']
        codigo_sit_pat = row['cdestadoconser']
        discr_pat = row['dsreduzida'][:255]
        datae_pat = row['dtaquisicao']
        dtlan_pat = row['dtlan']
        dt_contabil = row['dtlan']
        valaqu_pat = row['vlaquisicao']
        valatu_pat = row['vlatual']
        valres_pat = row['valres_pat']
        dae_pat = row['dae_pat']
        percentemp_pat = row['percentemp_pat']
        percenqtd_pat = int(row['percenqtd_pat'])
        codigo_cpl_pat = row['balco']
        codigo_tip_pat = row['cdclassificacao']
        codif = row['cdFornecedor']

        CUR_FDB.execute(insert, (codigo_pat, empresa_pat, codigo_gru_pat,
        chapa_pat, codigo_set_pat, codigo_set_atu_pat,
        nota_pat, orig_pat, obs_pat, codigo_sit_pat,
        discr_pat, datae_pat, dtlan_pat, dt_contabil,
        valaqu_pat, valatu_pat, valres_pat, dae_pat,
        percentemp_pat, percenqtd_pat, codigo_cpl_pat,
        codigo_tip_pat, codif, hash_sinc))
    commit()

def pt_movbem():
    limpa_tabela('pt_movbem')

    rows = fetchallmap("""
    select
        case intipomovimento when 'B' then 'B'
            when 'T' then 'T'
            when 'I' then 'A'
            else intipomovimento end tipomovimento ,   *
    from
        (
        select
            cditem ,
            dtmovimento ,
            intipomovimento ,
            case
                when m.intipomovimento = 'T' then 0
                when m.intipomovimento = 'B' then m.vlareavaliar *-1
                when m.intipomovimento = 'R' then m.vlareavaliar - m.vlanterior
                else m.vlareavaliar
            end as valor,
            cast(dsobservacao as varchar) as obs,
            cdlocalanterior,
            cdlocalatual,
            v.dsmotivo ,
            m.inestorno ,
            m.cdtpbaixa ,
            m.dsfundamento ,
            chmovimento,
            'N' depreciacao
        from
            movimento m
        left join motivo v on
            v.cdmotivo = m.cdmotivo
        where
            inestorno > -1
            and intipomovimento <> 'E'
    union
        select
            cditem,
            dtmovimento,
            'D',
            vldepreciacao *-1,
            'Depreciação',
            null,
            null,
            'Depreciação',
            inestorno,
            null,
            null,
            99999999,
            'S' depreciacao
        from
            movimentodepreciacao md
        where
            inestorno > -1
            and not exists(
            select
                1
            from
                movimentodepreciacao mc
            where
                mc.cditem = md.cditem
                and mc.dtestorno = md.dtmovimento
                and mc.vldepreciacao = md.vldepreciacao
                and mc.inestorno < 0 )
    union
        select
            cditem ,
            dtmovimento ,
            'R',
            vlcomplementar ,
            'Valor Complementar',
            null,
            null,
            'Valor Complementar',
            inestorno ,
            null,
            null,
            99999999,
            'N'
        from
            movimentovlcomplementar c
        where
            inestorno > -1 and exists(select 1 from item i where i.cditem = c.cditem)) as movimentacao
    order by
        cditem,
        dtmovimento ,
        chmovimento
    """)

    insert = CUR_FDB.prep("""
    insert
        into
        pt_movbem(
            codigo_mov
            , empresa_mov
            , codigo_pat_mov
            , data_mov
            , tipo_mov
            , valor_mov
            , lote_mov
            , codigo_cpl_mov
            , codigo_set_mov
            , documento_mov
            , historico_mov
            , dt_contabil
            , depreciacao_mov
            , codigo_bai_mov
            , codigo_set_mov_ant
        )
        values(?,?,?,?,?
        ,?,?,?,?,?,?,?,?,?,?)
    """)

    for i, row in enumerate(rows):
        i += 1
        codigo_mov = i
        empresa_mov = EMPRESA
        codigo_pat_mov = row['cditem']
        data_mov = row['dtmovimento']
        tipo_mov = row['tipomovimento']
        valor_mov = row['valor']
        lote_mov = 0
        codigo_cpl_mov = '123810199' if row.intipomovimento == 'D' else None
        codigo_set_mov = row['cdlocalatual']
        codigo_set_mov_ant = row['cdlocalanterior']
        documento_mov = row['obs']
        historico_mov = row['dsfundamento']
        dt_contabil = row['dtmovimento']
        depreciacao_mov = row['depreciacao']
        codigo_bai_mov = row['cdtpbaixa'] if tipo_mov == 'B' else None

        CUR_FDB.execute(insert , (codigo_mov
        , empresa_mov
        , codigo_pat_mov
        , data_mov
        , tipo_mov
        , valor_mov
        , lote_mov
        , codigo_cpl_mov
        , codigo_set_mov
        , codigo_set_mov_ant
        , documento_mov
        , historico_mov
        , dt_contabil
        , depreciacao_mov
        , codigo_bai_mov)) 
    commit()

    CUR_FDB.execute("""
    MERGE INTO PT_CADPAT A
    USING (
        SELECT DATA_MOV, CODIGO_PAT_MOV, CODIGO_BAI_MOV
        FROM PT_MOVBEM
        WHERE TIPO_MOV = 'B'
    ) B
    ON A.CODIGO_PAT = B.CODIGO_PAT_MOV
    WHEN MATCHED THEN
        UPDATE SET
            A.DTPAG_PAT = B.DATA_MOV,
            A.CODIGO_BAI_PAT = B.CODIGO_BAI_MOV;""")
    commit()

    CUR_FDB.execute("""
    MERGE INTO PT_MOVBEM A 
    USING (SELECT CODIGO_CPL_PAT FROM PT_CADPAT) B 
    ON A.CODIGO_PAT_MOV = B.CODIGO_PAT AND A.CODIGO_CPL_MOV IS NULL
    WHEN MATCHED THEN UPDATE SET A.CODIGO_CPL_MOV = B.CODIGO_CPL_PAT
    """)   
    commit()

    CUR_FDB.execute(
        """
        insert
            into
            pt_movbem(codigo_mov,
            empresa_mov,
            codigo_pat_mov,
            data_mov,
            codigo_cpl_mov,
            tipo_mov,
            codigo_set_mov,
            valor_mov,
            dt_contabil)
        select
            gen_id(gen_pt_movbem_id,1),
            empresa_pat ,
            codigo_pat,
            dtlan_pat ,
            '237110301',
            'R',
            codigo_set_atu_pat ,
            0,
            dtlan_pat
        from
            pt_cadpat p
        where
            p.dtpag_pat is null
            and exists(
            select
                1
            from
                pt_movbem m
            where
                m.codigo_pat_mov = p.codigo_pat
                and m.depreciacao_mov = 'S') 
            and not exists (
            select
                1
            from
                pt_movbem m
            where
                m.codigo_pat_mov = p.codigo_pat
                and m.codigo_cpl_mov = '237110301')
        """)
    commit()