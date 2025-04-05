from connection import commit, CUR_FDB, fetchallmap, ENTIDADE
from utils import EMPRESA, limpa_tabela, cria_campo

def pt_cadpat():
    limpa_tabela('pt_cadpat')

    query = f"""
    select distinct item.cditem,
	case
		when item.nrplaca = '' then cast(item.cditem as varchar)
		else item.nrplaca
	end nrplaca,
	item.nrnotafiscal,
	item.cdtpingresso,
	item.cdlocalizacao,
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
	case
		when substring(replace(c.cdnivelcontabil, '.', ''), 1, 4) = '1232' then 2
		else 1
	end as grupo
    from
        {ENTIDADE}_PATRI.dbo.item
    inner join {ENTIDADE}_PATRI.dbo.localizacao on
        item.cdlocalizacao = localizacao.cdlocalreduzido
    inner join {ENTIDADE}_PATRI.dbo.classificacao on
        item.cdclassificacao = classificacao.cdclassereduzido
    inner join {ENTIDADE}_PATRI.dbo.estadoconservacao on
        item.cdestadoconser = estadoconservacao.cdestadoconser
    left join {ENTIDADE}_PPA.dbo.ppaplanoconta c on
        c.cdcontacontabil = item.cdcontabil
    left join {ENTIDADE}_PATRI.dbo.itemempenhos on
        item.cditem = itemempenhos.cditem
    order by
        item.cditem"""

    rows = fetchallmap(query)

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
        chapa_pat = str(row['nrplaca']).zfill(6)
        codigo_set_pat = row['cdlocalizacao']
        codigo_set_atu_pat = row['cdlocalizacao']
        nota_pat = row['nrnotafiscal'][:10]
        orig_pat = tipos_bens.get(row['cdtpingresso'], "I")
        obs_pat = row['dsreduzida']
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
        codif = row['cdfornecedor']

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
    cria_campo('pt_movbem', 'codigo_set_mov_ant')

    rows = fetchallmap(f"""
    select
        case intipomovimento
            when 'B' then 'B'
            when 'T' then 'T'
            when 'I' then 'A'
            else intipomovimento
        end as tipomovimento,
        * 
    from (
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
                {ENTIDADE}_PATRI.dbo.movimento m
            left join {ENTIDADE}_PATRI.dbo.motivo v on
                v.cdmotivo = m.cdmotivo
            where
                inestorno > -1
                and intipomovimento <> 'E'
        union
            select
                cditem,
                dtmovimento,
                'R',
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
                {ENTIDADE}_PATRI.dbo.movimentodepreciacao md
            where
                inestorno > -1
                and not exists(
                select
                    1
                from
                    {ENTIDADE}_PATRI.dbo.movimentodepreciacao mc
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
                {ENTIDADE}_PATRI.dbo.movimentovlcomplementar c
            where
                inestorno > -1 and exists(select 1 from {ENTIDADE}_PATRI.dbo.item i where i.cditem = c.cditem)
    ) as movimentacao
    order by
        cditem,
        dtmovimento,
        case
            when intipomovimento = 'B' then 99
            when intipomovimento = 'T' then 50
            when intipomovimento = 'I' then 10
            when intipomovimento = 'R' then 20
            else 0
        end,
        chmovimento""")

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
            , codigo_set_mov_ant
            , documento_mov
            , historico_mov
            , dt_contabil
            , depreciacao_mov
            , codigo_bai_mov
        )
        values(?,?,?,?,?
        ,?,?,?,?,?,?,?,?,?,?)
    """)

    dtlan = {}

    for i, row in enumerate(rows):
        i += 1
        codigo_mov = i
        empresa_mov = EMPRESA
        codigo_pat_mov = row['cditem']
        data_mov = row['dtmovimento']
        tipo_mov = row['tipomovimento']
        valor_mov = row['valor']
        lote_mov = 0
        if (row['intipomovimento'] == 'R' and row['depreciacao'] == 'S'):
            codigo_cpl_mov = '123810199'  
        elif (row['intipomovimento'] == 'R' and row['depreciacao'] == 'N'):
            codigo_cpl_mov = '237110301'
            dtlan[codigo_pat_mov] = data_mov
        else: codigo_cpl_mov = None
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

    [CUR_FDB.execute(f"update pt_cadpat set dtlan_pat = '{v.strftime('%Y-%m-%d')}' where codigo_pat = {k}") for k,v in dtlan.items()]
    commit()

    # Atualiza a data e tipo de baixa no cadastro
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

    # Atualiza setor de incorporação com base na primeira transferência
    CUR_FDB.execute("""
    MERGE INTO PT_CADPAT A
    USING (
        SELECT 
            M1.CODIGO_PAT_MOV, 
            M1.CODIGO_SET_MOV_ant
        FROM PT_MOVBEM M1
        WHERE M1.TIPO_MOV = 'T'
        AND M1.DATA_MOV = (
            SELECT MIN(M2.DATA_MOV)
            FROM PT_MOVBEM M2
            WHERE M2.CODIGO_PAT_MOV = M1.CODIGO_PAT_MOV
                AND M2.TIPO_MOV = 'T'
        )
    ) B
    ON (A.CODIGO_PAT = B.CODIGO_PAT_MOV) and B.CODIGO_SET_MOV_ANT IS NOT NULL
    WHEN MATCHED THEN
        UPDATE SET
            A.CODIGO_SET_PAT = B.CODIGO_SET_MOV_ant;""")
    commit()

    # Atualiza setor atual com base na última transferencia
    CUR_FDB.execute("""
        MERGE INTO PT_CADPAT A
    USING (
        -- Seleciona a última movimentação para atualizar o setor atual
        SELECT 
            M1.CODIGO_PAT_MOV, 
            M1.CODIGO_SET_MOV
        FROM PT_MOVBEM M1
        WHERE M1.TIPO_MOV = 'T'
        AND M1.DATA_MOV = (
            SELECT MAX(M2.DATA_MOV)
            FROM PT_MOVBEM M2
            WHERE M2.CODIGO_PAT_MOV = M1.CODIGO_PAT_MOV
                AND M2.TIPO_MOV = 'T'
        )
    ) C
    ON (A.CODIGO_PAT = C.CODIGO_PAT_MOV) AND C.CODIGO_SET_MOV IS NOT NULL
    WHEN MATCHED THEN
        UPDATE SET
            A.CODIGO_SET_ATU_PAT = C.CODIGO_SET_MOV;
    """)

    # Atualiza setor na movimentação de aquisição
    CUR_FDB.execute("""
    MERGE INTO PT_MOVBEM A
    USING (
        SELECT CODIGO_PAT, CODIGO_SET_PAT
        FROM PT_CADPAT
    ) B
    ON B.CODIGO_PAT = A.CODIGO_PAT_MOV and A.TIPO_MOV = 'A' and b.codigo_set_pat <> a.codigo_set_mov
    WHEN MATCHED THEN
        UPDATE SET
            A.CODIGO_SET_MOV = b.CODIGO_SET_PAT;""")
    commit()

    # Atualiza a data de aquisição do cadastro
    CUR_FDB.execute("""
    MERGE INTO PT_CADPAT A
    USING (
        SELECT DATA_MOV, CODIGO_PAT_MOV, CODIGO_SET_MOV
        FROM PT_MOVBEM
        WHERE TIPO_MOV = 'A'
    ) B
    ON A.CODIGO_PAT = B.CODIGO_PAT_MOV
    WHEN MATCHED THEN
        UPDATE SET
            A.datae_pat = B.DATA_MOV""")
    commit()

    # Preenche conta contábil no cadastro com base no tipo de bem
    CUR_FDB.execute("""
    MERGE
    INTO
        pt_cadpat a
            USING (
        SELECT
            a.codigo_pat,
            c.balco
        FROM
            PT_CADPAT a
        JOIN PT_CADTIP b ON
            a.CODIGO_TIP_PAT = b.codigo_tip
        JOIN conpla_tce c ON
            b.DESCRICAO_TIP = c.TITCO
        WHERE
            codigo_cpl_pat IS NULL) b
    ON a.codigo_pat = b.codigo_pat 
    WHEN MATCHED THEN UPDATE SET a.codigo_cpl_pat = b.balco
    """)
    commit()

    # Atualiza a conta contábil nas movimentações
    CUR_FDB.execute("""
    MERGE INTO PT_MOVBEM A 
    USING (SELECT CODIGO_CPL_PAT, CODIGO_PAT FROM PT_CADPAT) B 
    ON A.CODIGO_PAT_MOV = B.CODIGO_PAT AND A.CODIGO_CPL_MOV IS NULL
    WHEN MATCHED THEN UPDATE SET A.CODIGO_CPL_MOV = B.CODIGO_CPL_PAT
    """)   
    commit()

    max_codigomov = CUR_FDB.execute('SELECT max(codigo_mov) FROM pt_movbem').fetchone()[0]
    CUR_FDB.execute(f'ALTER SEQUENCE gen_pt_movbem_id RESTART with {max_codigomov}')
    commit()

    CUR_FDB.execute("""
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

    CUR_FDB.execute("""
    EXECUTE BLOCK AS
    DECLARE VARIABLE CODIGO_MOV INTEGER;
    DECLARE VARIABLE CODIGO_PAT_MOV INTEGER;
    DECLARE VARIABLE CODIGO_SET_MOV INTEGER;
    BEGIN
        FOR SELECT CODIGO_MOV, CODIGO_PAT_MOV
            FROM PT_MOVBEM
            WHERE TIPO_MOV = 'R' AND DEPRECIACAO_MOV = 'N' AND COALESCE(CODIGO_SET_MOV, 0) = 0
            INTO :CODIGO_MOV, :CODIGO_PAT_MOV
        DO
        BEGIN
            -- Busca o CODIGO_SET_MOV da movimentação anterior (ou 0 se não existir)
            SELECT COALESCE(CODIGO_SET_MOV, 0)
            FROM PT_MOVBEM
            WHERE CODIGO_MOV = :CODIGO_MOV - 1
            AND CODIGO_PAT_MOV = :CODIGO_PAT_MOV
            INTO :CODIGO_SET_MOV;

            -- Atualiza a movimentação atual
            UPDATE PT_MOVBEM
            SET CODIGO_SET_MOV = :CODIGO_SET_MOV
            WHERE CODIGO_MOV = :CODIGO_MOV;
        END
    END
    """)
    commit()