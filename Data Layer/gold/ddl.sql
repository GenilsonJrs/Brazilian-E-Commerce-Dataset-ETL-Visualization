-- ============================================================================
-- Gold LAYER: DATA WAREHOUSE
-- Objetivo: Tabela Fatos e Dims para consultas de BI
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE gold.dim_prd (
    srk_prd          VARCHAR(32) PRIMARY KEY,
    cod_prd          VARCHAR(50),
    nam_cat          VARCHAR(100),
    nam_sgm_prc      VARCHAR(50)
);

CREATE TABLE gold.dim_vdr (
    srk_vdr          VARCHAR(32) PRIMARY KEY,
    cod_vdr          VARCHAR(50)
);

CREATE TABLE gold.dim_sts (
    srk_sts          VARCHAR(32) PRIMARY KEY,
    nam_sts          VARCHAR(50)
);

CREATE TABLE gold.dim_tmp (
    srk_tmp          INT PRIMARY KEY,
    dat_ref          DATE,
    num_ano          INT,
    num_mes          INT,
    nam_mes          VARCHAR(20),
    num_tri          INT,
    nam_dia_sem      VARCHAR(20),
    flg_fim_sem      BOOLEAN
);

CREATE TABLE gold.fat_vnd_itm (
    srk_fat_vnd      VARCHAR(32) PRIMARY KEY,   
    srk_prd          VARCHAR(32) REFERENCES gold.dim_prd(srk_prd),
    srk_vdr          VARCHAR(32) REFERENCES gold.dim_vdr(srk_vdr),
    srk_sts          VARCHAR(32) REFERENCES gold.dim_sts(srk_sts),
    srk_tmp          INT         REFERENCES gold.dim_tmp(srk_tmp),    
    cod_ped          VARCHAR(50),     
    val_uni          DECIMAL(10,2),
    val_frt          DECIMAL(10,2),
    val_tot          DECIMAL(10,2),
    qtd_dia_ent      INT
);