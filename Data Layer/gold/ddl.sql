-- ============================================================================
-- Gold LAYER: DATA WAREHOUSE
-- Objetivo: Tabela Fatos e Dims para consultas de BI
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS DataWarehouse;

CREATE TABLE DataWarehouse.dim_prd (
    sk_prd          VARCHAR(32) PRIMARY KEY,
    nk_prd          VARCHAR(50),
    nm_cat          VARCHAR(100),
    nm_sgm_prc      VARCHAR(50)
);

CREATE TABLE DataWarehouse.dim_vdr (
    sk_vdr          VARCHAR(32) PRIMARY KEY,
    nk_vdr          VARCHAR(50)
);

CREATE TABLE DataWarehouse.dim_sts (
    sk_sts          VARCHAR(32) PRIMARY KEY,
    nm_sts          VARCHAR(50)
);

CREATE TABLE DataWarehouse.dim_tmp (
    sk_tmp          INT PRIMARY KEY,
    dt_ref          DATE,
    nr_ano          INT,
    nr_mes          INT,
    nm_mes          VARCHAR(20),
    nr_trimestre    INT,
    nm_dia_semana   VARCHAR(20),
    fl_fim_semana   BOOLEAN
);

CREATE TABLE DataWarehouse.fat_vnd_itm (
    sk_fat_vnd      VARCHAR(32) PRIMARY KEY,
    
    sk_prd          VARCHAR(32) REFERENCES DataWarehouse.dim_prd(sk_prd),
    sk_vdr          VARCHAR(32) REFERENCES DataWarehouse.dim_vdr(sk_vdr),
    sk_sts          VARCHAR(32) REFERENCES DataWarehouse.dim_sts(sk_sts),
    sk_tmp          INT         REFERENCES DataWarehouse.dim_tmp(sk_tmp),
    
    vlr_unt         DECIMAL(10,2),
    vlr_frt         DECIMAL(10,2),
    vlr_ttl         DECIMAL(10,2),
    qtd_dia_ent     INT
);