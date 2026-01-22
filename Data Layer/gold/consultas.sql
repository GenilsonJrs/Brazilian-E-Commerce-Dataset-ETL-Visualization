-- ============================================================================
-- CONSULTAS ANALÍTICAS - GOLD LAYER
-- Objetivo: Validar o Star Schema e extrair insights
-- ============================================================================

SELECT 
    t.nr_ano,
    t.nm_mes,
    COUNT(DISTINCT f.nk_ped) AS Qtd_Pedidos,
    SUM(f.vlr_ttl) AS Receita_Total
FROM gold.fat_vnd_itm f
JOIN gold.dim_tmp t ON f.sk_tmp = t.sk_tmp
GROUP BY t.nr_ano, t.nr_mes, t.nm_mes
ORDER BY t.nr_ano, t.nr_mes;

SELECT 
    t.fl_fim_semana AS Is_Weekend,
    COUNT(DISTINCT f.nk_ped) AS Total_Pedidos,
    ROUND(AVG(f.vlr_ttl), 2) AS Ticket_Medio
FROM gold.fat_vnd_itm f
JOIN gold.dim_tmp t ON f.sk_tmp = t.sk_tmp
GROUP BY t.fl_fim_semana;

SELECT 
    s.nm_sts AS Status_Pedido,
    COUNT(*) AS Qtd_Itens,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()), 2) AS Perc_Total
FROM gold.fat_vnd_itm f
JOIN gold.dim_sts s ON f.sk_sts = s.sk_sts
GROUP BY s.nm_sts
ORDER BY Qtd_Itens DESC;

SELECT 
    p.nm_cat AS Categoria,
    SUM(f.vlr_ttl) AS Receita_Bruta,
    ROUND(AVG(f.vlr_unt), 2) AS Preco_Medio_Item
FROM gold.fat_vnd_itm f
JOIN gold.dim_prd p ON f.sk_prd = p.sk_prd
GROUP BY p.nm_cat
ORDER BY Receita_Bruta DESC
LIMIT 10;

SELECT 
    p.nm_sgm_prc AS Segmento_Preco,
    ROUND(AVG(f.vlr_unt), 2) AS Media_Valor_Produto,
    ROUND(AVG(f.vlr_frt), 2) AS Media_Valor_Frete,
    ROUND((AVG(f.vlr_frt) / AVG(f.vlr_unt)) * 100, 2) AS Perc_Frete_Sobre_Produto
FROM gold.fat_vnd_itm f
JOIN gold.dim_prd p ON f.sk_prd = p.sk_prd
GROUP BY p.nm_sgm_prc
ORDER BY Perc_Frete_Sobre_Produto DESC;

WITH Vendas_Trimestrais AS (
    SELECT 
        t.nr_ano,
        t.nr_trimestre,
        v.nk_vdr AS Seller_ID,
        SUM(f.vlr_ttl) AS Total_Vendido
    FROM gold.fat_vnd_itm f
    JOIN gold.dim_vdr v ON f.sk_vdr = v.sk_vdr
    JOIN gold.dim_tmp t ON f.sk_tmp = t.sk_tmp
    GROUP BY t.nr_ano, t.nr_trimestre, v.nk_vdr
),
Ranking_Vendedores AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY nr_ano, nr_trimestre ORDER BY Total_Vendido DESC) as Posicao_Ranking
    FROM Vendas_Trimestrais
)
SELECT * FROM Ranking_Vendedores 
WHERE Posicao_Ranking <= 3
ORDER BY nr_ano, nr_trimestre, Posicao_Ranking;

WITH Receita_Mensal AS (
    SELECT 
        t.nr_ano,
        t.nr_mes,
        SUM(f.vlr_ttl) AS Receita_Atual
    FROM gold.fat_vnd_itm f
    JOIN gold.dim_tmp t ON f.sk_tmp = t.sk_tmp
    GROUP BY t.nr_ano, t.nr_mes
),
Calculo_Crescimento AS (
    SELECT 
        nr_ano,
        nr_mes,
        Receita_Atual,
        LAG(Receita_Atual) OVER (ORDER BY nr_ano, nr_mes) AS Receita_Mes_Anterior
    FROM Receita_Mensal
)
SELECT 
    nr_ano,
    nr_mes,
    Receita_Atual,
    Receita_Mes_Anterior,
    ROUND(((Receita_Atual - Receita_Mes_Anterior) / NULLIF(Receita_Mes_Anterior, 0)) * 100, 2) AS Perc_Crescimento_MoM
FROM Calculo_Crescimento
WHERE Receita_Mes_Anterior IS NOT NULL;

WITH Media_Global_Entrega AS (
    SELECT AVG(qtd_dia_ent) AS Avg_Dias_Global FROM gold.fat_vnd_itm WHERE qtd_dia_ent IS NOT NULL
),
Media_Por_Categoria AS (
    SELECT 
        p.nm_cat,
        AVG(f.qtd_dia_ent) AS Avg_Dias_Categoria,
        COUNT(*) AS Qtd_Vendas
    FROM gold.fat_vnd_itm f
    JOIN gold.dim_prd p ON f.sk_prd = p.sk_prd
    WHERE f.qtd_dia_ent IS NOT NULL
    GROUP BY p.nm_cat
)
SELECT 
    mc.nm_cat,
    ROUND(mc.Avg_Dias_Categoria, 1) AS Dias_Entrega,
    ROUND(gl.Avg_Dias_Global, 1) AS Media_Global,
    CASE 
        WHEN mc.Avg_Dias_Categoria > gl.Avg_Dias_Global * 1.2 THEN 'Crítico (>20% lento)'
        WHEN mc.Avg_Dias_Categoria > gl.Avg_Dias_Global THEN 'Acima da Média'
        ELSE 'Eficiente'
    END AS Status_Logistico
FROM Media_Por_Categoria mc
CROSS JOIN Media_Global_Entrega gl
WHERE mc.Qtd_Vendas > 100
ORDER BY Dias_Entrega DESC;

WITH Classificacao_Vendas AS (
    SELECT 
        nk_ped,
        SUM(vlr_ttl) AS Valor_Total_Pedido
    FROM gold.fat_vnd_itm
    GROUP BY nk_ped
)
SELECT 
    CASE 
        WHEN Valor_Total_Pedido < 50 THEN '1. Baixo Ticket'
        WHEN Valor_Total_Pedido BETWEEN 50 AND 200 THEN '2. Médio Ticket'
        WHEN Valor_Total_Pedido BETWEEN 200 AND 500 THEN '3. Alto Ticket'
        ELSE '4. VIP / Whale'
    END AS Segmento_Cliente,
    COUNT(*) AS Qtd_Pedidos,
    SUM(Valor_Total_Pedido) AS Receita_Segmento
FROM Classificacao_Vendas
GROUP BY 1
ORDER BY 1;


WITH Metricas_Produto AS (
    SELECT 
        p.nm_cat,
        AVG(f.vlr_unt) as Preco_Medio,
        AVG(f.vlr_frt) as Frete_Medio
    FROM gold.fat_vnd_itm f
    JOIN gold.dim_prd p ON f.sk_prd = p.sk_prd
    GROUP BY p.nm_cat
)
SELECT 
    nm_cat,
    Preco_Medio,
    Frete_Medio
FROM Metricas_Produto
WHERE Preco_Medio > (SELECT AVG(vlr_unt) FROM gold.fat_vnd_itm)
  AND Frete_Medio < (SELECT AVG(vlr_frt) FROM gold.fat_vnd_itm)
ORDER BY Preco_Medio DESC;

SELECT 
    t.dt_ref,
    SUM(f.vlr_ttl) AS Receita_Dia
FROM gold.fat_vnd_itm f
JOIN gold.dim_tmp t ON f.sk_tmp = t.sk_tmp
GROUP BY t.dt_ref
HAVING SUM(f.vlr_ttl) > (
    SELECT AVG(receita_dia) * 3 
    FROM (SELECT sk_tmp, SUM(vlr_ttl) as receita_dia FROM gold.fat_vnd_itm GROUP BY sk_tmp) sub
)
ORDER BY Receita_Dia DESC;

SELECT 
    CASE 
        WHEN Qtd_Itens > 1 THEN 'Carrinho Composto (Cross-Sell)'
        ELSE 'Venda Única'
    END AS Tipo_Venda,
    COUNT(*) AS Total_Pedidos,
    AVG(Receita_Pedido) AS Ticket_Medio
FROM (
    SELECT nk_ped, COUNT(*) as Qtd_Itens, SUM(vlr_ttl) as Receita_Pedido
    FROM gold.fat_vnd_itm
    GROUP BY nk_ped
) sub
GROUP BY 1;