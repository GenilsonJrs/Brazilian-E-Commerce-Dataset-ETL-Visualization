-- ============================================================================
-- CONSULTAS ANALÍTICAS - GOLD LAYER
-- Objetivo: Validar o Star Schema e extrair insights
-- ============================================================================

-- 1. Curva de Vendas: Receita Total por Mês/Ano
SELECT 
    t.num_ano,
    t.num_mes,
    COUNT(DISTINCT f.cod_ped) AS Qtd_Pedidos,
    SUM(f.val_tot) AS Receita_Total
FROM DW.fat_vnd_itm f
JOIN DW.dim_tmp t ON f.srk_tmp = t.srk_tmp
GROUP BY t.num_ano, t.num_mes, t.nam_mes
ORDER BY t.num_ano, t.num_mes;

-- 2. "Efeito Fim de Semana": Comportamento de Compra
SELECT 
    t.flg_fim_sem AS Is_Weekend,
    COUNT(DISTINCT f.cod_ped) AS Total_Pedidos,
    ROUND(AVG(f.val_tot), 2) AS Ticket_Medio
FROM DW.fat_vnd_itm f
JOIN DW.dim_tmp t ON f.srk_tmp = t.srk_tmp
GROUP BY t.flg_fim_sem;;

-- 3. Status de Entrega
SELECT 
    s.nam_sts AS Status_Pedido,
    COUNT(*) AS Qtd_Itens,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()), 2) AS Perc_Total
FROM DW.fat_vnd_itm f
JOIN DW.dim_sts s ON f.srk_sts = s.srk_sts
GROUP BY s.nam_sts
ORDER BY Qtd_Itens DESC;

-- 4. Ranking de Categorias
SELECT 
    p.nam_cat AS Categoria,
    SUM(f.val_tot) AS Receita_Bruta,
    ROUND(AVG(f.val_uni), 2) AS Preco_Medio_Item
FROM DW.fat_vnd_itm f
JOIN DW.dim_prd p ON f.srk_prd = p.srk_prd
GROUP BY p.nam_cat
ORDER BY Receita_Bruta DESC
LIMIT 10;

-- 5. Análise de Custo Logístico (Frete vs Produto)
SELECT 
    p.nam_sgm_prc AS Segmento_Preco,
    ROUND(AVG(f.val_uni), 2) AS Media_Valor_Produto,
    ROUND(AVG(f.val_frt), 2) AS Media_Valor_Frete,
    ROUND((AVG(f.val_frt) / AVG(f.val_uni)) * 100, 2) AS Perc_Frete_Sobre_Produto
FROM DW.fat_vnd_itm f
JOIN DW.dim_prd p ON f.srk_prd = p.srk_prd
GROUP BY p.nam_sgm_prc
ORDER BY Perc_Frete_Sobre_Produto DESC;

-- 6. CTE + Window Function: Top 3 Vendedores por Trimestre
WITH Vendas_Trimestrais AS (
    SELECT 
        t.num_ano,
        t.num_tri,
        v.cod_vdr AS Seller_ID,
        SUM(f.val_tot) AS Total_Vendido
    FROM DW.fat_vnd_itm f
    JOIN DW.dim_vdr v ON f.srk_vdr = v.srk_vdr
    JOIN DW.dim_tmp t ON f.srk_tmp = t.srk_tmp
    GROUP BY t.num_ano, t.num_tri, v.cod_vdr
),
Ranking_Vendedores AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY num_ano, num_tri ORDER BY Total_Vendido DESC) as Posicao_Ranking
    FROM Vendas_Trimestrais
)
SELECT * FROM Ranking_Vendedores 
WHERE Posicao_Ranking <= 3
ORDER BY num_ano, num_tri, Posicao_Ranking;

-- 7. CTE: Crescimento Mês a Mês (MoM - Month over Month)
WITH Receita_Mensal AS (
    SELECT 
        t.num_ano,
        t.num_mes,
        SUM(f.val_tot) AS Receita_Atual
    FROM DW.fat_vnd_itm f
    JOIN DW.dim_tmp t ON f.srk_tmp = t.srk_tmp
    GROUP BY t.num_ano, t.num_mes
),
Calculo_Crescimento AS (
    SELECT 
        num_ano,
        num_mes,
        Receita_Atual,
        LAG(Receita_Atual) OVER (ORDER BY num_ano, num_mes) AS Receita_Mes_Anterior
    FROM Receita_Mensal
)
SELECT 
    num_ano,
    num_mes,
    Receita_Atual,
    Receita_Mes_Anterior,
    ROUND(((Receita_Atual - Receita_Mes_Anterior) / NULLIF(Receita_Mes_Anterior, 0)) * 100, 2) AS Perc_Crescimento_MoM
FROM Calculo_Crescimento
WHERE Receita_Mes_Anterior IS NOT NULL;

-- 8. CTE: Análise de SLA Logístico (Gargalos de Entrega)
WITH Media_Global_Entrega AS (
    SELECT AVG(qtd_dia_ent) AS Avg_Dias_Global FROM DW.fat_vnd_itm WHERE qtd_dia_ent IS NOT NULL
),
Media_Por_Categoria AS (
    SELECT 
        p.nam_cat,
        AVG(f.qtd_dia_ent) AS Avg_Dias_Categoria,
        COUNT(*) AS Qtd_Vendas
    FROM DW.fat_vnd_itm f
    JOIN DW.dim_prd p ON f.srk_prd = p.srk_prd
    WHERE f.qtd_dia_ent IS NOT NULL
    GROUP BY p.nam_cat
)
SELECT 
    mc.nam_cat,
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

-- 9. CTE: Segmentação de Clientes (Ticket Médio vs Frequência)
WITH Classificacao_Vendas AS (
    SELECT 
        cod_ped,
        SUM(val_tot) AS Valor_Total_Pedido
    FROM DW.fat_vnd_itm
    GROUP BY cod_ped
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

-- 10. CTE + Subquery: Produtos "Estrela" (Alta Receita e Baixo Frete)
WITH Metricas_Produto AS (
    SELECT 
        p.nam_cat,
        AVG(f.val_uni) as Preco_Medio,
        AVG(f.val_frt) as Frete_Medio
    FROM DW.fat_vnd_itm f
    JOIN DW.dim_prd p ON f.srk_prd = p.srk_prd
    GROUP BY p.nam_cat
)
SELECT 
    nam_cat,
    Preco_Medio,
    Frete_Medio
FROM Metricas_Produto
WHERE Preco_Medio > (SELECT AVG(val_uni) FROM DW.fat_vnd_itm)
  AND Frete_Medio < (SELECT AVG(val_frt) FROM DW.fat_vnd_itm)
ORDER BY Preco_Medio DESC;

-- 11. Dias de Pico
SELECT 
    t.dat_ref,
    SUM(f.val_tot) AS Receita_Dia
FROM DW.fat_vnd_itm f
JOIN DW.dim_tmp t ON f.srk_tmp = t.srk_tmp
GROUP BY t.dat_ref
HAVING SUM(f.val_tot) > (
    SELECT AVG(receita_dia) * 3 
    FROM (SELECT srk_tmp, SUM(val_tot) as receita_dia FROM DW.fat_vnd_itm GROUP BY srk_tmp) sub
)
ORDER BY Receita_Dia DESC;

-- 12. Vendas Compostas
SELECT 
    CASE 
        WHEN Qtd_Itens > 1 THEN 'Carrinho Composto'
        ELSE 'Venda Única'
    END AS Tipo_Venda,
    COUNT(*) AS Total_Pedidos,
    AVG(Receita_Pedido) AS Ticket_Medio
FROM (
    SELECT cod_ped, COUNT(*) as Qtd_Itens, SUM(val_tot) as Receita_Pedido
    FROM DW.fat_vnd_itm
    GROUP BY cod_ped
) sub
GROUP BY 1;