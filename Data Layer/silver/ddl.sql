-- ============================================================================
-- SILVER LAYER: TABELA SALES_ORDER_ITEMS (OLIST)
-- Objetivo: Tabela Fato Desnormalizada (One Big Table) com granularidade de Item
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.sales_order_items CASCADE;

CREATE TABLE silver.sales_order_items (
    sk_order_item VARCHAR(32) PRIMARY KEY,
    order_id VARCHAR(32) NOT NULL,
    order_item_id INTEGER,
    product_id VARCHAR(32),
    seller_id VARCHAR(32),
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    total_item_value DECIMAL(10,2),
    product_category_name VARCHAR(100),
    price_segment VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    days_to_deliver DECIMAL(10,2),

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_silver_sales_order_id ON silver.sales_order_items(order_id);
CREATE INDEX idx_silver_sales_date ON silver.sales_order_items(order_purchase_timestamp);
CREATE INDEX idx_silver_sales_product_category_name ON silver.sales_order_items(product_category_name);
CREATE INDEX idx_silver_sales_seller ON silver.sales_order_items(seller_id);

COMMENT ON TABLE silver.sales_order_items IS 'Tabela fato desnormalizada de itens vendidos';
COMMENT ON COLUMN silver.sales_order_items.sk_order_item IS 'Chave substituta (Hash MD5) de Pedido + Item';
COMMENT ON COLUMN silver.sales_order_items.order_id IS 'Identificador original do pedido';
COMMENT ON COLUMN silver.sales_order_items.order_item_id IS 'Número sequencial do item dentro do pedido';
COMMENT ON COLUMN silver.sales_order_items.price IS 'Preço unitário do produto (sem frete)';
COMMENT ON COLUMN silver.sales_order_items.total_item_value IS 'Valor total pago (Preço + Frete)';
COMMENT ON COLUMN silver.sales_order_items.price_segment IS 'Segmentação de preço baseada em quartis (Budget, Premium...)';
COMMENT ON COLUMN silver.sales_order_items.days_to_deliver IS 'Dias corridos entre compra e entrega ao cliente';

CREATE OR REPLACE VIEW silver.vw_daily_sales AS
SELECT 
    DATE(order_purchase_timestamp) as sale_date,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(total_item_value) as total_revenue,
    AVG(days_to_deliver) as avg_delivery_time
FROM silver.sales_order_items
GROUP BY 1
ORDER BY 1 DESC;

CREATE OR REPLACE VIEW silver.vw_product_category_name_performance AS
SELECT 
    product_category_name,
    COUNT(*) as total_items_sold,
    SUM(total_item_value) as total_revenue,
    AVG(price) as avg_ticket
FROM silver.sales_order_items
GROUP BY 1
ORDER BY 2 DESC;