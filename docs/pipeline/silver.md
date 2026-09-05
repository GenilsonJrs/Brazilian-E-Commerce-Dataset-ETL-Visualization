# Camada silver

Onde o dado vira confiável. Lê os CSVs brutos, resolve qualidade e carrega no PostgreSQL.

**Notebook:** `Transformer/etl_raw_to_silver.ipynb`

## O que a etapa faz

| Tratamento | Efeito |
|---|---|
| **Junção das três fontes** | Pedidos, itens e produtos consolidados em uma visão única |
| **Tipagem** | Datas viram `TIMESTAMP`, valores monetários viram `DECIMAL(10,2)` |
| **Remoção de duplicatas** | Chave sintética por item de pedido garante unicidade |
| **Colunas derivadas** | Valor total do item, segmento de preço e prazo de entrega |

## A tabela resultante

`silver.sales_order_items` — uma **tabela ampla**, com o grão no **item de pedido**.

| Coluna | Tipo | Observação |
|---|---|---|
| `sk_order_item` | `VARCHAR(32)` | Chave primária sintética |
| `order_id` | `VARCHAR(32)` | Pedido de origem |
| `order_item_id` | `INTEGER` | Sequencial do item dentro do pedido |
| `product_id` | `VARCHAR(32)` | Produto |
| `seller_id` | `VARCHAR(32)` | Vendedor |
| `price` | `DECIMAL(10,2)` | Preço do item |
| `freight_value` | `DECIMAL(10,2)` | Frete do item |
| `total_item_value` | `DECIMAL(10,2)` | **Derivada** — preço mais frete |
| `product_category_name` | `VARCHAR(100)` | Categoria do produto |
| `price_segment` | `VARCHAR(50)` | **Derivada** — faixa de preço |
| `order_status` | `VARCHAR(50)` | Status do pedido |
| `order_purchase_timestamp` | `TIMESTAMP` | Momento da compra |
| `days_to_deliver` | `DECIMAL(10,2)` | **Derivada** — prazo real de entrega |
| `created_at` | `TIMESTAMP` | Carimbo de carga, com `DEFAULT NOW()` |

## Por que uma tabela ampla

A silver não normaliza. Ela **desnormaliza de propósito**: junta as três fontes e deixa tudo em
uma linha por item.

!!! tip "O grão define tudo"
    Fixar o grão no **item de pedido** — e não no pedido — é a decisão mais consequente da camada.
    É ela que permite analisar preço, frete e categoria por item; se o grão fosse o pedido, essas
    métricas precisariam ser agregadas antes e a informação por produto se perderia.

As três colunas derivadas resolvem, de uma vez, cálculos que apareceriam repetidamente depois:

- **`total_item_value`** evita somar preço e frete em toda consulta
- **`price_segment`** transforma um valor contínuo em categoria analisável
- **`days_to_deliver`** converte dois marcos temporais na métrica que interessa

## Artefatos

| Arquivo | Conteúdo |
|---|---|
| `Data Layer/silver/ddl.sql` | Definição da tabela |
| `Data Layer/silver/mer_der_dld_silver.pdf` | Diagrama entidade-relacionamento |
| `Data Layer/silver/analytcs.ipynb` | Análise sobre o dado já tratado |
