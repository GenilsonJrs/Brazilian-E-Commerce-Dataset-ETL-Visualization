# Dicionário de Mnemônicos - Gold

Este guia consolida os padrões léxicos e as diretrizes de padronização de nomenclatura aplicadas à arquitetura multidimensional (Star Schema) na etapa final do pipeline de dados (Gold Layer).

---

## 1. Abreviações de Tabelas

| Abreviação | Significado | Tabela Completa |
|------------|-------------|-----------------|
| `prd` | **Pr**o**d**uto | `gold.dim_prd` |
| `vdr` | **V**ende**d**or | `gold.dim_vdr` |
| `sts` | **St**atu**s** | `gold.dim_sts` |
| `tmp` | **T**e**mp**o | `gold.dim_tmp` |
| `vnd` | **Vnd**a (Fato) | `gold.fat_vnd_itm` |

---

## 2. Prefixos de Tabelas

| Prefixo | Significado | Exemplo |
|---------|-------------|---------|
| `dim_` | **Dim**ensão (tabela dimensional) | `dim_prd`, `dim_vdr`, `dim_tmp` |
| `fat_` | **Fat**o (tabela fato) | `fat_vnd_itm` |
| `vw_` | **V**ie**w** (visão analítica) | `vw_daily_sales` |
| `idx_` | **Idx** (índice de performance) | `idx_gold_sales_date` |

---

## 3. Identificadores de Chaves

| Sigla | Significado | Uso |
|-------|-------------|-----|
| `srk_` | **S**urrogate **R**eference **K**ey | Prefixo para Chaves Primárias (PK) e Estrangeiras (FK) via Hash MD5 ou Inteiro. |
| `cod_` | **Cód**igo Original | Prefixo para IDs naturais vindos da origem (Natural Keys). |

**Exemplos:**
- `srk_prd` - Chave primária da tabela `gold.dim_prd`.
- `srk_prd` - Chave estrangeira na `gold.fat_vnd_itm` que referencia a dimensão de produtos.

---

## 4. Abreviações de Colunas

| Abreviação | Significado | Colunas Relacionadas |
|------------|-------------|----------------------|
| `nam_` | **Nam**e (Nome/Descrição) | `nam_cat`, `nam_mes`, `nam_grp_sts`. |
| `val_` | **Val**or Monetário | `val_uni`, `val_frt`, `val_tot`. |
| `qtd_` | **Qtd** (Quantidade/Medida) | `qtd_dia_ent`. |
| `num_` | **Núm**ero (Sequencial/Ref) | `num_ano`, `num_mes`, `num_tri`. |
| `dat_` | **Dat**a | `dat_ref`. |
| `flg_` | **Flg** (Flag/Booleano) | `flg_fim_sem`, `flg_fin`. |

---

## 5. Estrutura das Tabelas

### 5.1 Dimensão Produto (`gold.dim_prd`)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `srk_prd` | VARCHAR(32) | Chave primária surrogate (Hash MD5). |
| `cod_prd` | VARCHAR(50) | ID original do produto (Product ID). |
| `nam_cat` | VARCHAR(100) | Nome da categoria tratada. |
| `nam_sgm_prc` | VARCHAR(50) | Segmento de preço (Budget, Standard, etc.). |

### 5.2 Dimensão Tempo (`gold.dim_tmp`)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `srk_tmp` | INTEGER | Chave primária surrogate (YYYYMMDD). |
| `dat_ref` | DATE | Data de referência da transação. |
| `num_ano` | INTEGER | Ano da venda (ex: 2018). |
| `num_mes` | INTEGER | Número do mês (1-12). |
| `nam_mes` | VARCHAR(20) | Nome do mês por extenso. |
| `num_tri` | INTEGER | Número do trimestre (1-4). |
| `nam_dia_sem` | VARCHAR(20) | Nome do dia da semana. |
| `flg_fim_sem` | BOOLEAN | Indica se é sábado ou domingo. |

### 5.3 Dimensão Status (`gold.dim_sts`)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `srk_sts` | VARCHAR(32) | Chave primária surrogate (Hash MD5). |
| `nam_sts` | VARCHAR(50) | Nome do status original (ex: 'delivered'). |
| `nam_grp_sts` | VARCHAR(50) | Agrupamento lógico (Sucesso, Insucesso). |
| `flg_fin` | BOOLEAN | Indica se o ciclo do pedido foi finalizado. |

### 5.4 Dimensão Vendedor (`gold.dim_vdr`)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `srk_vdr` | VARCHAR(32) | Chave primária surrogate (Hash MD5). |
| `cod_vdr` | VARCHAR(50) | ID original do vendedor (Seller ID). |

### 5.5 Fato Vendas (`gold.fat_vnd_itm`)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `srk_fat_vnd` | VARCHAR(32) | Chave primária surrogate da fato. |
| `srk_prd` | VARCHAR(32) | FK para gold.dim_prd. |
| `srk_vdr` | VARCHAR(32) | FK para gold.dim_vdr. |
| `srk_sts` | VARCHAR(32) | FK para gold.dim_sts. |
| `srk_tmp` | INTEGER | FK para gold.dim_tmp. |
| `cod_ped` | VARCHAR(50) | ID original do pedido (Dimensão Degenerada). |
| `val_uni` | DECIMAL(10,2) | Valor unitário do item. |
| `val_frt` | DECIMAL(10,2) | Valor do frete do item. |
| `val_tot` | DECIMAL(10,2) | Valor total (Preço + Frete). |
| `qtd_dia_ent` | INTEGER | Lead time de entrega em dias. |

---

## 6. Views Analíticas

| View | Descrição |
|------|-----------|
| `vw_daily_sales` | Resumo diário de pedidos e faturamento acumulado. |
| `vw_product_category_performance` | Desempenho de vendas por categoria de produto. |

---

## 7. Convenções Gerais

1. **Nomes em português** (sem acentos) para colunas de negócio e atributos qualitativos.
2. **Snake_case** para todos os identificadores do banco de dados.
3. **Prefixos obrigatórios** (`srk_`, `nam_`, `val_`, etc.) conforme a tipagem do dado.
4. **Precisão Monetária** fixada em `DECIMAL(10,2)` para assegurar exatidão financeira.
5. **Chaves Surrogate** padronizadas em `VARCHAR(32)` para suportar Hashes MD5.

---

## 8. Diagrama do Star Schema



```text
                    +---------------+
                    |     dim_tmp   |
                    +---------------+
                    | srk_tmp (PK)  |
                    | dat_ref       |
                    | num_ano, mês  |
                    | ...           |
                    +-------+-------+
                            |
                            | srk_tmp (FK)
                            v
+---------------+    +---------------+    +---------------+
|     dim_prd   |    |   fat_vnd_itm |    |     dim_sts   |
+---------------+    +---------------+    +---------------+
| srk_prd (PK)  |<---| srk_prd (FK)  |    | srk_sts (PK)  |
| cod_prd       |    | srk_vdr (FK)  |--->| nam_sts       |
| nam_cat       |    | srk_sts (FK)  |    | nam_grp_sts   |
| nam_sgm_prc   |    | srk_tmp (FK)  |    +---------------+
+---------------+    |---------------|
                     | val_tot       |
                     | qtd_dia_ent   |
                     +-------+-------+
                                 |
                                 | srk_vdr (FK)
                                 v
                        +---------------+
                        |     dim_vdr   |
                        +---------------+
                        | srk_vdr (PK)  |
                        | cod_vdr       |
                        +---------------+
```
---

*Documento gerado para o projeto Brazilian E-Commerce Dataset ETL & Visualization*