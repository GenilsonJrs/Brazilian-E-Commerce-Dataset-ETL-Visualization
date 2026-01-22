# Dicionario de Mnemonicos - Gold Layer

Este documento define todas as abreviacoes e convencoes de nomenclatura utilizadas no Star Schema da camada Gold.

---

## 1. Prefixos de Tabelas

| Tipo | Prefixo | Descrição |
|------|---------|-----------|
| Fato | FAT | Tabela central contendo métricas e chaves estrangeiras. |
| Dimensão | DIM | Tabela descritiva contendo atributos do negócio. |

---

## 2. Sufixos e Prefixos de Colunas

| Tipo | Sigla | Descrição |
|------|-------|-----------|
| Surrogate Key | SRK | Chave primária artificial (hash ou sequencial) do DW. |
| Codigo Original | COD | Chave original do sistema transacional (Raw/Silver). |
| Nome/Descrição | NAM | Texto descritivo (Ex: Nome do Produto). |
| Valor Monetário | VAL | Métricas financeiras (R$). |
| Quantidade | QTD | Métricas de contagem. |
| Data | DAT | Campos de data. |
| Código/Flag | FLG | Códigos categoricos ou flags booleanas. |

---

## 3. Dicionário de Termos

| Termo | Sigla | Contexto |
|-------|-------|----------|
| Venda | VND | Fato Vendas |
| Item | ITM | Item do Pedido |
| Pedido| PED | Order ID |
| Produto| PRD | Product ID |
| Vendedor| VDR | Seller ID |
| Categoria| CAT | Categoria do Produto |
| Segmento| SGM | Segmento de Preço |
| Preço | PRC | Preço Unitário |
| Status | STS | Status do Pedido |
| Tempo | TMP | Dimensão Calendário |
| Frete | FRT | Valor do Frete |
| Total | TOT | Valor Total |
| Entrega | ENT | Entrega Logística |
| Unidade | UNI | Valor Unitário |
| Semana | SEM | Semana do ano |
| Trimestre| TRI | Trimestre fiscal |
| Fim | FIM | Fim de semana |

---

## 4. Diagrama do Star Schema

```
[DIM_PRD]              [DIM_STS]
       srk_prd (PK) <--------+ srk_sts (PK)
       nk_prd               | nm_sts
       nm_cat               |
                            |
                     [FAT_VND_ITM]
                     srk_fat_vnd (PK)
                     nk_ped
                     vlr_ttl
                     ...
                     srk_prd (FK)
                     srk_sts (FK)
           +-------> srk_vdr (FK) <-------+
           |         srk_tmp (FK)         |
           |                             |
       [DIM_VDR]                      [DIM_TMP]
       srk_vdr (PK)                    srk_tmp (PK)
       nk_vdr                         nr_ano
                                      nm_mes
                                      fl_fim_sem
```

---

*Documento gerado para o projeto Brazilian E-Commerce Dataset ETL & Visualization*