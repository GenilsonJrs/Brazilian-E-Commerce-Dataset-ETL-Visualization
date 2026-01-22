# Dicionario de Mnemonicos - Gold Layer

Este documento define todas as abreviacoes e convencoes de nomenclatura utilizadas no Star Schema da camada Gold.

---

## 1. Prefixos de Tabelas

| Tipo | Prefixo | Descrição |
|------|---------|-----------|
| Fato | FAT_ | Tabela central contendo métricas e chaves estrangeiras. |
| Dimensão | DIM_ | Tabela descritiva contendo atributos do negócio. |

---

## 2. Sufixos e Prefixos de Colunas

| Tipo | Sigla | Descrição |
|------|-------|-----------|
| Surrogate Key | SK_ | Chave primária artificial (hash ou sequencial) do DW. |
| Natural Key | NK_ | Chave original do sistema transacional (Raw/Silver). |
| Nome/Descrição | NM_ | Texto descritivo (Ex: Nome do Produto). |
| Valor Monetário | VLR_ | Métricas financeiras (R$). |
| Quantidade | QTD_ | Métricas de contagem. |
| Data | DT_ | Campos de data. |
| Código/Flag | CD_ | Códigos categoricos ou flags booleanas. |

---

## 3. Dicionário de Termos

| Termo de Negócio | Mnemônico | Exemplo de Uso |
|------------------|-----------|----------------|
| Venda | VND | FAT_VND_ITM (Fato Venda Item) |
| Produto | PRD | DIM_PRD (Dimensão Produto) |
| Tempo/Calendário | TMP | DIM_TMP (Dimensão Tempo) |
| Status | STS | DIM_STS (Dimensão Status) |
| Vendedor | VDR | DIM_VDR (Dimensão Vendedor) |
| Categoria | CAT | NM_CAT (Nome Categoria) |
| Frete | FRT | VLR_FRT (Valor Frete) |

---

## 4. Diagrama do Star Schema

```
[DIM_PRD]              [DIM_STS]
       sk_prd (PK) <--------+ sk_sts (PK)
       nk_prd               | nm_sts
       nm_cat               |
                            |
                     [FAT_VND_ITM]
                     sk_fat_vnd (PK)
                     nk_ped
                     vlr_ttl
                     ...
                     sk_prd (FK)
                     sk_sts (FK)
           +-------> sk_vdr (FK) <-------+
           |         sk_tmp (FK)         |
           |                             |
       [DIM_VDR]                      [DIM_TMP]
       sk_vdr (PK)                    sk_tmp (PK)
       nk_vdr                         nr_ano
                                      nm_mes
                                      fl_fim_sem
```

---

*Documento gerado para o projeto Brazilian E-Commerce Dataset ETL & Visualization*