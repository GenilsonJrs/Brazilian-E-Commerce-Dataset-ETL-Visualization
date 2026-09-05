# Brazilian E-Commerce ETL (Olist)

Pipeline de ETL utilizando a **Arquitetura de Medalhão** para análise de dados do E-commerce
Brasileiro (Olist), com modelagem dimensional em PostgreSQL e dashboard em Power BI.

**[Documentação do projeto](https://genilsonjrs.github.io/brazilian-ecommerce-etl/)** — arquitetura, camadas do pipeline, modelo dimensional e dashboard em formato navegável.

## Alunos

| Matrícula   | Aluno             |
|-------------|-------------------|
| 202045482   | [Genilson Silva de Araújo Júnior](https://github.com/GenilsonJrs)    |
| 190036427   | [Pedro Henrique Caldeira de Moraes](https://github.com/pedromoraes39)    |

## Dataset

Os dados utilizados são provenientes do [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), contendo informações de 100 mil pedidos de 2016 a 2018.

## Dashboard

![Visão Geral](docs/assets/dashboard-1.jpeg)
![Saúde Operacional](docs/assets/dashboard-2.jpeg)
![Análise Logística](docs/assets/dashboard-3.jpeg)
![Segmentação](docs/assets/dashboard-4.jpeg)

## Requirements

- **Docker** e **Docker Compose**
- **Python 3.10+**
- Gerenciador de pacotes `pip`

## Como Rodar

### 1. Subir o Banco de Dados

Certifique-se de que o Docker está rodando e execute:

```bash
docker compose up -d
```

### 2. Instalando Dependências

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Rodando Notebooks

Os notebooks devem ser executados na ordem da Arquitetura de Medalhão, localizados na pasta `Transformer/`:

1. `etl_raw_to_silver.ipynb`: carga dos CSVs brutos, limpeza, tipagem, remoção de duplicatas e colunas derivadas.
2. `etl_silver_to_gold.ipynb`: modelagem dimensional (Star Schema, tabelas fato e dimensão).

> A camada bruta é composta pelos próprios CSVs versionados em `Data Layer/raw/`, então o pipeline vai direto de raw para silver.

## Database

**Conexão:** `postgresql://admin:admin123@localhost:5432/brazilian-e-commerce`

As credenciais vêm do `docker-compose.yml` e podem ser alteradas pelas variáveis `POSTGRES_DB`, `POSTGRES_USER` e `POSTGRES_PASSWORD`. Ao alterar, ajuste também o `DB_CONFIG` no início dos notebooks.

Schemas:

- **`silver.*`**: dados limpos e padronizados.
    - `silver.sales_order_items`: tabela ampla no grão do item de pedido, com colunas derivadas (`total_item_value`, `price_segment`, `days_to_deliver`).
- **`DW.*`**: modelo dimensional (Star Schema) para análise de BI:
    - `DW.dim_prd`: produto e categoria.
    - `DW.dim_vdr`: vendedor.
    - `DW.dim_sts`: status do pedido.
    - `DW.dim_tmp`: dimensão temporal.
    - `DW.fat_vnd_itm`: tabela fato com métricas de venda e frete.

O padrão de nomenclatura está documentado em [`Data Layer/gold/mnemonico.md`](Data%20Layer/gold/mnemonico.md).

## Outros Comandos

**Verificar volume de dados carregados (Camada Silver):**

```bash
docker compose exec postgres psql -U admin -d brazilian-e-commerce -c "SELECT COUNT(*) FROM silver.sales_order_items;"
```

**Verificar a tabela fato (Camada Gold):**

```bash
docker compose exec postgres psql -U admin -d brazilian-e-commerce -c "SELECT COUNT(*) FROM DW.fat_vnd_itm;"
```
