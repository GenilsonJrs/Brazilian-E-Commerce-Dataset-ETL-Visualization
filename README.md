# Brazilian E-Commerce ETL (Olist)

Pipeline de ETL utilizando a **Arquitetura de Medalhão** (Bronze, Silver e Gold) para análise de dados do E-commerce Brasileiro (Olist).

## Alunos

| Matrícula   | Aluno             |
|-------------|-------------------|
| 202045482   | [Genilson Silva de Araújo Junior](https://github.com/GenilsonJrs)    |
| 190036427   | [Pedro Henrique Caldeira de Moraes](https://github.com/pedromoraes39)    |

## Dataset

Os dados utilizados são provenientes do [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), contendo informações de 100 mil pedidos de 2016 a 2018.

## Dashboard

![Visão Geral](Data%20Visualization/img/1.jpeg)
![Saúde Operacional](Data%20Visualization/img/1.jpeg)
![Análise Logística](Data%20Visualization/img/1.jpeg)
![Segmentação](Data%20Visualization/img/1.jpeg)

## Requirements

- **Docker** e **Docker Compose**
- **Python 3.8+**
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

Os notebooks devem ser executados na ordem da Arquitetura de Medalhão, localizados na pasta Transformer/:

1. `extract_to_bronze.ipynb`: Carga dos dados brutos do CSV para o PostgreSQL.
2. `bronze_to_silver.ipynb`: Limpeza, tratamento de tipos e remoção de duplicatas.
3. `silver_to_gold.ipynb`: Modelagem dimensional (Tabelas Fato e Dimensão).

## Database

**Conexão:** `postgresql://postgres:postgres@localhost:5432/brazilian-e-commerce`

Schemas:
- **`silver.*`**: Contém os dados limpos e padronizados (ex: `silver.orders`, `silver.products`, `silver.customers`).
- **`gold.*`**: Modelo Dimensional (Star Schema) para análise de BI:
    - `dw.dim_products`: Atributos dos produtos.
    - `dw.dim_customers`: Localização e identificação de clientes.
    - `dw.dim_sellers`: Dados dos vendedores.
    - `dw.dim_time`: Dimensão temporal para análise de tendências.
    - `dw.fact_orders`: Tabela fato com métricas de vendas e fretes.

## Outros Comandos

**Verificar volume de dados carregados (Camada Silver):**
```bash
docker compose exec postgres psql -U postgres -d olist_db -c "SELECT COUNT(*) FROM silver.orders;"
```