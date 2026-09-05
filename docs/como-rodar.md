# Como rodar

## Pré-requisitos

- **Docker** e **Docker Compose**
- **Python 3.10** ou superior
- `pip`

## 1. Subir o banco

```bash
docker compose up -d
```

Sobe um **PostgreSQL 15 Alpine** em contêiner, com volume persistente.

| Parâmetro | Valor padrão |
|---|---|
| Host | `localhost` |
| Porta | `5432` |
| Banco | `brazilian-e-commerce` |
| Usuário | `admin` |
| Senha | `admin123` |

```
postgresql://admin:admin123@localhost:5432/brazilian-e-commerce
```

!!! note "Credenciais configuráveis"
    O `docker-compose.yml` lê `POSTGRES_DB`, `POSTGRES_USER` e `POSTGRES_PASSWORD` do ambiente e
    cai nos valores acima quando não definidas. Se alterar, ajuste também o `DB_CONFIG` no início
    dos notebooks.

## 2. Instalar dependências

```bash
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Executar os notebooks

Em `Transformer/`, **nesta ordem**:

| Ordem | Notebook | Resultado |
|:---:|---|---|
| 1 | `etl_raw_to_silver.ipynb` | Popula `silver.sales_order_items` |
| 2 | `etl_silver_to_gold.ipynb` | Popula as dimensões e a fato no schema `DW` |

!!! warning "A ordem importa"
    O segundo notebook lê o que o primeiro escreveu. Rodar fora de ordem falha por tabela
    inexistente.

## 4. Conferir a carga

```bash
docker compose exec postgres \
  psql -U admin -d brazilian-e-commerce \
  -c "SELECT COUNT(*) FROM silver.sales_order_items;"
```

E, depois do segundo notebook:

```bash
docker compose exec postgres \
  psql -U admin -d brazilian-e-commerce \
  -c "SELECT COUNT(*) FROM DW.fat_vnd_itm;"
```

## 5. Abrir o dashboard

`Data Visualization/brazilian-e-commerce.pbix` no Power BI Desktop, apontando a conexão para o
banco local.

Quem não tem Power BI encontra a versão estática em `data-visualization.pdf` e as imagens em
[Dashboard](dashboard.md).

## Comandos úteis

```bash
docker compose down          # para o banco, preservando os dados
docker compose down -v       # para e APAGA o volume, zerando o banco
docker compose logs -f postgres
```

!!! danger "`-v` apaga os dados"
    `docker compose down -v` remove o volume. É útil para recomeçar do zero, mas exige rodar os
    dois notebooks de novo.
