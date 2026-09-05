# Dashboard

Quatro painéis construídos em **Power BI** sobre a camada gold. O arquivo `.pbix` está em
`Data Visualization/`, junto com a versão em PDF.

## Visão geral

![Visão geral](assets/dashboard-1.jpeg){ .shot }

Panorama do negócio: receita, volume de pedidos e distribuição por categoria.

## Saúde operacional

![Saúde operacional](assets/dashboard-2.jpeg){ .shot }

Situação dos pedidos por status — quanto foi entregue, quanto está em trânsito, quanto foi
cancelado.

## Análise logística

![Análise logística](assets/dashboard-3.jpeg){ .shot }

Prazos de entrega e frete. Aproveita a coluna `days_to_deliver` derivada na
[camada silver](pipeline/silver.md).

## Segmentação

![Segmentação](assets/dashboard-4.jpeg){ .shot }

Distribuição por faixa de preço, usando a coluna `price_segment`, e recorte por vendedor e
categoria.

## Sobre a conexão

O Power BI lê o **schema `DW`** diretamente do PostgreSQL. Como o modelo já está em Star Schema,
a ferramenta reconhece as relações entre fato e dimensões sem precisar de transformação adicional
no Power Query.

!!! tip "Por que modelar antes de visualizar"
    Seria possível apontar o Power BI direto para a camada silver e resolver tudo no Power Query.
    Funcionaria — e concentraria a lógica de negócio dentro de um arquivo binário, invisível ao
    controle de versão e impossível de revisar.

    Com o Star Schema no banco, a regra fica em SQL versionado e o `.pbix` cuida só da
    apresentação.
