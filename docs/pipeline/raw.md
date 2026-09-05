# Camada raw

A camada de origem. Guarda os arquivos **exatamente como vieram da Olist**, sem nenhuma
transformação.

## Arquivos

`Data Layer/raw/`

| Arquivo | Conteúdo |
|---|---|
| `orders.csv` | Pedidos: identificador, cliente, status e marcos temporais da compra à entrega |
| `order_items.csv` | Itens de cada pedido: produto, vendedor, preço e frete |
| `products.csv` | Catálogo: categoria e atributos físicos dos produtos |
| `dicionario_de_dados.pdf` | Dicionário do dataset original |
| `analytcs.ipynb` | Exploração inicial dos dados brutos |

## Por que versionar o CSV

Manter o dado bruto no repositório é o que torna o pipeline **reproduzível**. Sem ele, reprocessar
exigiria baixar o dataset do Kaggle de novo — e um dataset público pode mudar, sair do ar ou ter
sua licença alterada.

É também o que cumpre, aqui, o papel da camada bronze: a cópia fiel e imutável da origem.

## A exploração inicial

O notebook `analytcs.ipynb` faz o reconhecimento do terreno antes de qualquer transformação:
volume de linhas, tipos, valores ausentes e distribuições.

!!! tip "Explorar antes de transformar"
    É esse passo que revela o que a camada silver vai precisar tratar. Escrever a limpeza sem
    olhar o dado primeiro leva a regras que não correspondem ao que existe — e o erro só aparece
    depois, no meio do pipeline.
