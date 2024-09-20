# TP1 - Banco de Dados I - 2024/2

Atividade solicitada como avaliação parcial da disciplina de Banco de Dados I.

## Autoras

- Maria Gabriela Morais de Sá - 22250537 - maria.morais@icomp.ufam.edu.br
- Maria Giovanna Gonçalves Sales - 22251138 - maria.sales@icomp.ufam.edu.br
- Juíle Yoshie Sarkis Hanada - 22251135 - juile.hanada@icomp.ufam.edu.br

## Descrição

Objetivo deste trabalho prático é projetar e implementar um banco de dados sobre produtos vendidos em uma loja de comércio eletrônico, incluindo avaliações e comentários de usuários sobre estes produtos.

O trabalho consiste na criação de um Banco de Dados Relacional contendo dados sobre compras de produtos (script `tp1_3.2.py`) e elaboração de um Dashboard (script `tp1_3.3.py`).

O arquivo de entrada vem do dataset **Amazon product co-purchasing network metadata**, que contém metadados de produtos da Amazon, incluindo suas avaliações e produtos similares (_co-purchasing_). Disponível em: https://snap.stanford.edu/data/amazon-meta.html .

O trabalho inclui também a documentação apresentando um diagrama correspondendo ao esquema do Banco de Dados Relacional, além de um dicionário de dados descrevendo relações, atributos e restrições do esquema (`tp1_3.1.pdf`).

## Getting Started

### Dependências

- Python
- PostgreSQL
- re
- psycopg2

Caso deseje utilizar a versão do banco de dados Postgres com o `docker-compose.yml` fornecido, é necessário também:

- Docker
- Docker-compose

### Instalação

1. Clone o projeto

```
$ git clone https://github.com/juhanada/bd_tp1_gabriela_giovanna_juile.git](https://github.com/bd1-icomp-ufam/trabalho-pr-tico-i-bancos-de-dados-1-tp1-gabriela-giovanna-juile
```

2. Faça o download do arquivo de entrada no link https://snap.stanford.edu/data/amazon-meta.html

3. Adicione o arquivo baixado (`amazon-meta.txt`) no folder `input`

4. Caso opte por utilizar o PostgreSQL no container Docker, execute o comando para rodar o container. Caso contrário, pule para a etapa 5.

```
$ sudo docker-compose up
```

5. Altere as variáveis de ambiente que estão no início dos scripts `tp1_3.2.py` e `tp1_3.3.py` de acordo com a configuração do seu banco de dados. Mantenha os valores default caso esteja utilizando o Postgres com o `docker-compose.yml` fornecido.

| Nome    | Descrição                      | Default     |
| ------- | ------------------------------ | ----------- |
| DB_HOST | Nome do host do banco de dados | `localhost` |
| DB_PORT | Porta para conexão com o banco | `5437`      |
| DB_NAME | Nome do database               | `database`  |
| DB_USER | Username do Banco de Dados     | `username`  |
| DB_PASS | Senha do Banco de Dados        | `password`  |

### Execução

1. Execute o script `tp1_3.2.py` para extrair os dados do arquivo e popular o banco de dados. Esta etapa pode demorar um tempo.

```
$ python3 tp1_3.2.py
```

2. Execute o script `tp1_3.3.py` para iniciar o dashboard e realizar as consultas.

```
$ python3 tp1_3.3.py
```
