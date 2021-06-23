# Pipeline de Dados

## Requisitos

* Docker 20.10.x ou superior.
* Docker Compose 1.28.x ou superior.
* Conhecimentos básicos com *containers*.

## Inicialização dos Serviços

* Utilize o comando (na raiz do projeto):

```bash
docker-compose up -d
```

* Para visualização dos serviços:

```bash
docker-compose ps
```

Importante que todos os serviços estejam com o estado (state) *healthy*.

## Observações

A indicação das portas que os serviços estarão disponíveis, bem como, as informações de autenticação, como usuário e senha, estão definidos no arquivos **docker-compose.yml**.
