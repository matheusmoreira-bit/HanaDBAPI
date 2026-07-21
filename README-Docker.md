# Hana API — Docker

Preparação Docker para o `Hana API`.

> A versão em produção via prompt/PowerShell pode continuar na porta `8000`. O Docker publica a API em `8001` para rodar em paralelo sem conflito.

## Capacidade e protecoes

- 2 workers Uvicorn e ate 10 consultas simultaneas no total.
- Fila de 25 consultas por worker, com espera maxima de 30 segundos.
- Pool HANA de 4 conexoes fixas + 2 excedentes por worker; uma conexao fica de margem para o health check.
- Rate limit desativado.
- `limit=10000` por padrao e maximo de 10000 registros por consulta.
- Auditoria SQLite em modo WAL, com retencao de 30 dias e limpeza horaria.
- Container sem limites especificos de CPU, memoria ou processos/threads.
- Health check profundo, incluindo a conexao com o HANA.
- Credenciais aceitas somente em headers: `Authorization: Bearer`, `X-SAP-Session-ID` e `X-SAP-Route-ID`.
- Cache de metadados por 5 minutos e cache de validacao SAP por 30 segundos.
- Validacao SAP com timeout de conexao/leitura de 10/30 segundos, cache de 30 segundos, retry para timeout/429/5xx e circuit breaker.
- Validacao de Session ID temporariamente desativada por `sap_session_validation_enabled=false`; o token dinamico continua obrigatorio.
- Timeout HANA de 30 segundos e cancelamento quando o cliente desconecta.
- Auditoria assincrona em fila de 5000 itens, gravada em lotes de ate 100.
- Metricas locais de cada worker disponiveis em `/metrics`; consultas acima de 5 segundos geram alerta no log.

## Porta

- Host Docker: `8001`
- Container: `8000`
- Serviço atual via prompt: `8000`

## Arquivos

- `Dockerfile` — imagem Python com dependências do `requirements.txt`.
- `docker-compose.yml` — serviço `hana-api`, porta `8000:8000`, volumes de `config` e `logs`.
- `.dockerignore` — evita incluir `.venv`, caches, logs e bancos locais na imagem.

## Volumes

O Compose mantém fora da imagem:

- `./config:/app/config:ro`
- `./logs:/app/logs`

O SQLite do log de execuções ficará em:

```text
./logs/executions.db
```

## Quando for subir via Docker

```powershell
docker-compose -f "C:\Users\adm_anagaming\Documents\Hana API\HanaDBAPI\docker-compose.yml" --project-directory "C:\Users\adm_anagaming\Documents\Hana API\HanaDBAPI" up -d --build
```

Acesse em:

```text
http://localhost:8001/health
http://localhost:8001/databases
```

## Parar

```powershell
docker-compose -f "C:\Users\adm_anagaming\Documents\Hana API\HanaDBAPI\docker-compose.yml" --project-directory "C:\Users\adm_anagaming\Documents\Hana API\HanaDBAPI" down
```

## Observação

O Compose foi configurado para `8001:8000`, então não deve conflitar com o serviço já rodando na porta `8000`.
