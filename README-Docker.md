# Hana API — Docker

Preparação Docker para o `Hana API`.

> A versão em produção via prompt/PowerShell pode continuar na porta `8000`. O Docker publica a API em `8001` para rodar em paralelo sem conflito.

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
