# Como gerar o DynamicToken

O `DynamicToken` e um token temporario usado pela HanaDBAPI para liberar consultas.
Ele deve ser enviado em toda requisicao protegida, preferencialmente no header
`Authorization: Bearer`.

## Regra de geracao

O token e o HMAC-SHA256 de um bloco horario Unix usando o segredo configurado na API.

```text
hour_block = floor(unix_timestamp_atual_em_segundos / 3600)
dynamic_token = HMAC_SHA256(secret=dynamic_token_secret, message=hour_block)
```

Onde:

- `dynamic_token_secret`: valor configurado em `api.dynamic_token_secret` no arquivo
  `config`, ou pela variavel de ambiente `HANA_QUERY_DYNAMIC_TOKEN_SECRET`.
- `unix_timestamp_atual_em_segundos`: horario atual em segundos desde 1970-01-01 UTC.
- `hour_block`: numero inteiro que representa a hora atual.
- `dynamic_token`: digest hexadecimal gerado pelo HMAC-SHA256.

A API aceita o token da hora atual e tambem o token da hora anterior. Isso cria uma
tolerancia de ate aproximadamente 1 hora para diferenca de relogio ou troca de hora.

## Python

```python
import hashlib
import hmac
import time

dynamic_token_secret = "valor-do-segredo-configurado-na-api"
hour_block = int(time.time()) // 3600

dynamic_token = hmac.new(
    dynamic_token_secret.encode("utf-8"),
    str(hour_block).encode("utf-8"),
    hashlib.sha256,
).hexdigest()

print(dynamic_token)
```

## JavaScript / Node.js

```javascript
const crypto = require("crypto");

const dynamicTokenSecret = "valor-do-segredo-configurado-na-api";
const hourBlock = Math.floor(Date.now() / 1000 / 3600);

const dynamicToken = crypto
  .createHmac("sha256", dynamicTokenSecret)
  .update(String(hourBlock))
  .digest("hex");

console.log(dynamicToken);
```

## PowerShell

```powershell
$dynamicTokenSecret = "valor-do-segredo-configurado-na-api"
$unixTimestamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$hourBlock = [math]::Floor($unixTimestamp / 3600)

$hmac = [System.Security.Cryptography.HMACSHA256]::new(
    [System.Text.Encoding]::UTF8.GetBytes($dynamicTokenSecret)
)
$hashBytes = $hmac.ComputeHash([System.Text.Encoding]::UTF8.GetBytes("$hourBlock"))
$dynamicToken = -join ($hashBytes | ForEach-Object { $_.ToString("x2") })

Write-Output $dynamicToken
```

## Envio na requisicao

Forma recomendada:

```http
GET /data/VW_FORNECEDORES?schema=SBO_ANAGAMING&limit=100 HTTP/1.1
Host: localhost:8000
Authorization: Bearer {{dynamic_token}}
X-SAP-Session-ID: {{sap_session_id}}
X-SAP-Route-ID: {{sap_route_id}}
```

Tambem sao aceitos os headers `DynamicToken` e `X-Dynamic-Token`:

```http
DynamicToken: {{dynamic_token}}
```

```http
X-Dynamic-Token: {{dynamic_token}}
```

Credenciais nao devem ser enviadas pela URL. A API rejeita parametros como
`dynamicToken`, `dynamic_token`, `sessionId`, `session_id`, `routeId` e `route_id`
na query string.

## Exemplo com curl

```bash
curl "http://localhost:8000/data/VW_FORNECEDORES?schema=SBO_ANAGAMING&limit=100" \
  -H "Authorization: Bearer ${DYNAMIC_TOKEN}" \
  -H "X-SAP-Session-ID: ${SAP_SESSION_ID}" \
  -H "X-SAP-Route-ID: ${SAP_ROUTE_ID}"
```

## Erros comuns

- `401 DynamicToken nao informado.`: o header com o token nao foi enviado.
- `401 DynamicToken invalido.`: o token foi calculado com segredo incorreto, horario
  incorreto ou algoritmo diferente.
- `503 HANA_QUERY_DYNAMIC_TOKEN_SECRET nao configurado.`: a API esta sem
  `dynamic_token_secret` no `config` e sem a variavel `HANA_QUERY_DYNAMIC_TOKEN_SECRET`.

