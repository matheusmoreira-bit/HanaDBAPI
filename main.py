from fastapi import FastAPI, HTTPException, Query, Request
from hdbcli import dbapi
import uvicorn
import logging

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="HANA Universal Gateway")

HANA_CONFIG = {
    'address': '10.160.11.2',
    'port': 30015,
    'user': 'DBSAPB1',
    'password': 'lKd3b10BO*K1eNA2'
}

ALLOWED_VIEWS = ["SBO_ANAGAMING.VW_NOTIFICACAO_PAGAMENTO","SBO_ANAGAMING.VW_B1_APROVACAO_PENDENTE", "SBO_ANAGAMING.VW_APROVACOES_DETALHADAS", "SBO_CACTUS.VW_APROVACOES_DETALHADAS", "SBO_INSTITUTO_ANA.VW_APROVACOES_DETALHADAS"]

@app.get("/data/{view_name}")
def get_hana_data(view_name: str, request: Request, limit: int = 100):
    conn = None
    requested_view = view_name.upper()

    if requested_view not in [v.upper() for v in ALLOWED_VIEWS]:
        raise HTTPException(status_code=403, detail="View não autorizada.")

    try:
        conn = dbapi.connect(**HANA_CONFIG)
        cursor = conn.cursor()
        
        # 1. Descobrir quais colunas existem na View (Metadados)
        parts = requested_view.split(".")
        schema = parts[0] if len(parts) > 1 else None
        table = parts[1] if len(parts) > 1 else requested_view
        
        # Busca colunas reais no catálogo do HANA para evitar erros de SQL
        cursor.execute(f"SELECT COLUMN_NAME FROM TABLE_COLUMNS WHERE SCHEMA_NAME = '{schema}' AND TABLE_NAME = '{table}'")
        valid_columns = [row[0] for row in cursor.fetchall()]
        
        if not valid_columns:
            # Se não achou no catálogo, tenta via descrição da View
            cursor.execute(f'SELECT * FROM "{schema}"."{table}" WHERE 1=0')
            valid_columns = [col[0] for col in cursor.description]

        # 2. Capturar parâmetros da URL e filtrar apenas os válidos
        query_params = dict(request.query_params)
        
        sql = f'SELECT * FROM "{schema}"."{table}"'
        where_clauses = []
        params = []

        for key, value in query_params.items():
            col_upper = key.upper()
            if col_upper in [c.upper() for c in valid_columns]:
                # Encontra o nome exato da coluna (case sensitive do HANA)
                real_col_name = next(c for c in valid_columns if c.upper() == col_upper)
                
                # Se for campo de data (com base no nome), usa LIKE, senão usa igualdade exata
                if "DATA" in col_upper:
                    where_clauses.append(f'CAST("{real_col_name}" AS SECONDDATE) LIKE ?')
                    params.append(f"{value}%")
                else:
                    where_clauses.append(f'"{real_col_name}" = ?')
                    params.append(value)

        # 3. Montagem Final
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        
        sql += f" LIMIT {limit}"
        
        logger.info(f"Executando SQL Dinâmico: {sql}")
        cursor.execute(sql, params)
        
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        return {"success": True, "count": len(results), "data": results}

    except Exception as e:
        logger.error(f"Erro: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)