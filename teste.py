from hdbcli import dbapi

print("Iniciando teste de conexão...")
try:
    conn = dbapi.connect(
        address='10.160.11.2',
        port=30015,
        user='DBSAPB1',
        password='lKd3b10BO*K1eNA2'
    )
    print("Conectado com sucesso!")
    conn.close()
except Exception as e:
    print(f"Erro na conexão: {e}")