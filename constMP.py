# constMP.py

# ÚNICO endereço configurado manualmente no sistema:
# troque pelo IPv4 público da EC2 onde o namingService.py está rodando.
# Exemplo: SERVICE_NAMES_ADDR = "54.123.45.67"
SERVICE_NAMES_ADDR = "IP_PUBLICO_DO_NAMING_SERVICE"
SERVICE_NAMES_TCP_PORT = 5678

# Endereço usado pelos processos para escutar conexões na própria máquina.
BIND_ADDR = "0.0.0.0"

# Portas fixas da aplicação.
SERVER_PORT = 5679
PEER_TCP_PORT = 5680
PEER_UDP_PORT = 5680

# Nomes/tipos usados no Serviço de Nomes.
SERVER_NAME = "comparisonServer"
SERVER_TYPE = "server"
PEER_TYPE = "peer"

# Quantidade mínima de processos peer exigida pela implantação.
MIN_PEERS = 6

# Prefixo dos arquivos locais das réplicas.
DB_FILE_PREFIX = "replica_db_peer_"
