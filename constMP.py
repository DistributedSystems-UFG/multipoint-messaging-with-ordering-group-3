# constMP.py

# ÚNICO endereço configurado manualmente no sistema:
# coloque aqui o IPv4 público da EC2 onde o namingService.py está rodando
SERVICE_NAMES_ADDR = "IP_PUBLICO_DO_NAMING_SERVICE"
SERVICE_NAMES_TCP_PORT = 5678

# Endereço usado para escutar conexões na própria máquina
BIND_ADDR = "0.0.0.0"

# Portas fixas da aplicação
SERVER_PORT = 5679
PEER_TCP_PORT = 5680
PEER_UDP_PORT = 5680

# Tipos usados no Naming Service
SERVER_NAME = "comparisonServer"
SERVER_TYPE = "server"
PEER_TYPE = "peer"

# Quantidade mínima de peers esperada
MIN_PEERS = 6

# Prefixo dos arquivos de banco local dos peers
DB_FILE_PREFIX = "replica_db_peer_"
