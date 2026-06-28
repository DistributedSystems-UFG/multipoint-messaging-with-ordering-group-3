import os

SERVICE_NAMES_ADDR = os.getenv("SERVICE_NAMES_ADDR", "127.0.0.1")
SERVICE_NAMES_TCP_PORT = int(os.getenv("SERVICE_NAMES_TCP_PORT", "5678"))

SERVER_NAME = "comparisonServer"
SERVER_TYPE = "server"
PEER_TYPE = "peer"

SERVER_PORT = int(os.getenv("SERVER_PORT", "5679"))
PEER_TCP_PORT = int(os.getenv("PEER_TCP_PORT", "5680"))
PEER_UDP_PORT = int(os.getenv("PEER_UDP_PORT", "5680"))

ADVERTISE_ADDR = os.getenv("ADVERTISE_ADDR")
MIN_PEERS = int(os.getenv("MIN_PEERS", "6"))

DB_FILE_PREFIX = "replica_db_peer_"
