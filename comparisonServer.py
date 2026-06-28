from socket import AF_INET, SOCK_STREAM, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, socket
import pickle
import time

from constMP import BIND_ADDR, MIN_PEERS, PEER_TYPE, SERVER_NAME, SERVER_PORT, SERVER_TYPE
from namingService import NamingServiceClient, compose_endpoint, detect_local_ip, split_endpoint


def _read_all(sock):
    chunks = []
    while True:
        data = sock.recv(4096)
        if not data:
            break
        chunks.append(data)
    return b"".join(chunks)


class ComparisonServer:
    def __init__(self):
        self.naming_client = NamingServiceClient()
        self.local_ip = detect_local_ip()

        self.server_sock = socket(AF_INET, SOCK_STREAM)
        self.server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server_sock.bind((BIND_ADDR, SERVER_PORT))
        self.server_sock.listen(32)

        self.endpoint = compose_endpoint(self.local_ip, SERVER_PORT)
        self.naming_client.bind(SERVER_NAME, self.endpoint)
        self.naming_client.register(SERVER_NAME, SERVER_TYPE)
        print("[Server] Registered %s at %s" % (SERVER_NAME, self.endpoint))

        self.udp_sock = socket(AF_INET, SOCK_DGRAM)
        self.sequence_number = 0
        self.peer_list = []

        # Log do sequenciador. Usado para retransmitir mensagens UDP perdidas.
        self.ordered_log = {}
        self.last_local_seq_by_peer = {}

    def close(self):
        try:
            self.naming_client.unbind(SERVER_NAME)
        except Exception:
            pass

        try:
            self.server_sock.close()
        except Exception:
            pass

        try:
            self.udp_sock.close()
        except Exception:
            pass

    def _discover_peers(self):
        peers = self.naming_client.discover(PEER_TYPE)
        return sorted(peers, key=lambda item: item["nome"])

    def get_peer_list(self, wait=True, poll_interval=1.0):
        while True:
            peers = self._discover_peers()

            if not wait:
                self.peer_list = peers
                print("[Server] Discovered peers:", peers)
                return peers

            if len(peers) >= MIN_PEERS:
                self.peer_list = peers
                print("[Server] Discovered peers:", peers)
                return peers

            print("[Server] Waiting for peers... %d/%d" % (len(peers), MIN_PEERS))
            time.sleep(poll_interval)

    def _send_control(self, peer, payload, expect_response=True, timeout=5.0):
        host, port = split_endpoint(peer["endereco"])

        try:
            with socket(AF_INET, SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                sock.connect((host, port))
                sock.sendall(pickle.dumps(payload))
                sock.shutdown(1)

                if not expect_response:
                    return None

                raw = _read_all(sock)
        except Exception:
            return None

        if not raw:
            return None

        try:
            response = pickle.loads(raw)
        except Exception:
            return None

        if not isinstance(response, dict):
            return None

        return response

    def prepare_peers(self, peer_list, n_ops):
        print("[Server] Preparing discovered peers for %s operations each..." % n_ops)
        ready_peers = []

        for peer in peer_list:
            peer_id = len(ready_peers)
            payload = {
                "op": "control",
                "phase": "prepare",
                "peer_id": peer_id,
                "n_ops": n_ops,
                "expected_peers": MIN_PEERS,
                "peers": None,  # preenchido depois que sabemos quais peers estão ativos
            }

            response = self._send_control(peer, payload, expect_response=True, timeout=5.0)

            if response and response.get("status") == "ok":
                ready_peers.append(peer)
                print("[Server] Prepared %s at %s" % (peer["nome"], peer["endereco"]))
            else:
                print("[Server] Ignored unreachable peer %s at %s" % (peer["nome"], peer["endereco"]))

            if len(ready_peers) == MIN_PEERS:
                break

        if len(ready_peers) < MIN_PEERS:
            print("[Server] Only %d/%d active peers are ready." % (len(ready_peers), MIN_PEERS))
            return []

        # Envia uma segunda preparação apenas para os peers ativos, agora com a lista final.
        final_ready = []
        for idx, peer in enumerate(ready_peers):
            payload = {
                "op": "control",
                "phase": "prepare",
                "peer_id": idx,
                "n_ops": n_ops,
                "expected_peers": MIN_PEERS,
                "peers": ready_peers,
            }
            response = self._send_control(peer, payload, expect_response=True, timeout=5.0)
            if response and response.get("status") == "ok":
                final_ready.append(peer)

        if len(final_ready) != MIN_PEERS:
            print("[Server] Could not finalize preparation of all peers. Retrying next round.")
            return []

        return final_ready

    def release_peers(self, peer_list):
        print("[Server] Releasing %d peers to start together..." % len(peer_list))
        for peer in peer_list:
            self._send_control(
                peer,
                {"op": "control", "phase": "go"},
                expect_response=False,
                timeout=2.0,
            )

    def stop_peers(self, peer_list):
        print("[Server] Sending stop signal to %d peers..." % len(peer_list))
        for peer in peer_list:
            self._send_control(
                peer,
                {"op": "control", "phase": "stop"},
                expect_response=False,
                timeout=2.0,
            )

    def _broadcast(self, payload):
        data = pickle.dumps(payload)
        for peer in self.peer_list:
            host, port = split_endpoint(peer["endereco"])
            self.udp_sock.sendto(data, (host, port))

    def _recv_request(self, conn):
        data = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk

        if not data:
            return None

        return pickle.loads(data)

    def _handle_resend_request(self, req):
        seq = req.get("seq")
        if not isinstance(seq, int):
            return {"status": "erro", "mensagem": "Sequência inválida."}

        msg = self.ordered_log.get(seq)
        if msg is None:
            return {"status": "unavailable", "seq": seq}

        return {"status": "ok", "message": msg}

    def _validate_submit_order(self, req):
        peer_id = req["from"]
        local_seq = req["local_seq"]
        expected_local_seq = self.last_local_seq_by_peer.get(peer_id, 0) + 1

        if local_seq != expected_local_seq:
            return {
                "status": "erro",
                "mensagem": (
                    "Ordem local inválida para peer %s: esperado local_seq=%s, recebido local_seq=%s"
                    % (peer_id, expected_local_seq, local_seq)
                ),
            }

        self.last_local_seq_by_peer[peer_id] = local_seq
        return None

    def _handle_submit(self, req):
        order_error = self._validate_submit_order(req)
        if order_error is not None:
            return order_error, False

        self.sequence_number += 1

        ordered_msg = {
            "op": "apply",
            "seq": self.sequence_number,
            "kind": req["kind"],
            "from": req["from"],
            "local_seq": req["local_seq"],
            "key": req["key"],
            "value": req.get("value"),
        }

        self.ordered_log[self.sequence_number] = ordered_msg

        print(
            "[Server] seq=%s | peer=%s | local_seq=%s | %s %s%s"
            % (
                self.sequence_number,
                req["from"],
                req["local_seq"],
                req["kind"],
                req["key"],
                ("=%s" % req.get("value")) if req["kind"] == "WRITE" else "",
            )
        )

        self._broadcast(ordered_msg)

        return {"status": "ok", "seq": self.sequence_number}, True

    def receive_and_sequence_submissions(self, expected_total):
        print("[Server] Waiting for %d submitted operations..." % expected_total)

        received = 0
        while received < expected_total:
            conn, _ = self.server_sock.accept()
            try:
                req = self._recv_request(conn)

                if isinstance(req, dict) and req.get("op") == "submit":
                    response, accepted = self._handle_submit(req)
                    conn.sendall(pickle.dumps(response))
                    if accepted:
                        received += 1

                elif isinstance(req, dict) and req.get("op") == "resend":
                    conn.sendall(pickle.dumps(self._handle_resend_request(req)))

                else:
                    conn.sendall(pickle.dumps({"status": "ignored"}))
            finally:
                conn.close()

    def broadcast_end_marker(self):
        self.sequence_number += 1
        end_msg = {
            "op": "apply",
            "seq": self.sequence_number,
            "kind": "END",
            "from": "server",
            "local_seq": -1,
            "key": None,
            "value": None,
        }
        self.ordered_log[self.sequence_number] = end_msg
        print("[Server] Broadcasting END marker as seq=%s" % self.sequence_number)
        self._broadcast(end_msg)

    def collect_final_states(self, expected_count):
        states = []
        print("[Server] Waiting for final states from %d peers..." % expected_count)

        while len(states) < expected_count:
            conn, _ = self.server_sock.accept()
            try:
                req = self._recv_request(conn)

                if isinstance(req, dict) and req.get("op") == "final_state":
                    states.append(req)
                    print(
                        "[Server] Received final state from peer %s with %d records and %d log entries"
                        % (req["peer"], len(req["db"]), len(req["log"]))
                    )
                    conn.sendall(pickle.dumps({"status": "received"}))

                elif isinstance(req, dict) and req.get("op") == "resend":
                    conn.sendall(pickle.dumps(self._handle_resend_request(req)))

                else:
                    conn.sendall(pickle.dumps({"status": "ignored"}))
            finally:
                conn.close()

        return states

    def compare_final_states(self, states):
        if not states:
            print("[Server] No final states received.")
            return

        states = sorted(states, key=lambda state: state["peer"])
        reference_db = states[0]["db"]
        reference_log = states[0]["log"]
        ok = True

        for state in states[1:]:
            if state["db"] != reference_db:
                ok = False
                print("[Server] Replica DB mismatch detected on peer %s" % state["peer"])

            if state["log"] != reference_log:
                ok = False
                print("[Server] Operation-order log mismatch detected on peer %s" % state["peer"])

        if ok:
            print("[Server] CONSISTENCY OK: same final DB and same total operation order on all replicas.")
        else:
            print("[Server] CONSISTENCY VIOLATION detected.")
            for state in states:
                print("  peer=%s db_records=%d log_entries=%d" % (state["peer"], len(state["db"]), len(state["log"])))

    @staticmethod
    def prompt_user():
        return int(input("Enter the number of operations for each peer to submit (0 to terminate)=> "))

    def run(self):
        try:
            while True:
                n_ops = self.prompt_user()

                if n_ops == 0:
                    peer_list = self.get_peer_list(wait=False)
                    if peer_list:
                        self.stop_peers(peer_list)
                    print("[Server] Stopping.")
                    break

                peer_list = self.get_peer_list(wait=True)
                ready_peers = self.prepare_peers(peer_list, n_ops)
                if len(ready_peers) < MIN_PEERS:
                    print("[Server] Not enough active peers. Waiting for the next attempt...")
                    time.sleep(1.0)
                    continue

                self.peer_list = ready_peers
                self.release_peers(ready_peers)

                expected_total = MIN_PEERS * n_ops
                print("[Server] Peers started. Sequencing commands now...")

                self.sequence_number = 0
                self.ordered_log = {}
                self.last_local_seq_by_peer = {}

                self.receive_and_sequence_submissions(expected_total)
                self.broadcast_end_marker()

                states = self.collect_final_states(MIN_PEERS)
                self.compare_final_states(states)
        finally:
            self.close()


if __name__ == "__main__":
    server = ComparisonServer()
    server.run()
