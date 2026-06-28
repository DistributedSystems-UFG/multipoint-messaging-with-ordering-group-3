from socket import AF_INET, SOCK_STREAM, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, socket
import pickle
import time

from constMP import PEER_TYPE, SERVER_NAME, SERVER_PORT, SERVER_TYPE
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
        self.server_sock.bind(("0.0.0.0", SERVER_PORT))
        self.server_sock.listen(16)

        self.endpoint = compose_endpoint(self.local_ip, SERVER_PORT)
        self.naming_client.bind(SERVER_NAME, self.endpoint)
        self.naming_client.register(SERVER_NAME, SERVER_TYPE)

        self.udp_sock = socket(AF_INET, SOCK_DGRAM)
        self.sequence_number = 0
        self.peer_list = []

        # Log mantido pelo sequenciador: permite retransmitir mensagens
        # quando uma réplica detecta lacuna na sequência recebida por UDP.
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
        peers = sorted(peers, key=lambda item: item["nome"])
        return peers

    def get_peer_list(self, wait=True, poll_interval=1.0):
        while True:
            peers = self._discover_peers()
            if peers or not wait:
                self.peer_list = peers
                print("[Server] Discovered peers:", peers)
                return peers

            print("[Server] No peers discovered yet. Waiting for registrations...")
            time.sleep(poll_interval)

    def _send_control(self, peer, payload, expect_response=True, timeout=5.0):
        host, port = split_endpoint(peer["endereco"])

        with socket(AF_INET, SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect((host, port))
            sock.sendall(pickle.dumps(payload))
            sock.shutdown(1)

            if not expect_response:
                return None

            try:
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
        print(f"[Server] Preparing {len(peer_list)} peers for {n_ops} operations each...")
        ready_peers = []

        for idx, peer in enumerate(peer_list):
            payload = {
                "op": "control",
                "phase": "prepare",
                "peer_id": idx,
                "n_ops": n_ops,
                "expected_peers": len(peer_list),
                "peers": peer_list,
            }

            response = self._send_control(peer, payload, expect_response=True, timeout=5.0)

            if response and response.get("status") == "ok":
                ready_peers.append(peer)
                print(f"[Server] Prepared {peer['nome']} at {peer['endereco']}")
            else:
                print(f"[Server] Failed to prepare {peer['nome']} at {peer['endereco']}")

        return ready_peers

    def release_peers(self, peer_list):
        print(f"[Server] Releasing {len(peer_list)} peers to start together...")
        for peer in peer_list:
            self._send_control(
                peer,
                {"op": "control", "phase": "go"},
                expect_response=False,
                timeout=2.0,
            )

    def stop_peers(self, peer_list):
        print(f"[Server] Sending stop signal to {len(peer_list)} peers...")
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
            f"[Server] seq={self.sequence_number} | "
            f"peer={req['from']} | local_seq={req['local_seq']} | "
            f"{req['kind']} {req['key']}"
            + (f"={req.get('value')}" if req["kind"] == "WRITE" else "")
        )

        self._broadcast(ordered_msg)

        return {"status": "ok", "seq": self.sequence_number}, True

    def receive_and_sequence_submissions(self, expected_total):
        print(f"[Server] Waiting for {expected_total} submitted operations...")

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
        print(f"[Server] Broadcasting END marker as seq={self.sequence_number}")
        self._broadcast(end_msg)

    def collect_final_states(self, expected_count):
        states = []
        print(f"[Server] Waiting for final states from {expected_count} peers...")

        while len(states) < expected_count:
            conn, _ = self.server_sock.accept()
            try:
                req = self._recv_request(conn)

                if isinstance(req, dict) and req.get("op") == "final_state":
                    states.append(req)
                    print(
                        f"[Server] Received final state from peer {req['peer']} "
                        f"with {len(req['db'])} records"
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

        reference = states[0]["db"]
        ok = True

        for state in states[1:]:
            if state["db"] != reference:
                ok = False
                print(f"[Server] Replica mismatch detected on peer {state['peer']}")

        if ok:
            print("[Server] All replicas ended with the same database content.")
        else:
            print("[Server] Final replica contents:")
            for state in states:
                print(f"  peer={state['peer']} db={state['db']}")

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
                if not peer_list:
                    continue

                ready_peers = self.prepare_peers(peer_list, n_ops)
                if not ready_peers:
                    print("[Server] No peers confirmed readiness. Waiting for the next round...")
                    time.sleep(1.0)
                    continue

                self.peer_list = ready_peers
                self.release_peers(ready_peers)

                expected_total = len(ready_peers) * n_ops
                print("[Server] Peers started. Sequencing commands now...")

                self.sequence_number = 0
                self.ordered_log = {}
                self.last_local_seq_by_peer = {}

                self.receive_and_sequence_submissions(expected_total)
                self.broadcast_end_marker()

                states = self.collect_final_states(len(ready_peers))
                self.compare_final_states(states)
        finally:
            self.close()


if __name__ == "__main__":
    server = ComparisonServer()
    server.run()
