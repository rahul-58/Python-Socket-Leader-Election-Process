import argparse
import json
import queue
import socket
import threading
import time
import uuid
from dataclasses import dataclass, asdict
from typing import Optional, Tuple, List

# ---------- Configuration & constants ----------

RECV_BUFSIZE = 4096
CONNECT_RETRY_SECS = 2.0   # friendlier output, slower retry
CONNECT_MAX_RETRIES = 30
SOCKET_TIMEOUT = 2.0

# ---------- Logging helpers ----------

class Logger:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()

    def _write(self, line: str):
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line.rstrip() + "\n")

    def sent(self, msg_uuid: uuid.UUID, flag: int):
        self._write(f"Sent: uuid={msg_uuid}, flag={flag}")

    def received(self, msg_uuid: uuid.UUID, flag: int, relation: str, state: int, leader: Optional[uuid.UUID]):
        if state == 0:
            self._write(f"Received: uuid={msg_uuid}, flag={flag}, {relation}, {state}")
        else:
            self._write(f"Received: uuid={msg_uuid}, flag={flag}, {relation}, {state}, leader={leader}")

    def ignored(self, msg_uuid: uuid.UUID, flag: int, reason: str):
        self._write(f"Ignored: uuid={msg_uuid}, flag={flag}, reason={reason}")

    def announce(self, leader_id: uuid.UUID):
        self._write(f"Leader is decided to {leader_id}.")

# ---------- Message model ----------

@dataclass
class Message:
    uuid: str
    flag: int   # 0 = electing, 1 = leader announced

    def to_json_line(self) -> bytes:
        return (json.dumps(asdict(self)) + "\n").encode("utf-8")

    @staticmethod
    def from_json_line(line: str) -> "Message":
        obj = json.loads(line)
        return Message(uuid=obj["uuid"], flag=int(obj["flag"]))

# ---------- Utility ----------

def parse_hostport(s: str) -> Tuple[str, int]:
    if "," in s:
        host, port = s.split(",", 1)
    elif ":" in s:
        host, port = s.split(":", 1)
    else:
        raise ValueError("host,port format expected")
    return host.strip(), int(port.strip())

def load_config(path: str) -> List[Tuple[str, int]]:
    with open(path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f.readlines() if ln.strip()]
    return [parse_hostport(line) for line in lines]

# ---------- Server thread ----------

class ServerThread(threading.Thread):
    def __init__(self, bind_host: str, bind_port: int, inbound_queue: queue.Queue, logger: Logger):
        super().__init__(daemon=True)
        self.bind_host = bind_host
        self.bind_port = bind_port
        self.inbound_queue = inbound_queue
        self.logger = logger
        self._stop = threading.Event()

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.bind_host, self.bind_port))
        srv.listen(5)
        print(f"  [Server] Listening on {self.bind_host}:{self.bind_port}")
        while not self._stop.is_set():
            try:
                conn, addr = srv.accept()
                print(f"  [Server] Accepted connection from {addr}")
                conn.settimeout(SOCKET_TIMEOUT)
                threading.Thread(target=self.handle_conn, args=(conn,), daemon=True).start()
            except Exception:
                break
        try: srv.close()
        except: pass

    def handle_conn(self, conn: socket.socket):
        buf = ""
        while not self._stop.is_set():
            try:
                chunk = conn.recv(RECV_BUFSIZE)
                if not chunk:
                    break
                buf += chunk.decode("utf-8")
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    if line.strip():
                        self.inbound_queue.put(line.strip())
            except socket.timeout:
                continue
            except Exception:
                break
        try: conn.close()
        except: pass

    def stop(self):
        self._stop.set()

# ---------- Outgoing client ----------

class OutClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        retries = 0
        while retries < CONNECT_MAX_RETRIES:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.host, self.port))
                self.sock = s
                print(f"  [Client] Connected to neighbor ({self.host},{self.port})")
                return
            except Exception:
                retries += 1
                print(f"  [Client] Connection to ({self.host},{self.port}) failed, retrying in {CONNECT_RETRY_SECS}s...")
                time.sleep(CONNECT_RETRY_SECS)
        raise RuntimeError(f"Could not connect to neighbor at {self.host}:{self.port}")

    def send_line(self, b: bytes):
        if not self.sock:
            return
        view = memoryview(b)
        while view:
            sent = self.sock.send(view)
            view = view[sent:]

    def close(self):
        if self.sock:
            try: self.sock.shutdown(socket.SHUT_RDWR)
            except: pass
            try: self.sock.close()
            except: pass

# ---------- Node ----------

class Node:
    def __init__(self, role: str, binds: List[Tuple[str,int]], nexts: List[Tuple[str,int]], log_path: str):
        self.role = role
        self.my_id: uuid.UUID = uuid.uuid4()
        self.state: int = 0
        self.leader_id: Optional[uuid.UUID] = None

        self.logger = Logger(log_path)
        self.inbound_queue: queue.Queue[str] = queue.Queue()

        self.servers: List[ServerThread] = [ServerThread(h,p,self.inbound_queue,self.logger) for h,p in binds]
        self.clients: List[OutClient] = [OutClient(h,p) for h,p in nexts]

        self._stop = threading.Event()

    def _compare(self, incoming: uuid.UUID) -> str:
        if incoming.int > self.my_id.int: return "greater"
        if incoming.int < self.my_id.int: return "less"
        return "same"

    def _send_message(self, msg: Message):
        for cli in self.clients:
            cli.send_line(msg.to_json_line())
        self.logger.sent(uuid.UUID(msg.uuid), msg.flag)

    def start(self):
        role_name = {"n": "Normal Node", "x": "Extra Client (X)", "y": "Extra Server (Y)"}[self.role]

        print(f"\n=== {role_name} ===")
        print(f"UUID: {self.my_id}")
        print(f"Server address: {self.servers[0].bind_host}:{self.servers[0].bind_port}")
        print(f"Neighbor addresses: {[ (c.host,c.port) for c in self.clients ]}")

        for s in self.servers:
            s.start()
        for c in self.clients:
            c.connect()

        print("Starting leader election...")
        first = Message(uuid=str(self.my_id), flag=0)
        self._send_message(first)


    def loop(self):
        while not self._stop.is_set():
            try:
                line = self.inbound_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                msg = Message.from_json_line(line)
                incoming_id = uuid.UUID(msg.uuid)
            except Exception:
                continue

            relation = self._compare(incoming_id)
            self.logger.received(incoming_id, msg.flag, relation, self.state, self.leader_id)

            if msg.flag == 0:  # Election phase
                if relation == "greater":
                    # Forward larger candidate
                    self._send_message(Message(uuid=msg.uuid, flag=0))
                    # Remember the largest seen so far
                    if self.leader_id is None or incoming_id.int > self.leader_id.int:
                        self.leader_id = incoming_id

                elif relation == "less":
                    self.logger.ignored(incoming_id, msg.flag, "smaller_uuid")

                else:  # relation == "same"
                    # Only self-elect if no larger UUID has been seen
                    if self.leader_id is None or self.my_id.int > self.leader_id.int:
                        self.state = 1
                        self.leader_id = self.my_id
                        print(f"Leader is {self.leader_id}")
                        self.logger.announce(self.leader_id)
                        self._send_message(Message(uuid=str(self.leader_id), flag=1))
                    else:
                        self.logger.ignored(incoming_id, msg.flag, "larger_uuid_already_seen")
            else:
                if self.state == 0:
                    self.state = 1
                    self.leader_id = incoming_id
                    print(f"Leader is {self.leader_id}")
                    self.logger.announce(self.leader_id)
                if incoming_id == self.my_id:
                    self._stop.set()
                else:
                    self._send_message(Message(uuid=str(incoming_id), flag=1))
                    self._stop.set()
        self.shutdown()

    def shutdown(self):
        for c in self.clients:
            try: c.close()
            except: pass
        for s in self.servers:
            try: s.stop()
            except: pass

# ---------- CLI ----------

def parse_args():
    p = argparse.ArgumentParser(description="Leader election node (Task 1 & 2)")
    p.add_argument("role", choices=["n","x","y"], help="node type: n=normal, x=extra-client, y=extra-server")
    p.add_argument("--config", default="config.txt", help="config file")
    p.add_argument("--log", default="log.txt", help="log file path")
    return p.parse_args()

def main():
    args = parse_args()
    cfg = load_config(args.config)

    if args.role == "n":
        binds = [cfg[0]]
        nexts = [cfg[1]]
    elif args.role == "x":
        binds = [cfg[0]]
        nexts = cfg[1:]
    elif args.role == "y":
        binds = [cfg[0]]
        nexts = [cfg[1]]
    else:
        raise ValueError("invalid role")

    node = Node(args.role, binds, nexts, args.log)
    node.start()
    try:
        node.loop()
    except KeyboardInterrupt:
        node.shutdown()

if __name__ == "__main__":
    main()