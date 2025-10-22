import json, socket, threading, time, uuid, argparse, logging, queue, os
from dataclasses import dataclass
from typing import Tuple, Optional, List

# Socket reading buffer size and non-blocking timeouts
RECV_BUF = 4096
SOCKET_TIMEOUT = 0.5

# common message 
@dataclass
class Message:
    uuid: str
    flag: int

    # serialize to a single JSON line, terminated by '\n'
    def to_line(self) -> bytes:
        return (json.dumps({"uuid": self.uuid, "flag": self.flag}) + "\n").encode("utf-8")

    # parse a JSON line back to a message
    @staticmethod
    def from_line(s: str) -> "Message":
        o = json.loads(s)
        return Message(uuid=o["uuid"], flag=int(o["flag"]))

# parse 'ip,port' into (ip, port)
def parse_hostport(line: str) -> Tuple[str,int]:
    ip, port = [p.strip() for p in line.strip().split(",")]
    return ip, int(port)

# create a named logger that logs to both file and console
def setup_logger(name: str, log_path: str) -> logging.Logger:
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(message)s")
    fh = logging.FileHandler(log_path)
    ch = logging.StreamHandler()
    fh.setFormatter(fmt); ch.setFormatter(fmt)
    log.handlers = []
    log.addHandler(fh); 
    log.addHandler(ch)
    return log

# compare UUIDs numerically to implement Chang–Roberts comparisons
def uuid_relation(my_uuid: str, other: str) -> str:
    a = uuid.UUID(my_uuid).int
    b = uuid.UUID(other).int
    return "greater" if b > a else "less" if b < a else "same"



# Task 1 
class NodeTask1:

    """
    Task 1: Asynchronous ring with:
      - 1 incoming server (bind from line 1 of config)
      - 1 outgoing client (target from line 2 of config)
    Direction is client -> server.
    """

    # initialize sockets, state, load config, and prepare logging
    def __init__(self, config_path: str, log_path: str):
        self.my_uuid = str(uuid.uuid4())
        self.leader_id: Optional[str] = None
        self.state_flag = 0  # 0 seeking, 1 know leader
        self.shutdown = threading.Event()

        # logger for Task 1
        self.log = setup_logger("Task1", log_path)

        # load config: line1 = my bind, line2 = client target
        with open(config_path, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
        if len(lines) < 2:
            raise ValueError("Task 1 config must have 2 lines: <my_bind> and <target>")

        self.my_bind = parse_hostport(lines[0])
        self.client_target = parse_hostport(lines[1])

        # socket objects and threading helpers
        self.server_sock: Optional[socket.socket] = None
        self.server_thread: Optional[threading.Thread] = None
        self.incoming_conn: Optional[socket.socket] = None
        self.reader_thread: Optional[threading.Thread] = None

        # queue and buffer for framed JSON lines
        self.recv_q = queue.Queue()
        self.recv_buf = bytearray()

        # outgoing client socket
        self.client: Optional[socket.socket] = None

        self.log.info(f"[Task1] my_uuid={self.my_uuid}")
        self.log.info(f"[Task1] bind={self.my_bind}, target={self.client_target}")

    # server
    # bind, listen, and accept one incoming connection
    def server_start(self):
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(self.my_bind)
        self.server_sock.listen(1)
        self.server_sock.settimeout(1.0)
        self.log.info("[Task1] Listening...")
        while not self.shutdown.is_set():
            try:
                conn, addr = self.server_sock.accept()
                conn.settimeout(SOCKET_TIMEOUT)
                self.incoming_conn = conn
                self.log.info(f"[Task1] Accepted {addr}")
                self.reader_thread = threading.Thread(target=self.reader_loop, daemon=True)
                self.reader_thread.start()
                break
            except socket.timeout:
                pass
            except OSError as e:
                self.log.info(f"[Task1] Accept error: {e}")
                time.sleep(0.2)

    # read bytes from the accepted connection, reassemble '\n'-terminated JSON messages, parse them to Message, and enqueue for processing
    def reader_loop(self):
        c = self.incoming_conn
        while not self.shutdown.is_set():
            try:
                data = c.recv(RECV_BUF)
                if not data: break
                self.recv_buf.extend(data)
                while True:
                    nl = self.recv_buf.find(b"\n")
                    if nl == -1: break
                    line = self.recv_buf[:nl].decode("utf-8", errors="replace")
                    del self.recv_buf[:nl+1]
                    if line.strip():
                        try:
                            self.recv_q.put(Message.from_line(line))
                        except Exception as e:
                            self.log.info(f"[Task1] Parse error: {e} line={line!r}")
            except socket.timeout:
                pass
            except Exception as e:
                self.log.info(f"[Task1] Read error: {e}")
                time.sleep(0.2)
        try: c.close()
        except: pass

    # client
    # Connect to the next node (client_target) with retry/backoff
    def client_connect(self):
        host, port = self.client_target
        backoff = [0.3,0.5,1,1,2,2,3,5,5]
        attempt = 0
        while not self.shutdown.is_set():
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            try:
                self.log.info(f"[Task1] Connecting to {host}:{port} ...")
                s.connect((host, port))
                s.settimeout(SOCKET_TIMEOUT)
                self.client = s
                self.log.info("[Task1] Connected.")
                return True
            except Exception as e:
                try: s.close()
                except: pass
                delay = backoff[attempt] if attempt < len(backoff) else backoff[-1]
                self.log.info(f"[Task1] Connect failed: {e}; retry {delay}s")
                attempt += 1
                time.sleep(delay)
        return False

    # send a message over the established client socket
    def send(self, msg: Message):
        if not self.client: return
        try:
            self.client.sendall(msg.to_line())
            self.log.info(f"Sent: uuid={msg.uuid}, flag={msg.flag}")
        except Exception as e:
            self.log.info(f"[Task1] Send error: {e}")


    # Chang–Roberts Algorithm
    """
    Process one incoming message according to Chang-Roberts:
        - If flag=0 (candidate): forward only if candidate > mine;
          if candidate == mine, I am leader and announce.
        - If flag=1 (leader): adopt if unknown or higher-priority.
    """
    def handle(self, m: Message):
        rel = uuid_relation(self.my_uuid, m.uuid)
        if self.state_flag == 0:
            self.log.info(f"Received: uuid={m.uuid}, flag={m.flag}, {rel}, 0")
        else:
            self.log.info(f"Received: uuid={m.uuid}, flag={m.flag}, {rel}, 1, leader_id={self.leader_id}")

        if m.flag == 0:
            if rel == "greater":
                self.send(Message(uuid=m.uuid, flag=0))
            elif rel == "same":
                if self.state_flag == 0:
                    self.leader_id = self.my_uuid
                    self.state_flag = 1
                    self.log.info(f"Leader is decided to {self.leader_id}.")
                    self.send(Message(uuid=self.leader_id, flag=1))
            else:
                self.log.info(f"Ignored: uuid={m.uuid}, flag=0, reason=smaller candidate than mine")
        else:
            if self.state_flag == 0:
                self.leader_id = m.uuid
                self.state_flag = 1
                self.log.info(f"Leader is decided to {self.leader_id}.")
                self.send(Message(uuid=m.uuid, flag=1))
            else:
                if self.leader_id == m.uuid:
                    self.log.info(f"Ignored: uuid={m.uuid}, flag=1, reason=announcement already known (same leader)")
                else:
                    if uuid_relation(self.my_uuid, m.uuid) == "greater":
                        self.leader_id = m.uuid
                        self.log.info(f"Leader updated to {self.leader_id} due to higher announcement.")
                        self.send(Message(uuid=m.uuid, flag=1))
                    else:
                        self.log.info(f"Ignored: uuid={m.uuid}, flag=1, reason=lower-priority announcement than current leader")

    
    # Run the node by starting server thread, connecting client and sending initial candidate and process messages until a quiet period after knowing the leader
    def run(self):
        try:
            self.server_thread = threading.Thread(target=self.server_start, daemon=True)
            self.server_thread.start()
            time.sleep(0.2)
            if not self.client_connect():
                return
            self.send(Message(uuid=self.my_uuid, flag=0))
            idle = 0
            while not self.shutdown.is_set():
                progressed = False
                try:
                    while True:
                        m = self.recv_q.get_nowait()
                        self.handle(m)
                        progressed = True
                except queue.Empty:
                    pass
                idle = 0 if progressed else idle + 1
                if self.state_flag == 1 and idle > 25:  
                    break
                time.sleep(0.1)
        finally:
            self.shutdown.set()
            for s in [self.incoming_conn, self.client, self.server_sock]:
                try:
                    if s: s.close()
                except: pass
            print(f"leader is {self.leader_id}" if self.leader_id else "leader is (unknown)")


# Task 2 
class NodeTask2:
    """
    Task 2: Partially double ring.
    node_type:
      - 'x' : 1 incoming server, 2 outgoing clients 
      - 'y' : 2 incoming servers, 1 outgoing client 
      - 'n' : 1 incoming server, 1 outgoing client 
    Direction is client -> server.
    """

    # initialize Task 2 node state, sockets, config, and logger
    def __init__(self, node_type: str, config_path: str, log_path: str):
        assert node_type in ("x","y","n")
        self.node_type = node_type
        self.my_uuid = str(uuid.uuid4())
        self.leader_id: Optional[str] = None
        self.state_flag = 0
        self.shutdown = threading.Event()

        self.log = setup_logger(f"Task2-{node_type}", log_path)

        # Load config:
        # line1 = my bind; line2 = first client target; line3 (only for x) = second client target
        with open(config_path, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
        if len(lines) < 2:
            raise ValueError("Task 2 config must have at least 2 lines")
        self.my_bind = parse_hostport(lines[0])
        first_client = parse_hostport(lines[1])
        self.client_targets: List[Tuple[str,int]] = [first_client]
        if node_type == "x":
            if len(lines) < 3:
                raise ValueError("Node type 'x' requires a 3rd line (second outgoing client).")
            self.client_targets.append(parse_hostport(lines[2]))
        self.expected_incoming = 2 if node_type == "y" else 1

        # Server and client sockets and helpers
        self.server_sock: Optional[socket.socket] = None
        self.server_thread: Optional[threading.Thread] = None
        self.incoming_conns: List[socket.socket] = []
        self.reader_threads: List[threading.Thread] = []
        self.recv_qs: List[queue.Queue] = []
        self.recv_bufs: List[bytearray] = []
        self.clients: List[socket.socket] = []
        self.forwarded_ann: set[str] = set()
        self.seen_candidates: set[str] = set()

        self.log.info(f"[Task2] type={node_type} my_uuid={self.my_uuid}")
        self.log.info(f"[Task2] bind={self.my_bind}, targets={self.client_targets}, expect_in={self.expected_incoming}")

    # server
    # Bind and listen for expected number of incoming connections.
    # - 'y' expects 2 incoming connections, others expect 1.
    # - Each accepted connection gets its own reader thread and queue.
    def server_start(self):
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(self.my_bind)
        self.server_sock.listen(self.expected_incoming)
        self.server_sock.settimeout(1.0)
        self.log.info("[Task2] Listening...")
        accepted = 0
        while not self.shutdown.is_set() and accepted < self.expected_incoming:
            try:
                conn, addr = self.server_sock.accept()
                conn.settimeout(SOCKET_TIMEOUT)
                self.incoming_conns.append(conn)
                q = queue.Queue()
                self.recv_qs.append(q)
                buf = bytearray()
                self.recv_bufs.append(buf)
                t = threading.Thread(target=self.reader_loop, args=(conn, q, buf), daemon=True)
                t.start()
                self.reader_threads.append(t)
                accepted += 1
                self.log.info(f"[Task2] Accepted {addr} ({accepted}/{self.expected_incoming})")
            except socket.timeout:
                pass
            except OSError as e:
                self.log.info(f"[Task2] Accept error: {e}")
                time.sleep(0.2)
        self.log.info("[Task2] Finished accepting expected connections.")

    

    # Reader loop for a single incoming connection.
    # Accumulates bytes into a buffer until '\n' is found.
    # Converts lines to Message and enqueues them for processing.
    def reader_loop(self, conn: socket.socket, out_q: queue.Queue, buf: bytearray):
        while not self.shutdown.is_set():
            try:
                data = conn.recv(RECV_BUF)
                if not data: break
                buf.extend(data)
                while True:
                    nl = buf.find(b"\n")
                    if nl == -1: break
                    line = buf[:nl].decode("utf-8", errors="replace")
                    del buf[:nl+1]
                    if line.strip():
                        try:
                            out_q.put(Message.from_line(line))
                        except Exception as e:
                            self.log.info(f"[Task2] Parse error: {e} line={line!r}")
            except socket.timeout:
                pass
            except Exception as e:
                self.log.info(f"[Task2] Read error: {e}")
                time.sleep(0.2)
        try: conn.close()
        except: pass

    # clients
    # Connect to all configured outgoing targets with retry/backoff.
    # - 'x' connects to two neighbors (second line and third line of config).
    # - Others connect to one neighbor (second line of config).
    def client_connect_all(self):
        for (host, port) in self.client_targets:
            backoff = [0.3,0.5,1,1,2,2,3,5,5]
            attempt = 0
            while not self.shutdown.is_set():
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1.0)
                try:
                    self.log.info(f"[Task2] Connecting to {host}:{port} ...")
                    s.connect((host, port))
                    s.settimeout(SOCKET_TIMEOUT)
                    self.clients.append(s)
                    self.log.info("[Task2] Connected.")
                    break
                except Exception as e:
                    try: s.close()
                    except: pass
                    delay = backoff[attempt] if attempt < len(backoff) else backoff[-1]
                    self.log.info(f"[Task2] Connect failed: {e}; retry {delay}s")
                    attempt += 1
                    time.sleep(delay)

    
    # Broadcast a message to all outgoing client sockets.
    def send_all(self, msg: Message):
        data = msg.to_line()
        for c in list(self.clients):
            try:
                c.sendall(data)
                self.log.info(f"Sent: uuid={msg.uuid}, flag={msg.flag}")
            except Exception as e:
                self.log.info(f"[Task2] Send error: {e}; dropping client")
                try: c.close()
                except: pass
                self.clients.remove(c)


    # Chang–Roberts Algorithm
    """
    Process an incoming message:
        - flag=0 (candidate):
            forward only if candidate > mine
            dedupe: forward each candidate UUID at most once per node
            if candidate == mine, I become leader and announce

        - flag=1 (announcement):
            adopt if unknown
            dedupe announcements to avoid loops on the double path
    """
    def handle(self, m: Message):
        rel = uuid_relation(self.my_uuid, m.uuid)
        if self.state_flag == 0:
            self.log.info(f"Received: uuid={m.uuid}, flag={m.flag}, {rel}, 0")
        else:
            self.log.info(f"Received: uuid={m.uuid}, flag={m.flag}, {rel}, 1, leader_id={self.leader_id}")

        if m.flag == 0:
            if rel == "greater":
                if m.uuid not in self.seen_candidates:
                    self.seen_candidates.add(m.uuid)
                    self.send_all(Message(uuid=m.uuid, flag=0))
                else:
                    self.log.info(f"Ignored: uuid={m.uuid}, flag=0, reason=duplicate candidate already forwarded")
            elif rel == "same":
                if self.state_flag == 0:
                    self.leader_id = self.my_uuid
                    self.state_flag = 1
                    self.log.info(f"Leader is decided to {self.leader_id}.")
                    self.forwarded_ann.add(self.leader_id)
                    self.send_all(Message(uuid=self.leader_id, flag=1))
            else:
                self.log.info(f"Ignored: uuid={m.uuid}, flag=0, reason=smaller candidate than mine")
        else:
            if self.state_flag == 0:
                self.leader_id = m.uuid
                self.state_flag = 1
                self.log.info(f"Leader is decided to {self.leader_id}.")
                if m.uuid not in self.forwarded_ann:
                    self.forwarded_ann.add(m.uuid)
                    self.send_all(Message(uuid=m.uuid, flag=1))
                else:
                    self.log.info(f"Ignored: uuid={m.uuid}, flag=1, reason=announcement already forwarded")
            else:
                if self.leader_id == m.uuid:
                    self.log.info(f"Ignored: uuid={m.uuid}, flag=1, reason=announcement already known (same leader)")
                else:
                    if uuid_relation(self.my_uuid, m.uuid) == "greater":
                        self.leader_id = m.uuid
                        self.log.info(f"Leader updated to {self.leader_id} due to higher announcement.")
                        if m.uuid not in self.forwarded_ann:
                            self.forwarded_ann.add(m.uuid)
                            self.send_all(Message(uuid=m.uuid, flag=1))
                    else:
                        self.log.info(f"Ignored: uuid={m.uuid}, flag=1, reason=lower-priority announcement than current leader")

    # Run the node by starting server thread to accept expected incoming connections, connect all outgoing clients
    # sending my candidate to all and processing messages until quiet period after leader known.
    def run(self):
        try:
            self.server_thread = threading.Thread(target=self.server_start, daemon=True)
            self.server_thread.start()
            time.sleep(0.2)

            self.client_connect_all()
            self.send_all(Message(uuid=self.my_uuid, flag=0))

            idle = 0
            while not self.shutdown.is_set():
                progressed = False
                for q in self.recv_qs:
                    try:
                        while True:
                            m = q.get_nowait()
                            self.handle(m)
                            progressed = True
                    except queue.Empty:
                        pass
                idle = 0 if progressed else idle + 1
                if self.state_flag == 1 and idle > 25:  # ~2.5s
                    break
                time.sleep(0.1)
        finally:
            self.shutdown.set()
            for c in self.clients:
                try: c.close()
                except: pass
            for s in self.incoming_conns:
                try: s.close()
                except: pass
            try:
                if self.server_sock: self.server_sock.close()
            except: pass
            print(f"leader is {self.leader_id}" if self.leader_id else "leader is (unknown)")


"""
Parse CLI flags and launch the appropriate task/node:

    --task {1,2}  : select Task 1 or Task 2
    --type {x,y,n}: (Task 2 only) node type: x=two clients, y=two servers, n=normal
    --config PATH : path to config file (bind and targets)
    --log PATH    : path to log file for this node
"""
# main 
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Leader Election: Task 1 and Task 2")
    ap.add_argument("--task", choices=["1","2"], required=True, help="1 = ring, 2 = partially double ring")
    ap.add_argument("--type", choices=["x","y","n"], help="(Task 2 only) x=two clients, y=two servers, n=normal")
    ap.add_argument("--config", default="config.txt")
    ap.add_argument("--log", default="log.txt")
    args = ap.parse_args()

    if args.task == "1":
        NodeTask1(args.config, args.log).run()
    else:
        if not args.type:
            raise SystemExit("For --task 2 you must also pass --type {x,y,n}")
        NodeTask2(args.type, args.config, args.log).run()
