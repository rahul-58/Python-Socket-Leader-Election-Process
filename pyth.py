# python3 - <<'PY'
import socket

def pick_outbound_ip(dest_host: str, fallback: str = "0.0.0.0") -> str:
    """Infer the best local IP for reaching dest_host (no packets sent)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((dest_host, 9))  # UDP 'discard' port; just to populate getsockname
        ip = s.getsockname()[0]
    except Exception:
        ip = fallback
    finally:
        try: s.close()
        except: pass
    return ip


DEST = ("10.0.0.29", 5001)
LOCAL_IP = "10.0.0.243"   # <-- replace with ipconfig getifaddr result
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((LOCAL_IP, 0))
s.settimeout(5)
s.connect(DEST)
print("Python connect OK (bound to", s.getsockname()[0], ")")
s.close()
# PY
