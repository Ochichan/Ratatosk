import socket, time, sys, json, statistics
mode, target = sys.argv[1], sys.argv[2]
n = int(sys.argv[3]) if len(sys.argv) > 3 else 20000
if mode == "tcp":
    host, port = target.rsplit(":", 1)
    s = socket.create_connection((host, int(port)))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
else:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); s.connect(target)
req = b"*1\r\n$4\r\nPING\r\n"
def rt():
    s.sendall(req); b = b""
    while not b.endswith(b"\r\n"): b += s.recv(64)
    assert b == b"+PONG\r\n", b
for _ in range(2000): rt()
xs = []
for _ in range(n):
    t = time.perf_counter_ns(); rt(); xs.append(time.perf_counter_ns() - t)
xs.sort()
q = lambda p: xs[min(len(xs)-1, int(p*len(xs)))]
print(json.dumps({"mode": mode, "n": n, "p50_us": q(0.5)/1e3, "p95_us": q(0.95)/1e3, "p99_us": q(0.99)/1e3, "p999_us": q(0.999)/1e3, "max_us": xs[-1]/1e3}))
