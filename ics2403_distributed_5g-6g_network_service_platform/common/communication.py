"""
communication.py
-----------------
[MILESTONE 1 - Distributed OS Foundation]

Provides the basic inter-node communication mechanism required by
Milestone 1: a minimal request/reply protocol over TCP sockets,
carrying JSON messages. Every node in the platform (Edge, RAN, Core,
Cloud) runs a small threaded TCP server built on this module.

This module is deliberately kept low-level (raw sockets, not a
framework) in Milestone 1 because the point of this stage is to show
*how* distributed communication works. Milestone 11 (RPC and
Distributed Shared Memory) later builds a proper RPC abstraction
(client stub / server stub) on top of this same transport, so the
protocol framing defined here (length-prefixed JSON) is reused
end-to-end through the whole 12-week system.
"""

import socket
import json
import struct
import threading
import time


def _send_framed(sock: socket.socket, payload: dict):
    data = json.dumps(payload).encode("utf-8")
    sock.sendall(struct.pack(">I", len(data)) + data)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed while reading")
        buf += chunk
    return buf


def _recv_framed(sock: socket.socket) -> dict:
    header = _recv_exact(sock, 4)
    (length,) = struct.unpack(">I", header)
    data = _recv_exact(sock, length)
    return json.loads(data.decode("utf-8"))


def send_message(host: str, port: int, message: dict, timeout: float = 5.0) -> dict:
    """
    Client-side call: connects to (host, port), sends a JSON message,
    waits for a JSON reply. Records round-trip latency in the reply
    under '_rtt_ms' for use by Milestone 2 performance measurement.
    """
    t0 = time.time()
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        _send_framed(sock, message)
        reply = _recv_framed(sock)
    rtt_ms = (time.time() - t0) * 1000.0
    reply["_rtt_ms"] = round(rtt_ms, 3)
    return reply


class MessageServer:
    """
    Server-side: a minimal threaded TCP server. `handler(message: dict) -> dict`
    is invoked for every incoming message and its return value is sent back.
    """

    def __init__(self, host: str, port: int, handler):
        self.host = host
        self.port = port
        self.handler = handler
        self._sock = None
        self._running = False
        self._thread = None

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(64)
        self._running = True
        self._thread = threading.Thread(target=self._serve_loop, daemon=True)
        self._thread.start()

    def _serve_loop(self):
        while self._running:
            try:
                self._sock.settimeout(1.0)
                try:
                    conn, addr = self._sock.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()
            except OSError:
                break

    def _handle_conn(self, conn: socket.socket):
        try:
            with conn:
                message = _recv_framed(conn)
                reply = self.handler(message)
                _send_framed(conn, reply)
        except Exception as e:
            try:
                _send_framed(conn, {"status": "error", "error": str(e)})
            except Exception:
                pass

    def stop(self):
        self._running = False
        if self._sock:
            self._sock.close()
