"""Networking module for Blueprint.

Provides TCP, UDP, and cross-platform socket operations.
"""

# === TCP ===

def _tcp_connect(host, port):
    """Connect to a TCP server."""
    return __bp_event_source("tcp_connect", {"host": host, "port": port})

def _tcp_listen(host, port):
    """Start a TCP listener."""
    return __bp_event_source("tcp_listen", {"host": host, "port": port})

def _tcp_send(handle, data):
    """Send data on a TCP connection."""
    return __bp_event_write(handle, data)

def _tcp_recv(handle, timeout_ms=-1):
    """Receive data from a TCP connection."""
    event = __bp_event_poll([handle], timeout_ms)
    if event == None:
        return None
    if event["type"] == "closed":
        return None
    if event["type"] == "error":
        fail("TCP recv failed: " + event["data"]["message"])
    return event["data"]["data"]

def _tcp_close(handle):
    """Close a TCP connection or listener."""
    __bp_event_source_close(handle)

def _tcp_accept(listener_handle, timeout_ms=-1):
    """Accept a connection on a TCP listener."""
    event = __bp_event_poll([listener_handle], timeout_ms)
    if event == None:
        return None
    if event["type"] == "error":
        fail("TCP accept failed: " + event["data"]["message"])
    return event["data"]["client_handle"]

def _tcp_request(host, port, data, timeout_ms=30000):
    """Make a simple TCP request/response."""
    conn = _tcp_connect(host, port)
    _tcp_send(conn, data)
    response = _tcp_recv(conn, timeout_ms)
    _tcp_close(conn)
    return response

tcp = struct(
    connect = _tcp_connect,
    listen = _tcp_listen,
    send = _tcp_send,
    recv = _tcp_recv,
    close = _tcp_close,
    accept = _tcp_accept,
    request = _tcp_request,
)

# === UDP ===

def _udp_bind(host, port):
    """Bind a UDP socket."""
    return __bp_event_source("udp", {"host": host, "port": port})

def _udp_send_to(handle, data, host, port):
    """Send a datagram to a specific address."""
    return __bp_event_write(handle, data, host, port)

def _udp_recv_from(handle, timeout_ms=-1):
    """Receive a datagram."""
    event = __bp_event_poll([handle], timeout_ms)
    if event == None:
        return None
    if event["type"] == "error":
        fail("UDP recv failed: " + event["data"]["message"])
    return {
        "data": event["data"]["data"],
        "host": event["data"]["from_host"],
        "port": event["data"]["from_port"],
    }

def _udp_close(handle):
    """Close a UDP socket."""
    __bp_event_source_close(handle)

def _udp_request(host, port, data, bind_port=0, timeout_ms=5000):
    """Make a simple UDP request/response."""
    sock = _udp_bind("0.0.0.0", bind_port)
    _udp_send_to(sock, data, host, port)
    response = _udp_recv_from(sock, timeout_ms)
    _udp_close(sock)
    return response

udp = struct(
    bind = _udp_bind,
    send_to = _udp_send_to,
    recv_from = _udp_recv_from,
    close = _udp_close,
    request = _udp_request,
)

# === Socket (cross-platform: Unix domain sockets / Windows named pipes) ===

def _socket_connect(path):
    """Connect to a local socket.

    On Unix: connects to a Unix domain socket at the given path.
    On Windows: connects to a named pipe at the given path.
    """
    return __bp_event_source("socket_connect", {"path": path})

def _socket_listen(path):
    """Start a local socket listener.

    On Unix: creates a Unix domain socket at the given path.
    On Windows: creates a named pipe at the given path.
    """
    return __bp_event_source("socket_listen", {"path": path})

def _socket_send(handle, data):
    """Send data on a socket connection."""
    return __bp_event_write(handle, data)

def _socket_recv(handle, timeout_ms=-1):
    """Receive data from a socket connection."""
    event = __bp_event_poll([handle], timeout_ms)
    if event == None:
        return None
    if event["type"] == "closed":
        return None
    if event["type"] == "error":
        fail("Socket recv failed: " + event["data"]["message"])
    return event["data"]["data"]

def _socket_close(handle):
    """Close a socket connection or listener."""
    __bp_event_source_close(handle)

def _socket_accept(listener_handle, timeout_ms=-1):
    """Accept a connection on a socket listener."""
    event = __bp_event_poll([listener_handle], timeout_ms)
    if event == None:
        return None
    if event["type"] == "error":
        fail("Socket accept failed: " + event["data"]["message"])
    return event["data"]["client_handle"]

def _socket_request(path, data, timeout_ms=30000):
    """Make a simple socket request/response."""
    conn = _socket_connect(path)
    _socket_send(conn, data)
    response = _socket_recv(conn, timeout_ms)
    _socket_close(conn)
    return response

socket = struct(
    connect = _socket_connect,
    listen = _socket_listen,
    send = _socket_send,
    recv = _socket_recv,
    close = _socket_close,
    accept = _socket_accept,
    request = _socket_request,
)
