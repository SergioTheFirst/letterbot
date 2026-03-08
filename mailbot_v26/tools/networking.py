from __future__ import annotations

import ipaddress
import socket


def _is_private_ipv4(value: str) -> bool:
    try:
        return ipaddress.ip_address(value).is_private
    except ValueError:
        return False


def get_primary_ipv4() -> str | None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        candidate = sock.getsockname()[0]
        if candidate and candidate != "0.0.0.0":
            return candidate
    except OSError:
        candidate = None
    finally:
        sock.close()

    hostname = socket.gethostname()
    try:
        addresses = socket.getaddrinfo(
            hostname, None, socket.AF_INET, socket.SOCK_DGRAM
        )
    except OSError:
        return None
    for item in addresses:
        candidate = item[4][0]
        if _is_private_ipv4(candidate):
            return candidate
    return None


__all__ = ["get_primary_ipv4"]
