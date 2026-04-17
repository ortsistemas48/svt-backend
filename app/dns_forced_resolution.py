"""
Workaround temporal: forzar resolución DNS para un host concreto.

DigitalOcean App Platform a veces devuelve [Errno -3] Temporary failure in name resolution
para ciertos dominios; el resolver público (p. ej. 8.8.8.8) resuelve bien.

Este módulo reemplaza socket.getaddrinfo por un envoltorio que, solo dentro del
context manager `forced_dns_resolution`, devuelve direcciones estáticas para el
host indicado. El resto de resoluciones delegan en getaddrinfo original.

Se usa una pila por contexto (contextvars) en lugar de restaurar el global en cada
salida: así no se rompe la concurrencia en asyncio/Quart cuando varias peticiones
entrelazan llamadas.
"""

from __future__ import annotations

import contextvars
import logging
import socket
from collections.abc import Sequence
from contextlib import contextmanager
from typing import Any

log = logging.getLogger(__name__)

_original_getaddrinfo = socket.getaddrinfo

# Pila de (host_normalizado, tuple[ips...]) por contexto de ejecución (async-safe).
_forced_stack: contextvars.ContextVar[tuple[tuple[str, tuple[str, ...]], ...]] = (
    contextvars.ContextVar("forced_dns_stack", default=())
)


def _normalize_host(host: object) -> str:
    if isinstance(host, bytes):
        host_s = host.decode("idna")
    else:
        host_s = str(host)
    return host_s.lower().rstrip(".")


def _maybe_synthetic_addrinfo(
    host: object,
    port: Any,
    family: int,
    socktype: int,
    proto: int,
    flags: int,
    forced_host: str,
    static_ips: tuple[str, ...],
) -> list[tuple[int, int, int, str, tuple[str, int]]] | None:
    host_key = _normalize_host(host)
    if host_key != forced_host:
        return None

    try:
        port_num = int(port)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None

    if family not in (0, socket.AF_UNSPEC, socket.AF_INET):
        return None

    st = socktype or socket.SOCK_STREAM
    pr = proto or 0
    out: list[tuple[int, int, int, str, tuple[str, int]]] = []
    for ip in static_ips:
        out.append((socket.AF_INET, st, pr, "", (ip, port_num)))
    log.debug(
        "DNS workaround: getaddrinfo sintético host=%s port=%s → %s",
        host_key,
        port_num,
        static_ips,
    )
    return out


def _patched_getaddrinfo(
    host: Any,
    port: Any,
    family: int = 0,
    type: int = 0,
    proto: int = 0,
    flags: int = 0,
) -> list[tuple[int, int, int, str, Any]]:
    stack = _forced_stack.get()
    if stack and host is not None:
        host_key = _normalize_host(host)
        for forced_host, static_ips in reversed(stack):
            if host_key == forced_host:
                synthetic = _maybe_synthetic_addrinfo(
                    host, port, family, type, proto, flags, forced_host, static_ips
                )
                if synthetic is not None:
                    return synthetic
                break
    return _original_getaddrinfo(host, port, family, type, proto, flags)


# Instalar una sola vez: el envoltorio delega al original salvo en contexto forzado.
socket.getaddrinfo = _patched_getaddrinfo  # type: ignore[assignment]


@contextmanager
def forced_dns_resolution(host: str, static_ips: Sequence[str]):
    """
    Durante el bloque, `host` resuelve únicamente a las IPs dadas (en orden:
    primario, luego fallback). Solo afecta a ese hostname exacto; el resto de
    llamadas a getaddrinfo no cambian.
    """
    host_norm = _normalize_host(host)
    ips_tuple = tuple(static_ips)
    if not host_norm or not ips_tuple:
        yield
        return

    prev = _forced_stack.get()
    token = _forced_stack.set(prev + ((host_norm, ips_tuple),))
    log.warning(
        "DNS workaround ACTIVO (DO App Platform): resolución forzada host=%s ips=%s",
        host_norm,
        ips_tuple,
    )
    try:
        yield
    finally:
        _forced_stack.reset(token)
        log.warning(
            "DNS workaround DESACTIVADO para host=%s",
            host_norm,
        )
