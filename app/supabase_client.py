"""
Cliente Supabase centralizado + workaround DNS para App Platform.

El parche solo aplica cuando SUPABASE_URL apunta al proyecto afectado; otros
entornos (otro host Supabase) no ven resolución forzada.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from urllib.parse import urlparse

from supabase import Client, create_client

from app.dns_forced_resolution import forced_dns_resolution

log = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Host del proyecto con fallos intermitentes de DNS en DigitalOcean App Platform.
_SUPABASE_DO_DNS_PATCH_HOST = "uedevplogwlaueyuofft.supabase.co"
# Cloudflare Anycast: primario y fallback (orden probado por create_connection).
_SUPABASE_DO_DNS_PATCH_IPS = ("104.18.38.10", "172.64.149.246")


def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


@contextmanager
def supabase_dns_workaround():
    """
    Activa la resolución DNS forzada solo para llamadas HTTP al host de Supabase
    configurado en SUPABASE_URL, y solo si coincide con el proyecto conocido.
    """
    hostname = (urlparse(SUPABASE_URL or "").hostname or "").lower().rstrip(".")
    if hostname != _SUPABASE_DO_DNS_PATCH_HOST:
        yield
        return
    log.info(
        "Supabase: aplicando workaround DNS temporal (hostname=%s)",
        hostname,
    )
    with forced_dns_resolution(hostname, _SUPABASE_DO_DNS_PATCH_IPS):
        yield
