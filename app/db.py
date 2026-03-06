import asyncpg
import os
import ssl
from contextlib import asynccontextmanager

db_pool: asyncpg.Pool | None = None

async def init_db() -> None:

    global db_pool

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    db_pool = await asyncpg.create_pool(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 5432)),
        ssl=ssl_ctx,
        # PgBouncer transaction-mode settings
        statement_cache_size=0,
        max_inactive_connection_lifetime=300.0,
        command_timeout=60.0,
        min_size=0,
        max_size=int(os.getenv("DB_POOL_MAX_SIZE", 10)),
    )

def _assert_pool():
    if db_pool is None:
        raise RuntimeError("DB pool no inicializado, llamá a init_db() primero")
    return db_pool

# Evitá exponer una conexión suelta, en transaction pooling no hay estado de sesión
# Usá siempre context managers que abren y cierran transacciones cortas
@asynccontextmanager
async def get_conn_ctx():
    """
    Adquiere una conexión del pool.
    El manejo de transacciones queda a cargo del llamador.
    """
    pool = _assert_pool()
    async with pool.acquire(timeout=10.0) as conn:
        try:
            yield conn
        finally:
            pass
        
# Helpers convenientes para consultas simples
# Cada llamada adquiere y libera conexión, útil para operaciones de una sola query

async def fetch(query: str, *args):
    pool = _assert_pool()
    return await pool.fetch(query, *args)

async def fetchrow(query: str, *args):
    pool = _assert_pool()
    return await pool.fetchrow(query, *args)

async def fetchval(query: str, *args):
    pool = _assert_pool()
    return await pool.fetchval(query, *args)

async def execute(query: str, *args):
    pool = _assert_pool()
    return await pool.execute(query, *args)

async def close_db() -> None:
    global db_pool
    if db_pool is not None:
        await db_pool.close()
        db_pool = None
