import asyncpg
import os 
from contextlib import asynccontextmanager

db_pool = None


async def init_db():
    print("Intentando conexión con:")
    print("USER:", os.getenv("DB_USER"))
    print("PASSWORD:", os.getenv("DB_PASSWORD"))
    print("DATABASE:", os.getenv("DB_NAME"))
    print("HOST:", os.getenv("DB_HOST"))
    print("PORT:", os.getenv("DB_PORT"))

    global db_pool
    db_pool = await asyncpg.create_pool(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 5432)),
    )
    
    
async def get_conn():
    if db_pool is None:
        raise Exception("DB pool no inicializado. ¿Llamaste a init_db()?")

    return await db_pool.acquire()


@asynccontextmanager
async def get_conn_ctx():
    conn = await db_pool.acquire()
    try:
        yield conn
    finally:
        await db_pool.release(conn)
