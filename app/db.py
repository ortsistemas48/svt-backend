import asyncpg
import os 

db_pool = None


async def init_db():
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
