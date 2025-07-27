import os
from dotenv import load_dotenv

load_dotenv()

def load_config():
    return {
        "DATABASE_URL": os.getenv("DATABASE_URL"),
        "JWT_SECRET": os.getenv("JWT_SECRET"),
        "JWT_EXPIRATION_SECONDS": int(os.getenv("JWT_EXPIRATION_SECONDS", "3600")),
    }
