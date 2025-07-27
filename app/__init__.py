from quart import Quart
from .config import load_config
from .db import init_db
from .routes import register_routes
from quart_cors import cors
import os

def create_app():
    app = Quart(__name__)
    app.config.from_mapping(load_config())
    app = cors(app, allow_origin="http://localhost:3000", allow_credentials=True)

    @app.before_serving
    async def startup():
        await init_db()

    register_routes(app)
    return app
