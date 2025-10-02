from quart import Quart, request, g, current_app
from .config import load_config
from .db import init_db
from .routes import register_routes
from quart_cors import cors
import os
import jwt

def create_app():
    app = Quart(__name__)
    app.config.from_mapping(load_config())

    @app.before_serving
    async def startup():
        print(">> Startup iniciado")
        await init_db()
        print(">> Startup completado")

    @app.before_request
    async def load_user():
        token = request.cookies.get("token")
        g.user_id = None  # Siempre lo inicializamos por las dudas

        if not token:
            return

        try:
            payload = jwt.decode(token, current_app.config["JWT_SECRET"], algorithms=["HS256"])
            g.user_id = payload.get("user_id")
        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
            pass

    register_routes(app)

    app = cors(
        app,
        allow_origin=[
            "http://localhost:3000",
            "https://svt-frontend.vercel.app"
        ],
        allow_credentials=True
    )
    return app
