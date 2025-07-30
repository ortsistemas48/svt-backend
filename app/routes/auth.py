from quart import Blueprint, request, jsonify, current_app, g
from passlib.hash import bcrypt
from app.db import get_conn
import jwt
import datetime
from quart.wrappers.response import Response
from quart.utils import run_sync

auth_bp = Blueprint("auth", __name__)

# Registro
@auth_bp.route("/register", methods=["POST"])
async def register():
    """Los campos requeridos para registrar son estos:
    {
    "email": "ejemplo@mail.com",
    "password": "123456",
    "confirm_password": "123456",
    "first_name": "Juan",
    "last_name": "Pérez",
    "dni": 12345678,
    "phone_number": "1234567890",
    "workshop_id": 1,
    "user_type_id": 2
    }"""
    data = await request.get_json()
    email = data.get("email")
    password = data.get("password")
    confirm_password = data.get("confirm_password")
    first_name = data.get("first_name")
    last_name = data.get("last_name")
    dni = data.get("dni")
    phone_number = data.get("phone_number")

    workshop_id = data.get("workshop_id")
    user_type_id = data.get("user_type_id")

    if not all([email, password, confirm_password, first_name, last_name, workshop_id, user_type_id]):
        return jsonify({"error": "Todos los campos obligatorios deben completarse"}), 400

    if password != confirm_password:
        return jsonify({"error": "Las contraseñas no coinciden"}), 400

    conn = await get_conn()
    existing_user = await conn.fetchrow("SELECT 1 FROM users WHERE email = $1", email)
    if existing_user:
        return jsonify({"error": "El email ya está en uso"}), 400

    hashed_password = bcrypt.hash(password)

    async with conn.transaction():
        user_row = await conn.fetchrow(
            """
            INSERT INTO users (email, first_name, last_name, phone_number, dni, password, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
            RETURNING id
            """,
            email, first_name, last_name, phone_number, dni, hashed_password
        )
        user_id = user_row["id"]

        await conn.execute(
            """
            INSERT INTO workshop_users (workshop_id, user_id, user_type_id, created_at)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
            """,
            workshop_id, user_id, user_type_id
        )

    return jsonify({"message": "Usuario registrado correctamente", "user_id": str(user_id)}), 201


# Login
@auth_bp.route("/login", methods=["POST"])
async def login():
    data = await request.get_json()
    identifier = data.get("email")
    password = data.get("password")

    if not identifier or not password:
        return jsonify({"error": "Email o DNI y contraseña requeridos"}), 400

    conn = await get_conn()
    user = await conn.fetchrow(
        "SELECT * FROM users WHERE email = $1 OR dni = $1", identifier
    )

    if not user or not bcrypt.verify(password, user["password"]):
        return jsonify({"error": "Credenciales inválidas"}), 401

    payload = {
        "user_id": str(user["id"]),
        "exp": datetime.datetime.utcnow() + datetime.timedelta(days=1)
    }
    token = jwt.encode(payload, current_app.config["JWT_SECRET"], algorithm="HS256")

    response: Response = await run_sync(jsonify)({
        "message": "Login exitoso",
        "user": {
            "id": str(user["id"]),
            "email": user["email"],
            "first_name": user["first_name"],
            "last_name": user["last_name"],
            "avatar": user["avatar"]
        }
    })

    response.set_cookie(
        "token",
        token,
        httponly=True,
        samesite="Lax",
        secure=False
    )
    return response


# Obtener sesión actual
@auth_bp.route("/me", methods=["GET"])
async def me():
    token = request.cookies.get("token")
    if not token:
        return jsonify({"error": "No autenticado"}), 401

    try:
        payload = jwt.decode(token, current_app.config["JWT_SECRET"], algorithms=["HS256"])
        user_id = payload["user_id"]
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token expirado"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Token inválido"}), 401

    conn = await get_conn()
    user = await conn.fetchrow("""
        SELECT id, email, first_name, last_name, phone_number, avatar
        FROM users
        WHERE id = $1
    """, user_id)

    if not user:
        return jsonify({"error": "Usuario no encontrado"}), 404

    workshops = await conn.fetch("""
        SELECT wu.workshop_id, w.name AS workshop_name, ut.name AS role
        FROM workshop_users wu
        JOIN workshop w ON wu.workshop_id = w.id
        JOIN user_types ut ON wu.user_type_id = ut.id
        WHERE wu.user_id = $1
    """, user_id)

    return jsonify({
        "user": dict(user),
        "workshops": [dict(w) for w in workshops]
    })


# Logout
@auth_bp.route("/logout", methods=["POST"])
async def logout():
    response: Response = await run_sync(jsonify)({"message": "Sesión cerrada correctamente"})
    response.delete_cookie("token")
    return response
