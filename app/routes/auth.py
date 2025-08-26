from quart import Blueprint, request, jsonify, current_app, g, redirect
from passlib.hash import bcrypt
from app.db import get_conn_ctx
import jwt
import datetime
from quart.wrappers.response import Response
from quart.utils import run_sync
import os
from app.email import generate_email_token, send_verification_email

auth_bp = Blueprint("auth", __name__)

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

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

    async with get_conn_ctx() as conn:
        existing_user = await conn.fetchrow("SELECT 1 FROM users WHERE email = $1", email)
        if existing_user:
            return jsonify({"error": "El email ya está en uso"}), 400

        hashed_password = bcrypt.hash(password)

        async with conn.transaction():
            user_row = await conn.fetchrow(
                """
                INSERT INTO users (
                    email, first_name, last_name, phone_number, dni, password,
                    created_at, is_active, is_approved
                )
                VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, true, true)
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



@auth_bp.route("/owner/register", methods=["POST"])
async def register_user():
    data = await request.get_json()

    email = data.get("email")
    password = data.get("password")
    confirm_password = data.get("confirm_password")
    first_name = data.get("first_name")
    last_name = data.get("last_name")
    dni = data.get("dni")
    phone_number = data.get("phone_number")

    # Validaciones mínimas
    if not all([email, password, confirm_password, first_name, last_name]):
        return jsonify({"error": "Todos los campos obligatorios deben completarse"}), 400

    if password != confirm_password:
        return jsonify({"error": "Las contraseñas no coinciden"}), 400

    async with get_conn_ctx() as conn:

        existing_user = await conn.fetchrow(
            "SELECT 1 FROM users WHERE email = $1",
            email
        )
        if existing_user:
            return jsonify({"error": "El email ya está en uso"}), 400

        hashed_password = bcrypt.hash(password)
        token = generate_email_token()
        expires_at = datetime.datetime.utcnow() + datetime.timedelta(hours=24)

        async with conn.transaction():
            user_row = await conn.fetchrow(
                """
                INSERT INTO users (
                    email, first_name, last_name, phone_number, dni,
                    password, created_at, is_active, is_approved
                ) VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, false, false)
                RETURNING id, email
                """,
                email, first_name, last_name, phone_number, dni, hashed_password
            )

            user_id = user_row["id"]

            await conn.execute(
                """
                INSERT INTO email_verification_tokens (user_id, token, expires_at)
                VALUES ($1, $2, $3)
                """,
                user_id, token, expires_at
            )

    # Envío del email de verificación
    try:
        await send_verification_email(email, token)
    except Exception:
        # Usuario queda creado y se puede reintentar el envío
        return jsonify({
            "message": "Usuario creado, no se pudo enviar el email de verificación, reintentalo más tarde"
        }), 201

    return jsonify({"message": "Email enviado, esperando verificación"}), 201


@auth_bp.route("/verify-email", methods=["GET"])
async def verify_email():
    token = request.args.get("token")
    mode = request.args.get("mode")  # <- agrega esto
    if not token:
        if mode == "json":
            return jsonify({"status": "invalid"}), 400
        return redirect(f"{FRONTEND_URL}/email-verified?status=invalid")

    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT evt.id, evt.user_id, evt.expires_at, evt.used_at, u.is_active, u.email
            FROM email_verification_tokens evt
            JOIN users u ON u.id = evt.user_id
            WHERE evt.token = $1
            """,
            token
        )
        print(row)

        def out(status: str, email: str | None = None):
            if mode == "json":
                return jsonify({"status": status, "email": email})
            suffix = "/email-verified?status=" + status
            if email:
                suffix += f"&email={email}"
            return redirect(f"{FRONTEND_URL}{suffix}")

        if not row:
            return out("invalid")

        # fechas aware
        def to_aware_utc(dt: datetime.datetime) -> datetime.datetime:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=datetime.timezone.utc)
            return dt.astimezone(datetime.timezone.utc)

        now_utc = datetime.datetime.now(datetime.timezone.utc)

        if row["used_at"] is not None:
            return out("used", row["email"])

        expires_at = to_aware_utc(row["expires_at"])
        if expires_at < now_utc:
            return out("expired", row["email"])

        async with conn.transaction():
            await conn.execute(
                "UPDATE email_verification_tokens SET used_at = now() WHERE id = $1",
                row["id"]
            )
            await conn.execute(
                "UPDATE users SET is_active = true WHERE id = $1",
                row["user_id"]
            )

    return out("ok", row["email"])


@auth_bp.route("/resend-verification", methods=["POST"])
async def resend_verification():
    data = await request.get_json()
    email = data.get("email")

    if not email:
        return jsonify({"error": "Email requerido"}), 400

    async with get_conn_ctx() as conn:
        user = await conn.fetchrow("""
            SELECT id, is_active
            FROM users
            WHERE email = $1
        """, email)

        if not user:
            return jsonify({"error": "Usuario no encontrado"}), 404

        if user["is_active"]:
            return jsonify({"message": "Tu email ya está verificado"}), 200

        # cooldown 5 minutos usando expires_at como proxy de created_at
        # si expires_at > now() + 23h55m, se creó hace menos de 5 min
        too_soon = await conn.fetchval("""
            SELECT EXISTS (
              SELECT 1
              FROM email_verification_tokens
              WHERE user_id = $1
                AND used_at IS NULL
                AND expires_at > (now() AT TIME ZONE 'utc') + INTERVAL '23 hours 55 minutes'
            )
        """, user["id"])

        if too_soon:
            return jsonify({"error": "Esperá unos minutos antes de pedir otro email"}), 429

        # generar y guardar nuevo token
        token = generate_email_token()
        expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=24)

        await conn.execute("""
            INSERT INTO email_verification_tokens (user_id, token, expires_at)
            VALUES ($1, $2, $3)
        """, user["id"], token, expires_at)

    # enviar email fuera de la transacción
    try:
        await send_verification_email(email, token)
    except Exception:
        return jsonify({"error": "No se pudo enviar el email, intentá más tarde"}), 500

    return jsonify({"message": "Te enviamos un nuevo email de verificación"}), 200


# Login, bloquea si no activo o no aprobado
@auth_bp.route("/login", methods=["POST"])
async def login():
    data = await request.get_json()
    identifier = data.get("email")
    password = data.get("password")

    if not identifier or not password:
        return jsonify({"error": "Email o DNI y contraseña requeridos"}), 400

    async with get_conn_ctx() as conn:
        user = await conn.fetchrow(
            "SELECT * FROM users WHERE email = $1 OR dni = $1", identifier
        )

    if not user or not bcrypt.verify(password, user["password"]):
        return jsonify({"error": "Credenciales inválidas"}), 401

    if not user["is_active"]:
        return jsonify({"error": "Tu email no está verificado, revisá tu bandeja de entrada"}), 403
    if not user["is_approved"]:
        return jsonify({"error": "Estamos verificando tus datos, te avisaremos cuando puedas usar tu cuenta"}), 403

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
        }
    })
    response.set_cookie("token", token, httponly=True, samesite="Lax", secure=False)
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

    async with get_conn_ctx() as conn:
        user = await conn.fetchrow("""
            SELECT id, email, first_name, last_name, phone_number, is_admin
            FROM users
            WHERE id = $1
        """, user_id)

        if not user:
            return jsonify({"error": "Usuario no encontrado"}), 404

        workshops = await conn.fetch("""
            SELECT wu.workshop_id, w.name AS workshop_name, ut.name AS role, wu.user_type_id
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


@auth_bp.route("/<int:user_id>", methods=["PUT"])
async def update_user(user_id):
    token = request.cookies.get("token")
    if not token:
        return jsonify({"error": "No autenticado"}), 401

    try:
        payload = jwt.decode(token, current_app.config["JWT_SECRET"], algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token expirado"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Token inválido"}), 401

    data = await request.get_json()
    first_name = data.get("first_name")
    last_name = data.get("last_name")
    phone_number = data.get("phone_number")

    if not any([first_name, last_name, phone_number]):
        return jsonify({"error": "No se enviaron campos para actualizar"}), 400

    async with get_conn_ctx() as conn:
        user = await conn.fetchrow("SELECT id FROM users WHERE id = $1", user_id)
        if not user:
            return jsonify({"error": "Usuario no encontrado"}), 404

        await conn.execute("""
            UPDATE users
            SET first_name = COALESCE($1, first_name),
                last_name = COALESCE($2, last_name),
                phone_number = COALESCE($3, phone_number)
            WHERE id = $4
        """, first_name, last_name, phone_number, user_id)

    return jsonify({"message": "Usuario actualizado correctamente"})


@auth_bp.route("/<int:user_id>", methods=["DELETE"])
async def delete_user(user_id):
    token = request.cookies.get("token")
    if not token:
        return jsonify({"error": "No autenticado"}), 401

    try:
        payload = jwt.decode(token, current_app.config["JWT_SECRET"], algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token expirado"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Token inválido"}), 401

    async with get_conn_ctx() as conn:
        user = await conn.fetchrow("SELECT id FROM users WHERE id = $1", user_id)
        if not user:
            return jsonify({"error": "Usuario no encontrado"}), 404

        await conn.execute("DELETE FROM workshop_users WHERE user_id = $1", user_id)
        await conn.execute("DELETE FROM users WHERE id = $1", user_id)

    return jsonify({"message": "Usuario eliminado correctamente"})


@auth_bp.route("/<int:user_id>/approve", methods=["POST"])
async def approve_user(user_id):
    # acá validar que el caller sea admin
    async with get_conn_ctx() as conn:
        await conn.execute("UPDATE users SET is_approved = true WHERE id = $1", user_id)
    return jsonify({"message": "Usuario aprobado"})
