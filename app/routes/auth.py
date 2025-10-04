from quart import Blueprint, request, jsonify, current_app, g, redirect
from passlib.hash import bcrypt
from app.db import get_conn_ctx
import jwt
import datetime
from quart.wrappers.response import Response
from quart.utils import run_sync
import os
from app.email import generate_email_token, send_verification_email, send_account_credentials_email,send_assigned_to_workshop_email
import logging
import pytz

log = logging.getLogger(__name__)

auth_bp = Blueprint("auth", __name__)

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")
EMAIL_PLAIN_PASSWORDS = True
ENGINEER_ROLE_ID = 3

@auth_bp.route("/register", methods=["POST"])
async def register():
    """
    Requeridos:
      email, password, confirm_password, first_name, last_name, workshop_id, user_type_id
    Opcionales:
      dni, phone_number
    Si el rol es Ingeniero (id=3), opcionales/adicionales:
      license_number, title_name, engineer_kind ("Titular" | "Suplente")
    """
    data = await request.get_json()

    email            = (data.get("email") or "").strip().lower()
    password         = data.get("password") or ""
    confirm_password = data.get("confirm_password") or ""
    first_name       = (data.get("first_name") or "").strip()
    last_name        = (data.get("last_name") or "").strip()
    dni              = data.get("dni")
    phone_number     = data.get("phone_number")
    workshop_id      = data.get("workshop_id")
    user_type_id     = data.get("user_type_id")  # se usa sólo para saber si es ingeniero

    license_number = data.get("license_number") or data.get("nro_matricula")
    title_name     = data.get("title_name")     or data.get("titulo_universitario")
    engineer_kind  = data.get("engineer_kind")  # "Titular" | "Suplente" (solo ing)

    if not all([email, password, confirm_password, first_name, last_name, workshop_id, user_type_id]):
        return jsonify({"error": "Todos los campos obligatorios deben completarse"}), 400

    if password != confirm_password:
        return jsonify({"error": "Las contraseñas no coinciden"}), 400

    is_engineer = int(user_type_id) == ENGINEER_ROLE_ID
    if not is_engineer:
        license_number = None
        title_name = None
        engineer_kind = None
    else:
        # Validaciones de ingeniero
        if not license_number or not title_name:
            return jsonify({"error": "Para Ingeniero, completá matrícula y título"}), 400
        if engineer_kind not in ("Titular", "Suplente"):
            return jsonify({"error": "Elegí si el ingeniero es Titular o Suplente"}), 400

    inviter_id = g.get("user_id")  # opcional, quien crea el usuario

    async with get_conn_ctx() as conn:
        # Validaciones previas
        existing_user = await conn.fetchrow("SELECT 1 FROM users WHERE email = $1", email)
        if existing_user:
            return jsonify({"error": "El email ya está en uso"}), 400

        ws = await conn.fetchrow("SELECT id, name FROM workshop WHERE id = $1", workshop_id)
        if not ws:
            return jsonify({"error": "Workshop no encontrado"}), 404

        # nombre del invitador
        inviter_name = None
        if inviter_id:
            inv = await conn.fetchrow(
                "SELECT first_name, last_name FROM users WHERE id = $1",
                inviter_id
            )
            if inv:
                fn = (inv.get("first_name") or "").strip()
                ln = (inv.get("last_name") or "").strip()
                inviter_name = (" ".join([x for x in (fn, ln) if x])).strip() or None

        hashed_password = bcrypt.hash(password)

        async with conn.transaction():
            user_row = await conn.fetchrow(
                """
                INSERT INTO users (
                    email, first_name, last_name, phone_number, dni, password,
                    license_number, title_name,
                    created_at, is_active, is_approved
                )
                VALUES ($1, $2, $3, $4, $5, $6,
                        $7, $8,
                        CURRENT_TIMESTAMP, true, true)
                RETURNING id, first_name, last_name
                """,
                email, first_name, last_name, phone_number, dni, hashed_password,
                license_number, title_name
            )
            user_id = user_row["id"]

            # Relación workshop_users SIN user_type_id, sólo engineer_kind
            await conn.execute(
                """
                INSERT INTO workshop_users (workshop_id, user_id, engineer_kind, created_at)
                VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                ON CONFLICT (workshop_id, user_id)
                DO UPDATE SET engineer_kind = EXCLUDED.engineer_kind
                """,
                workshop_id, user_id, engineer_kind
            )

    # Emails fuera de la transacción
    full_name = f"{first_name} {last_name}".strip()

    # 1) Credenciales
    try:
        displayed_password = password if EMAIL_PLAIN_PASSWORDS else "Error"
        login_url = f"{FRONTEND_URL}/login"
        force_reset_url = None if EMAIL_PLAIN_PASSWORDS else f"{FRONTEND_URL}/reset-password?email={email}"

        await send_account_credentials_email(
            to_email=email,
            full_name=full_name or None,
            login_email=email,
            temp_password=displayed_password,
            login_url=login_url,
            force_reset_url=force_reset_url,
        )
    except Exception as e:
        log.exception("No se pudo enviar email de credenciales a %s, error: %s", email, e)

    # 2) Asignación a taller (rol descriptivo derivado)
    try:
        workshop_url = f"{FRONTEND_URL}/dashboard/{workshop_id}"
        role_name = None
        if is_engineer:
            role_name = f"Ingeniero {engineer_kind}"
        await send_assigned_to_workshop_email(
            to_email=email,
            workshop_name=ws["name"],
            role_name=role_name,
            inviter_name=inviter_name,
            workshop_url=workshop_url,
        )
    except Exception as e:
        log.exception("No se pudo enviar email de asignación a %s, error: %s", email, e)

    return jsonify({"message": "Usuario registrado correctamente", "user_id": str(user_id)}), 201


@auth_bp.route("/register_bulk", methods=["POST"])
async def register_bulk():
    """
    Requeridos:
      email, password, confirm_password, first_name, last_name, workshop_id, user_type_id
    Opcionales:
      dni, phone_number
    Si el rol es Ingeniero (id=3), adicionales:
      license_number, title_name, engineer_kind ("Titular" | "Suplente")
    """
    data = await request.get_json()

    email            = (data.get("email") or "").strip().lower()
    password         = data.get("password") or ""
    confirm_password = data.get("confirm_password") or ""
    first_name       = (data.get("first_name") or "").strip()
    last_name        = (data.get("last_name") or "").strip()
    dni              = (data.get("dni") or None)
    phone_number     = (data.get("phone_number") or None)
    workshop_id      = data.get("workshop_id")
    user_type_id     = data.get("user_type_id")  # se usa sólo para saber si es ingeniero

    license_number = data.get("license_number") or data.get("nro_matricula")
    title_name     = data.get("title_name")     or data.get("titulo_universitario")
    engineer_kind  = data.get("engineer_kind")

    if not all([email, password, confirm_password, first_name, last_name, workshop_id, user_type_id]):
        return jsonify({"error": "Todos los campos obligatorios deben completarse"}), 400

    if password != confirm_password:
        return jsonify({"error": "Las contraseñas no coinciden"}), 400

    is_engineer = int(user_type_id) == ENGINEER_ROLE_ID
    if not is_engineer:
        license_number = None
        title_name = None
        engineer_kind = None
    else:
        if not license_number or not title_name:
            return jsonify({"error": "Para Ingeniero, completá matrícula y título"}), 400
        if engineer_kind not in ("Titular", "Suplente"):
            return jsonify({"error": "Elegí si el ingeniero es Titular o Suplente"}), 400

    inviter_id = g.get("user_id")  # quién ejecuta el alta, opcional

    async with get_conn_ctx() as conn:
        # Validaciones previas y datos auxiliares
        existing_user = await conn.fetchrow("SELECT 1 FROM users WHERE email = $1", email)
        if existing_user:
            return jsonify({"error": "El email ya está en uso"}), 400

        ws = await conn.fetchrow("SELECT id, name FROM workshop WHERE id = $1", workshop_id)
        if not ws:
            return jsonify({"error": "Workshop no encontrado"}), 404

        # nombre del invitador
        inviter_name = None
        if inviter_id:
            inv = await conn.fetchrow(
                "SELECT first_name, last_name FROM users WHERE id = $1",
                inviter_id
            )
            if inv:
                fn = (inv.get("first_name") or "").strip()
                ln = (inv.get("last_name") or "").strip()
                inviter_name = (" ".join([x for x in (fn, ln) if x])).strip() or None

        hashed_password = bcrypt.hash(password)

        async with conn.transaction():
            user_row = await conn.fetchrow(
                """
                INSERT INTO users (
                    email, first_name, last_name, phone_number, dni, password,
                    license_number, title_name,
                    created_at, is_active, is_approved
                )
                VALUES ($1, $2, $3, $4, $5, $6,
                        $7, $8,
                        CURRENT_TIMESTAMP, false, false)
                RETURNING id, first_name, last_name
                """,
                email, first_name, last_name, phone_number, dni, hashed_password,
                license_number, title_name
            )
            user_id = user_row["id"]

            # Relación workshop_users SIN user_type_id
            await conn.execute(
                """
                INSERT INTO workshop_users (workshop_id, user_id, engineer_kind, created_at)
                VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                ON CONFLICT (workshop_id, user_id) DO UPDATE SET engineer_kind = EXCLUDED.engineer_kind
                """,
                workshop_id, user_id, engineer_kind
            )

    # =========================
    # Envío de emails (fuera TX)
    # =========================
    full_name = f"{first_name} {last_name}".strip()
    try:
        # 1) Credenciales de cuenta
        displayed_password = password if EMAIL_PLAIN_PASSWORDS else "definida por vos"
        login_url = f"{FRONTEND_URL}/login"
        force_reset_url = f"{FRONTEND_URL}/reset-password?email={email}"

        await send_account_credentials_email(
            to_email=email,
            full_name=full_name or None,
            login_email=email,
            temp_password=displayed_password,
            login_url=login_url,
            force_reset_url=force_reset_url if not EMAIL_PLAIN_PASSWORDS else None,
        )
    except Exception as e:
        log.exception("No se pudo enviar email de credenciales a %s, error: %s", email, e)

    try:
        # 2) Asignación a taller (rol descriptivo derivado)
        workshop_url = f"{FRONTEND_URL}/dashboard/{workshop_id}"
        role_name = f"Ingeniero {engineer_kind}" if is_engineer else None
        await send_assigned_to_workshop_email(
            to_email=email,
            workshop_name=ws["name"],
            role_name=role_name,
            inviter_name=inviter_name,
            workshop_url=workshop_url,
        )
    except Exception as e:
        log.exception("No se pudo enviar email de asignación a %s, error: %s", email, e)

    return jsonify({"message": "Usuario registrado correctamente", "user_id": str(user_id)}), 201


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

        # fechas aware - usar UTC para tokens de sistema
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
                ) VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, false, true)
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

    return jsonify({"message": "Email de verificación enviado, apruebalo para ingresar."}), 201


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
    ENV = os.getenv("ENV", "development")
    if ENV == "production":
        response.set_cookie(
            "token",
            token,
            httponly=True,
            samesite="None",   
            secure=True       
        )
    else:
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

    async with get_conn_ctx() as conn:
        user = await conn.fetchrow("""
            SELECT id, email, first_name, dni, last_name, phone_number, is_admin
            FROM users
            WHERE id = $1
        """, user_id)

        if not user:
            return jsonify({"error": "Usuario no encontrado"}), 404

        workshops = await conn.fetch("""
            SELECT wu.workshop_id, w.name AS workshop_name, ut.name AS role, wu.user_type_id, w.is_approved as is_approved
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
