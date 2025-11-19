# app/email.py
import os, secrets, httpx, logging
from typing import Optional

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
RESEND_FROM = os.getenv("RESEND_FROM", "no-reply@example.com")
FRONTEND_URL = os.getenv("FRONTEND_URL", "https://www.checkrto.com")
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:5000")

log = logging.getLogger("email")

def generate_email_token() -> str:
    return secrets.token_urlsafe(32)

# =========================
# Helpers
# =========================
def _wrap_html(title: str, intro: str, cta_text: Optional[str] = None, cta_url: Optional[str] = None, extra_html: str = "") -> str:
    cta_block = ""
    if cta_text and cta_url:
        cta_block = f"""
          <a href="{cta_url}" style="display:inline-block; padding: 12px 20px; border-radius: 6px; background: #0040B8; color: #fff; text-decoration: none; font-weight: 600; font-size: 15px; box-shadow: 0 2px 6px rgba(0,0,0,0.15);">
            {cta_text}
          </a>
          <p style="margin-top: 16px; font-size: 14px; color: #777;">
            Si no podés hacer clic, copiá y pegá este enlace en tu navegador:
          </p>
          <p style="word-break: break-all; font-size: 14px;">
            <a href="{cta_url}" style="color: #0040B8; text-decoration: underline;">{cta_url}</a>
          </p>
        """

    return f"""
      <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 520px; margin: 0 auto; background-color: #f9f9f9; border-radius: 10px; padding: 24px; border: 1px solid #e0e0e0;">
        <div style="text-align: center;">
          <h2 style="color: #0040B8; margin-bottom: 8px;">{title}</h2>
          <p style="color: #555; font-size: 15px; margin-bottom: 20px;">{intro}</p>
          {cta_block}
          {extra_html}
          <hr style="margin: 24px 0; border: 0; border-top: 1px solid #e6e6e6;" />
          <p style="font-size: 12px; color: #999; text-align: center;">
            Este es un mensaje automático, no respondas a este correo.
          </p>
        </div>
      </div>
    """

async def _send_email(to_email: str, subject: str, html: str):
    if not RESEND_API_KEY:
        log.error("RESEND_API_KEY no configurado, no se puede enviar email")
        raise RuntimeError("Falta RESEND_API_KEY")
    if not RESEND_FROM:
        log.error("RESEND_FROM no configurado")
        raise RuntimeError("Falta RESEND_FROM")

    payload = {
        "from": RESEND_FROM,
        "to": [to_email],
        "subject": subject,
        "html": html,
    }
    log.info("Enviando email a %s con subject='%s' desde '%s'", to_email, subject, RESEND_FROM)

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
                json=payload,
            )
    except httpx.RequestError as e:
        # errores de red, DNS, TLS, timeout
        log.exception("Error de red enviando email a %s: %s", to_email, e)
        raise

    if r.status_code >= 400:
        # log detallado del cuerpo para diagnosticar 401/422
        log.error("Resend devolvió %s al enviar a %s. Body=%s", r.status_code, to_email, r.text)
        r.raise_for_status()

    log.info("Email enviado ok a %s. Respuesta=%s", to_email, r.text[:500])
    return True

# =========================
# 1) Verificación de email
# =========================
async def send_verification_email(to_email: str, token: str):
    verify_url = f"{FRONTEND_URL}/email-verified?token={token}"
    subject = "Verificá tu email"
    html = _wrap_html(
        title="Verificá tu email",
        intro="Hacé clic en el botón para verificar tu cuenta y empezar a usar todos nuestros servicios.",
        cta_text="Verificar email",
        cta_url=verify_url,
    )
    return await _send_email(to_email, subject, html)

# ================================================
# 2) Taller creado pendiente de aprobación
# ================================================
async def send_workshop_pending_email(to_email: str, workshop_name: str, review_url: Optional[str] = None):
    # review_url podría ser una página con estado del taller
    url = review_url or f"{FRONTEND_URL}/select-workshop"
    subject = "Tu taller fue creado y está pendiente de aprobación"
    intro = f"Recibimos la solicitud para crear el taller {workshop_name}. Estamos revisando la información. Te avisaremos por email cuando quede aprobado."
    html = _wrap_html(
        title="Taller pendiente de aprobación",
        intro=intro,
        cta_text="Ver estado del taller",
        cta_url=url,
    )
    return await _send_email(to_email, subject, html)

# ================================================
# 3) Taller aprobado
# ================================================
async def send_workshop_approved_email(to_email: str, workshop_name: str, workshop_id: Optional[str] = None):
    # Enlazamos directo al panel del taller si tenemos ID
    url = f"{FRONTEND_URL}/dashboard/{workshop_id}" if workshop_id else f"{FRONTEND_URL}/select-workshop"
    subject = "Tu taller fue aprobado"
    intro = f"Listo, aprobamos el taller {workshop_name}. Ya podés ingresar al panel para configurarlo y empezar a trabajar."
    html = _wrap_html(
        title="Taller aprobado",
        intro=intro,
        cta_text="Entrar al panel",
        cta_url=url,
    )
    return await _send_email(to_email, subject, html)

# ================================================
# 3b) Pago de orden aprobado
# ================================================
async def send_payment_order_approved_email(
    to_email: str,
    workshop_name: str,
    quantity: int,
    workshop_id: Optional[int] = None,
):
    url = f"{FRONTEND_URL}/dashboard/{workshop_id}/payment" if workshop_id else f"{FRONTEND_URL}/select-workshop"
    subject = "Aprobamos tu pago"
    intro = (
        f"Aprobamos y acreditamos tu pago por {quantity} revisiones del taller {workshop_name}. "
        "Ya podés continuar normalmente."
    )
    html = _wrap_html(
        title="Pago acreditado",
        intro=intro,
        cta_text="Ver órdenes de pago",
        cta_url=url,
    )
    return await _send_email(to_email, subject, html)

# ================================================
# 3c) Notificaciones para administradores
# ================================================
async def send_admin_workshop_registered_email(
    to_email: str,
    workshop_name: str,
    workshop_id: int,
):
    subject = "Nuevo taller registrado"
    intro = f"Se registró el taller {workshop_name} (ID {workshop_id}). Revisá los datos y aprobalo si corresponde."
    url = f"{FRONTEND_URL}/admin-dashboard/approve-workshops"
    html = _wrap_html(
        title="Nuevo taller registrado",
        intro=intro,
        cta_text="Abrir aprobaciones",
        cta_url=url,
    )
    return await _send_email(to_email, subject, html)

async def send_admin_payment_order_created_email(
    to_email: str,
    workshop_name: str,
    workshop_id: int,
    order_id: int,
    quantity: int,
    amount: float,
    zone: str,
):
    subject = "Nueva orden de pago registrada"
    intro = (
        f"Se creó la orden #{order_id} del taller {workshop_name} "
        f"por {quantity} revisiones, zona {zone}, monto ${amount:,.2f}."
    )
    url = f"{FRONTEND_URL}/admin-dashboard/payments"
    html = _wrap_html(
        title="Orden de pago registrada",
        intro=intro,
        cta_text="Ver pagos",
        cta_url=url,
    )
    return await _send_email(to_email, subject, html)

async def send_admin_ticket_created_email(
    to_email: str,
    ticket_id: int,
    workshop_id: int,
    subject_text: str,
):
    subject = "Nuevo ticket creado"
    intro = (
        f"Se creó el ticket #{ticket_id} para el taller {workshop_id} "
        f"con asunto: “{subject_text}”."
    )
    url = f"{FRONTEND_URL}/admin-dashboard/support/{ticket_id}"
    html = _wrap_html(
        title="Nuevo ticket",
        intro=intro,
        cta_text="Abrir ticket",
        cta_url=url,
    )
    return await _send_email(to_email, subject, html)

async def send_admin_ticket_message_email(
    to_email: str,
    ticket_id: int,
    workshop_id: int,
    message_preview: str,
):
    subject = "Nuevo mensaje en ticket"
    preview = (message_preview or "").strip()
    if len(preview) > 160:
        preview = preview[:157] + "..."
    intro = (
        f"Nuevo mensaje en el ticket #{ticket_id} (taller {workshop_id}). "
        f"Contenido: “{preview}”"
    )
    url = f"{FRONTEND_URL}/admin-dashboard/support/{ticket_id}"
    html = _wrap_html(
        title="Nuevo mensaje en ticket",
        intro=intro,
        cta_text="Ver conversación",
        cta_url=url,
    )
    return await _send_email(to_email, subject, html)

async def send_user_ticket_message_email(
    to_email: str,
    ticket_id: int,
    workshop_id: int,
    message_preview: str,
):
    subject = "Nuevo mensaje de soporte"
    preview = (message_preview or "").strip()
    if len(preview) > 160:
        preview = preview[:157] + "..."
    intro = (
        f"Recibiste un nuevo mensaje de soporte en el ticket #{ticket_id} para tu taller {workshop_id}. "
        f"Contenido: “{preview}”"
    )
    url = f"{FRONTEND_URL}/dashboard/{workshop_id}/help/{ticket_id}"
    html = _wrap_html(
        title="Nuevo mensaje de soporte",
        intro=intro,
        cta_text="Abrir conversación",
        cta_url=url,
    )
    return await _send_email(to_email, subject, html)

# ================================================
# 4) Email de credenciales al crear la cuenta
# ================================================
async def send_account_credentials_email(
    to_email: str,
    full_name: Optional[str],
    login_email: str,
    temp_password: str,
    login_url: Optional[str] = None,
    force_reset_url: Optional[str] = None,
):
    url = login_url or f"{FRONTEND_URL}/login"
    subject = "Tu cuenta fue creada"
    saludo = f"Hola {full_name}," if full_name else "Hola,"
    cred_block = f"""
      <div style="text-align: left; display: inline-block; margin-top: 12px; background: #fff; border: 1px solid #eee; border-radius: 8px; padding: 12px 14px;">
        <p style="margin: 0 0 8px; font-weight: 600;">Acceso</p>
        <p style="margin: 0;"><strong>Email:</strong> {login_email}</p>
        <p style="margin: 0;"><strong>Contraseña temporal:</strong> {temp_password}</p>
      </div>
      <p style="color: #777; font-size: 13px; margin-top: 14px;">
        Por seguridad, te vamos a pedir cambiar la contraseña al ingresar por primera vez.
      </p>
    """
    if force_reset_url:
        cred_block += f"""
          <p style="margin-top: 10px; font-size: 13px; color: #555;">
            También podés cambiarla desde aquí:
            <a href="{force_reset_url}" style="color: #0040B8; text-decoration: underline;">Restablecer contraseña</a>
          </p>
        """

    html = _wrap_html(
        title="Tu cuenta está lista",
        intro=f"{saludo} creamos tu cuenta para que puedas ingresar al panel. Guardá estas credenciales.",
        cta_text="Iniciar sesión",
        cta_url=url,
        extra_html=cred_block,
    )
    return await _send_email(to_email, subject, html)

# ================================================
# 5) Asignación a un taller
# ================================================
async def send_assigned_to_workshop_email(
    to_email: str,
    workshop_name: str,
    role_name: str,
    inviter_name: Optional[str] = None,
    workshop_url: Optional[str] = None,
):
    url = workshop_url or f"{FRONTEND_URL}/select-workshop"
    subject = "Fuiste asignado a un taller"
    quien = f" por {inviter_name}" if inviter_name else ""
    intro = f"Te asignamos al taller {workshop_name}{quien} con el rol {role_name}. Ya podés ingresar y empezar a colaborar."
    html = _wrap_html(
        title="Nuevo acceso a taller",
        intro=intro,
        cta_text="Abrir taller",
        cta_url=url,
    )
    return await _send_email(to_email, subject, html)

async def send_password_reset_email(
    to_email: str,
    first_name: Optional[str],
    reset_url: str,
):
    subject = "Restablecé tu contraseña"
    saludo = f"Hola {first_name}," if first_name else "Hola,"
    intro = (
        f"{saludo} recibimos una solicitud para restablecer tu contraseña. "
        "Hacé clic en el botón para continuar, el enlace vence en 60 minutos."
    )

    extra_html = f"""
      <div style="text-align:left;margin-top:18px">
        <p style="margin:0 0 8px;color:#555;font-size:14px">
          Si no fuiste vos, ignorá este mensaje. Tu cuenta seguirá segura.
        </p>
        <div style="margin:14px 0; padding:12px; background:#fff; border:1px solid #eee; border-radius:8px;">
          <p style="margin:0 0 6px; font-weight:600; font-size:14px;">Consejos de seguridad</p>
          <ul style="margin:0; padding-left:18px; color:#666; font-size:13px; line-height:1.5">
            <li>Usá una contraseña única y difícil de adivinar</li>
            <li>No compartas tu contraseña con nadie</li>
            <li>Actualizá tu contraseña si sospechás actividad inusual</li>
          </ul>
        </div>
      </div>
    """

    html = _wrap_html(
        title="Restablecé tu contraseña",
        intro=intro,
        cta_text="Crear nueva contraseña",
        cta_url=reset_url,
        extra_html=extra_html,
    )

    return await _send_email(to_email, subject, html)
