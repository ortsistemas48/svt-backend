# app/email.py
import os, secrets, httpx, logging
from typing import Optional

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
RESEND_FROM = os.getenv("RESEND_FROM", "no-reply@example.com")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")
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
    payload = {
        "from": RESEND_FROM,
        "to": [to_email],
        "subject": subject,
        "html": html,
    }
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            json=payload,
        )
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            # logueamos detalle para debug
            log.exception("Error enviando email a %s: %s | body=%s", to_email, e, r.text)
            raise
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
