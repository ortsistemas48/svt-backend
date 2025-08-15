# app/email.py
import os, secrets, datetime, httpx, logging

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
RESEND_FROM = os.getenv("RESEND_FROM", "no-reply@example.com")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:5000")

log = logging.getLogger("email")

def generate_email_token() -> str:
    return secrets.token_urlsafe(32)

async def send_verification_email(to_email: str, token: str):
    verify_url = f"{FRONTEND_URL}/email-verified?token={token}"
    subject = "Verificá tu email"
    html = f"""
      <div style="font-family: 'Segoe UI', sans-serif; max-width: 480px; margin: auto; background-color: #f9f9f9; border-radius: 10px; padding: 24px; border: 1px solid #e0e0e0;">
        <div style="text-align: center;">
          <h2 style="color: #0040B8; margin-bottom: 8px;">Verificá tu email</h2>
          <p style="color: #555; font-size: 15px; margin-bottom: 24px;">
            Hacé clic en el siguiente botón para verificar tu cuenta y comenzar a usar todos nuestros servicios.
          </p>
          <a href="{verify_url}" style="display:inline-block; padding: 12px 20px; border-radius: 6px; background: #0040B8; color: #fff; text-decoration: none; font-weight: 600; font-size: 15px; box-shadow: 0 2px 6px rgba(0,0,0,0.15);">
            Verificar email
          </a>
          <p style="margin-top: 28px; font-size: 14px; color: #777;">
            Si no podés hacer clic, copiá y pegá este enlace en tu navegador:
          </p>
          <p style="word-break: break-all; font-size: 14px;">
            <a href="{verify_url}" style="color: #0040B8; text-decoration: underline;">{verify_url}</a>
          </p>
        </div>
      </div>
    """
    payload = {
      "from": RESEND_FROM,
      "to": [to_email],
      "subject": subject,
      "html": html,
    }
    
    print(payload, html, verify_url, subject )
    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            json=payload,
        )
        r.raise_for_status()
