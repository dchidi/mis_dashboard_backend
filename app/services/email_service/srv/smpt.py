from email.message import EmailMessage
from aiosmtplib import SMTP
from app.core.config import settings

async def send_via_smtp(from_email: str, to_email: str, subject: str, html_content: str):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email
    msg.add_alternative(html_content, subtype="html")

    try:
        smtp = SMTP(
            hostname=settings.smtp_server,
            port=settings.smtp_port,
            start_tls=settings.smtp_use_tls,  # for port 587 (STARTTLS)
        )
        await smtp.connect()
        await smtp.login(settings.smtp_user, settings.smtp_password)
        await smtp.send_message(msg)
        await smtp.quit()
    except Exception as e:
        print(f"[Email Error] Failed to send email to {to_email}: {e}")
        raise RuntimeError("Failed to send email") from e
