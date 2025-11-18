from app.core.config import settings
from app.services.email_service.template_path import template_env
from app.services.email_service.srv.mailgun import send_via_mailgun_api

async def send_welcome_email(
    from_email: str | None,
    to_email: str,
    verification_url: str,
):
    subject = f"{settings.app_name} Account Verification"

    template = template_env.get_template("welcome_email.html")
    html_content = template.render(
        verification_url=verification_url,
        app_name=settings.app_name,
        expiry_time=settings.access_token_expire_minutes,
    )

    await send_via_mailgun_api(
        from_email=from_email or settings.default_from_email,
        to_email=to_email,
        subject=subject,
        html_content=html_content,
    )
