from app.core.config import settings
from app.services.email_service.template_path import template_env
from app.services.email_service.srv.mailgun import send_via_mailgun_api


async def send_reset_password_email(
    to_email: str,
    reset_link: str,
    from_email: str | None = None,
) -> None:
    subject = f"{settings.app_name} Reset Your Password"
    template = template_env.get_template("reset_password_email.html")
    html_content = template.render(
        verification_url=reset_link,
        app_name=settings.app_name,
        expiry_time=settings.access_token_expire_minutes,
    )
    
    # MAILGUN Account is disabled. We are trying to fix it
    # await send_via_mailgun_api(
    #     from_email=from_email or settings.email_from,
    #     to_email=to_email,
    #     subject=subject,
    #     html_content=html_content,
    # )
