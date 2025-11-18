import httpx
from app.core.config import settings

async def send_via_mailgun_api(
    to_email: str,
    subject: str,
    html_content: str,
    from_email: str | None = None,
):
    if from_email is None:
        from_email = settings.default_from_email

    url = f"https://api.mailgun.net/v3/{settings.mailgun_domain}/messages"

    data = {
        "from": f"{settings.app_name} <{from_email}>",
        "to": [to_email],
        "subject": subject,
        "html": html_content,
    }

    async with httpx.AsyncClient(
        auth=("api", settings.mailgun_api_key),
        timeout=10.0,
    ) as client:
        response = await client.post(url, data=data)

    if response.status_code >= 400:
        error_body = response.text
        print(f"[Mailgun Error] {response.status_code} {error_body}")
        # include details in the exception so you can see it in the traceback
        raise RuntimeError(
            f"Failed to send email via Mailgun API "
            f"(status={response.status_code} body={error_body})"
        )
