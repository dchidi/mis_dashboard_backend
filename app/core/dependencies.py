from fastapi import Header, HTTPException, Query, status

from app.core.security import verify_token


async def require_authentication(
    authorization: str | None = Header(None),
    auth_token: str | None = Query(None),
):
    payload = await optional_authentication(authorization, auth_token)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication credentials were not provided.",
        )
    return payload


def _extract_token(
    authorization: str | None,
    auth_token: str | None,
) -> str | None:
    if authorization:
        if not authorization.lower().startswith("bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication credentials were not provided.",
            )
        return authorization.split(" ", 1)[1].strip()
    if auth_token:
        return auth_token.strip()
    return None


async def optional_authentication(
    authorization: str | None = Header(None),
    auth_token: str | None = Query(None),
):
    token = _extract_token(authorization, auth_token)
    if not token:
        return None
    payload = verify_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired authentication token.",
        )
    return payload
