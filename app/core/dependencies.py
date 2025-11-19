from fastapi import Header, HTTPException, status

from app.core.security import verify_token


async def require_authentication(authorization: str = Header(None)):
    payload = await optional_authentication(authorization)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication credentials were not provided.",
        )
    return payload


async def optional_authentication(authorization: str = Header(None)):
    if not authorization:
        return None
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication credentials were not provided.",
        )

    token = authorization.split(" ", 1)[1].strip()
    payload = verify_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired authentication token.",
        )
    return payload
