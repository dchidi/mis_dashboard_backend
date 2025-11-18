from __future__ import annotations

import hashlib
import hmac
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import settings
from app.core.security import create_access_token
from app.services.email_service.reset_password_email import (
    send_reset_password_email,
)

logger = logging.getLogger(__name__)


@dataclass
class UserAccount:
    id: str
    email: str
    full_name: str
    role: str
    password_hash: str
    created_at: datetime


@dataclass
class PasswordResetTicket:
    token: str
    email: str
    expires_at: datetime


class PasswordHasher:
    """Handles password hashing and verification."""

    def __init__(self, salt_size: int = 16):
        self._salt_size = salt_size

    def hash(self, raw_password: str) -> str:
        salt = secrets.token_hex(self._salt_size)
        digest = hashlib.sha256(f"{salt}{raw_password}".encode("utf-8")).hexdigest()
        return f"{salt}${digest}"

    def verify(self, raw_password: str, encoded_password: str) -> bool:
        try:
            salt, stored_hash = encoded_password.split("$", 1)
        except ValueError:
            return False
        digest = hashlib.sha256(f"{salt}{raw_password}".encode("utf-8")).hexdigest()
        return hmac.compare_digest(stored_hash, digest)


class TokenProvider:
    """Issues JWT tokens for authenticated users."""

    def __init__(self, expires_delta: Optional[timedelta] = None):
        self._expires_delta = expires_delta

    def issue(self, user: UserAccount) -> str:
        payload = {
            "sub": user.id,
            "email": user.email,
            "name": user.full_name,
            "role": user.role,
        }
        return create_access_token(payload, self._expires_delta)


class UserRepository:
    """Persistence abstraction for auth related entities."""

    def get_by_email(self, email: str) -> Optional[UserAccount]:
        raise NotImplementedError

    def save(self, user: UserAccount) -> UserAccount:
        raise NotImplementedError

    def save_reset_ticket(self, ticket: PasswordResetTicket) -> PasswordResetTicket:
        raise NotImplementedError

    def get_reset_ticket(self, token: str) -> Optional[PasswordResetTicket]:
        raise NotImplementedError

    def delete_reset_ticket(self, token: str) -> None:
        raise NotImplementedError

    def update_password(self, email: str, password_hash: str) -> None:
        raise NotImplementedError


class SQLUserRepository(UserRepository):
    """SQL Server backed repository using the MIS database."""

    USERS_TABLE = "app_users"
    RESET_TABLE = "app_user_password_resets"

    def __init__(self, engine: Engine):
        self._engine = engine
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        users_sql = f"""
        IF OBJECT_ID('dbo.{self.USERS_TABLE}', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.{self.USERS_TABLE} (
                id NVARCHAR(64) NOT NULL PRIMARY KEY,
                email NVARCHAR(255) NOT NULL UNIQUE,
                full_name NVARCHAR(255) NOT NULL,
                role NVARCHAR(50) NOT NULL,
                password_hash NVARCHAR(512) NOT NULL,
                created_at DATETIME2 NOT NULL
            );
        END
        """
        reset_sql = f"""
        IF OBJECT_ID('dbo.{self.RESET_TABLE}', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.{self.RESET_TABLE} (
                token NVARCHAR(128) NOT NULL PRIMARY KEY,
                email NVARCHAR(255) NOT NULL,
                expires_at DATETIME2 NOT NULL
            );
            CREATE INDEX IX_{self.RESET_TABLE}_email ON dbo.{self.RESET_TABLE}(email);
        END
        """
        with self._engine.begin() as conn:
            conn.execute(text(users_sql))
            conn.execute(text(reset_sql))

    @staticmethod
    def _normalize_email(email: str) -> str:
        return email.strip().lower()

    def get_by_email(self, email: str) -> Optional[UserAccount]:
        normalized_email = self._normalize_email(email)
        query = text(
            f"""
            SELECT id, email, full_name, role, password_hash, created_at
            FROM dbo.{self.USERS_TABLE}
            WHERE email = :email
            """
        )
        with self._engine.connect() as conn:
            row = conn.execute(query, {"email": normalized_email}).fetchone()
        if not row:
            return None
        created_at = row.created_at
        if isinstance(created_at, datetime) and created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        return UserAccount(
            id=row.id,
            email=row.email,
            full_name=row.full_name,
            role=row.role,
            password_hash=row.password_hash,
            created_at=created_at,
        )

    def save(self, user: UserAccount) -> UserAccount:
        normalized_email = self._normalize_email(user.email)
        insert_sql = text(
            f"""
            INSERT INTO dbo.{self.USERS_TABLE} (
                id, email, full_name, role, password_hash, created_at
            ) VALUES (
                :id, :email, :full_name, :role, :password_hash, :created_at
            )
            """
        )
        with self._engine.begin() as conn:
            conn.execute(
                insert_sql,
                {
                    "id": user.id,
                    "email": normalized_email,
                    "full_name": user.full_name,
                    "role": user.role,
                    "password_hash": user.password_hash,
                    "created_at": user.created_at,
                },
            )
        return user

    def save_reset_ticket(self, ticket: PasswordResetTicket) -> PasswordResetTicket:
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    f"DELETE FROM dbo.{self.RESET_TABLE} WHERE email = :email"
                ),
                {"email": ticket.email},
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO dbo.{self.RESET_TABLE} (token, email, expires_at)
                    VALUES (:token, :email, :expires_at)
                    """
                ),
                {
                    "token": ticket.token,
                    "email": ticket.email,
                    "expires_at": ticket.expires_at,
                },
            )
        return ticket

    def update_password(self, email: str, password_hash: str) -> None:
        normalized_email = self._normalize_email(email)
        update_sql = text(
            f"""
            UPDATE dbo.{self.USERS_TABLE}
            SET password_hash = :password_hash
            WHERE email = :email
            """
        )
        with self._engine.begin() as conn:
            result = conn.execute(
                update_sql,
                {"password_hash": password_hash, "email": normalized_email},
            )
            if result.rowcount == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found.",
                )

    def get_reset_ticket(self, token: str) -> Optional[PasswordResetTicket]:
        query = text(
            f"""
            SELECT token, email, expires_at
            FROM dbo.{self.RESET_TABLE}
            WHERE token = :token
            """
        )
        with self._engine.connect() as conn:
            row = conn.execute(query, {"token": token}).fetchone()
        if not row:
            return None
        expires_at = row.expires_at
        if isinstance(expires_at, datetime) and expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        return PasswordResetTicket(token=row.token, email=row.email, expires_at=expires_at)

    def delete_reset_ticket(self, token: str) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    f"DELETE FROM dbo.{self.RESET_TABLE} WHERE token = :token"
                ),
                {"token": token},
            )


def default_reset_link_builder(token: str) -> str:
    base_url = settings.password_reset_base_url
    if "{token}" in base_url:
        return base_url.format(token=token)
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}token={token}"


class AuthService:
    """Co-ordinates registration, authentication and password recovery."""

    def __init__(
        self,
        repository: Optional[UserRepository] = None,
        hasher: Optional[PasswordHasher] = None,
        token_provider: Optional[TokenProvider] = None,
        reset_token_ttl: timedelta = timedelta(minutes=30),
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        reset_link_builder: Optional[Callable[[str], str]] = None,
    ):
        self._repository = repository or UserRepository()
        self._hasher = hasher or PasswordHasher()
        self._token_provider = token_provider or TokenProvider()
        self._reset_token_ttl = reset_token_ttl
        self._clock = clock
        self._reset_link_builder = reset_link_builder or default_reset_link_builder

    def register_user(
        self,
        email: str,
        full_name: str,
        password: str,
        role: str = "user",
    ) -> UserAccount:
        normalized_email = email.strip().lower()
        if self._repository.get_by_email(normalized_email):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="An account with this email already exists.",
            )

        password_hash = self._hasher.hash(password)
        user = UserAccount(
            id=secrets.token_hex(16),
            email=normalized_email,
            full_name=full_name.strip(),
            role=role.strip() or "user",
            password_hash=password_hash,
            created_at=self._clock(),
        )
        return self._repository.save(user)

    def authenticate(self, email: str, password: str) -> UserAccount:
        user = self._repository.get_by_email(email)
        if not user or not self._hasher.verify(password, user.password_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password.",
            )
        return user

    def issue_token(self, user: UserAccount) -> str:
        return self._token_provider.issue(user)

    def update_password(self, token: str, new_password: str) -> None:
        ticket = self._repository.get_reset_ticket(token)
        if not ticket:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Invalid or expired reset link.",
            )
        if ticket.expires_at < self._clock():
            self._repository.delete_reset_ticket(token)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Reset link has expired.",
            )
        user = self._repository.get_by_email(ticket.email)
        if not user:
            self._repository.delete_reset_ticket(token)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found.",
            )
        new_hash = self._hasher.hash(new_password)
        self._repository.update_password(user.email, new_hash)
        self._repository.delete_reset_ticket(token)

    async def request_password_reset(self, email: str) -> None:
        user = self._repository.get_by_email(email)
        if not user:
            logger.info("Password reset requested for non-existent email: %s", email)
            return
        token = secrets.token_urlsafe(32)
        expires_at = self._clock() + self._reset_token_ttl
        ticket = PasswordResetTicket(token=token, email=user.email, expires_at=expires_at)
        self._repository.save_reset_ticket(ticket)
        reset_link = self._reset_link_builder(token)
        logger.info("Password reset link generated for %s %s", user.email, token)
        try:
            await send_reset_password_email(
                to_email=user.email,
                reset_link=reset_link,
                from_email=settings.email_from
            )
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Unable to send reset password email.",
            ) from exc
        
    
