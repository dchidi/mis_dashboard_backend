from fastapi import APIRouter, Depends, status
from sqlalchemy.engine import Engine

from app.schemas.auth import (
    AuthResponse,
    LoginRequest,
    PasswordResetRequest,
    PasswordResetResponse,
    PasswordUpdateRequest,
    RegistrationRequest,
    UserProfile,
)
from app.db.sqlserver import get_mis_db_engine
from app.services.auth import (
    AuthService,
    SQLUserRepository,
    UserAccount,
)

router = APIRouter(prefix="/auth", tags=["Auth"])


def get_auth_service_dep(
    mis_db: Engine = Depends(get_mis_db_engine),
) -> AuthService:
    repository = SQLUserRepository(mis_db)
    return AuthService(repository=repository)


def _to_profile(user: UserAccount) -> UserProfile:
    return UserProfile(email=user.email, full_name=user.full_name, role=user.role)


@router.post(
    "/register",
    response_model=AuthResponse,
    status_code=status.HTTP_201_CREATED,
)
def register_user(
    payload: RegistrationRequest,
    auth_service: AuthService = Depends(get_auth_service_dep),
) -> AuthResponse:
    user = auth_service.register_user(
        email=payload.email,
        full_name=payload.full_name,
        password=payload.password,
        role=payload.role,
    )
    access_token = auth_service.issue_token(user)
    return AuthResponse(access_token=access_token, user=_to_profile(user))


@router.post("/login", response_model=AuthResponse)
def login_user(
    payload: LoginRequest,
    auth_service: AuthService = Depends(get_auth_service_dep),
) -> AuthResponse:
    user = auth_service.authenticate(payload.email, payload.password)
    access_token = auth_service.issue_token(user)
    return AuthResponse(access_token=access_token, user=_to_profile(user))


@router.post(
    "/update-password",
    response_model=PasswordResetResponse,
)
def update_password(
    payload: PasswordUpdateRequest,
    auth_service: AuthService = Depends(get_auth_service_dep),
) -> PasswordResetResponse:
    auth_service.update_password(token=payload.token, new_password=payload.new_password)
    return PasswordResetResponse(detail="Password updated successfully.")


@router.post(
    "/retrieve-password",
    response_model=PasswordResetResponse,
)
async def retrieve_password(
    payload: PasswordResetRequest,
    auth_service: AuthService = Depends(get_auth_service_dep),
) -> PasswordResetResponse:
    await auth_service.request_password_reset(payload.email)
   
    message = (
        "If an account exists for this email, password reset instructions "
        "have been sent."
    )
    return PasswordResetResponse(detail=message)
