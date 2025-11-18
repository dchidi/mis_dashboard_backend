from fastapi import APIRouter

from app.api.v2.endpoints import router as auth_router

router = APIRouter()

router.include_router(
    auth_router,
    prefix="/auth",
    tags=["Auth"]
)

