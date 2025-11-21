from fastapi import APIRouter

from app.api.v1.endpoints import (
    etl_mis, quote, policy, sales, auth
)


router = APIRouter()


router.include_router(
    etl_mis.router,
    prefix="/etl_mis",
    tags=["ETL"]
)

router.include_router(
    quote.router,
    prefix="/quote",
    tags=["Quotes"]
)

router.include_router(
    policy.router,
    prefix="/policy",
    tags=["Policy"]
)

router.include_router(
    sales.router,
    prefix="/sales",
    tags=["Sales"]
)

router.include_router(
    auth.router,
    prefix="/auth",
    tags=["Auth"]
)