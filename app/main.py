from fastapi import FastAPI
from app.core.extensions import add_extensions
from app.api.api_router import api_router


app = FastAPI()


# Add extensions
add_extensions(app)

# Include API router
app.include_router(api_router, prefix="/api")

#  Api documentation paths
# Swagger UI documentation on /docs
# ReDoc documentation on /redoc
