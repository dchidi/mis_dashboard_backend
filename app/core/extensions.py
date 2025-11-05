from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from fastapi.security import OAuth2PasswordBearer

# from jose import JWTError, jwt
# from .config import settings

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="token"
)  # This redirects to /token endpoint


def add_extensions(app: FastAPI):
    # CORS setup
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Adjust this to your needs        
        # allow_origins=["https://your-frontend.example"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Content-Disposition"], 
    )

    # Additional extensions can be added here
