# from fastapi import Depends
from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker, Session
from app.core.config import Settings

settings = Settings()


class SQLServerConnection:
    def __init__(self, server_url: str):
        self.engine = create_engine(
            server_url,
            fast_executemany=True,
            future=True,
        )


# Instantiate connection handlers for each SQL Server database
au_uts = SQLServerConnection(settings.sql_server_uts_url_au)
# au_fit = SQLServerConnection(settings.sql_server_fit_url_au)

nz_uts = SQLServerConnection(settings.sql_server_uts_url_nz)
# nz_fit = SQLServerConnection(settings.sql_server_fit_url_nz)

at_uts = SQLServerConnection(settings.sql_server_uts_url_at)
de_uts = SQLServerConnection(settings.sql_server_uts_url_de)
uk_uts = SQLServerConnection(settings.sql_server_uts_url_uk)

mis_db = SQLServerConnection(settings.sql_server_mis_url)

print("Connections created")


def get_au_uts_engine():
    return au_uts.engine


def get_nz_uts_engine():
    return nz_uts.engine


def get_at_uts_engine():
    return at_uts.engine


def get_de_uts_engine():
    return de_uts.engine


def get_uk_uts_engine():
    return uk_uts.engine


def get_mis_db_engine():
    return mis_db.engine
