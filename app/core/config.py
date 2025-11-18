from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    environment: str

    app_name: str = "Trade Report Platform"
    secret_key: str
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    password_reset_base_url: str = "http://localhost/reset-password"

    # Email settings
    smtp_server: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    email_from: str
    smtp_use_tls: bool

    # Mailgun email settings
    # MAILGUN_DOMAIN: str
    mailgun_domain:str
    # MAILGUN_API_KEY: str
    mailgun_api_key:str

    # MIS database credentials
    mis_db_host: str
    mis_db_user: str
    mis_db_password: str
    mis_db_name: str

    # AU uts connection credentials
    au_uts_host: str
    au_uts_user: str
    au_uts_password: str
    au_uts_db_name: str

    # # AU fit connection credentials
    # au_fit_host: str
    # au_fit_user: str
    # au_fit_password: str
    # au_fit_db_name: str

    # NZ uts connection credentials
    nz_uts_host: str
    nz_uts_user: str
    nz_uts_password: str
    nz_uts_db_name: str

    # # NZ fit connection credentials
    # nz_fit_host: str
    # nz_fit_user: str
    # nz_fit_password: str
    # nz_fit_db_name: str

    # UK, AT and DE uts connection credentials
    uk_at_de_uts_host: str
    uk_at_de_uts_user: str
    uk_at_de_uts_password: str
    uk_uts_db_name: str
    at_uts_db_name: str
    de_uts_db_name: str

    # Mongo DB
    mongo_url: str

    # Automatically load .env file content into environment variable.
    class Config:
        env_file = ".env"

    @property
    def sql_server_mis_url(self) -> str:
        return f"mssql+pyodbc://{self.mis_db_user}:{self.mis_db_password}@{self.mis_db_host}/{self.mis_db_name}?driver=ODBC+Driver+17+for+SQL+Server"  # noqa

    @property
    def sql_server_uts_url_au(self) -> str:
        # "mssql+pymssql://username:password@localhost/sales_database"
        return f"mssql+pyodbc://{self.au_uts_user}:{self.au_uts_password}@{self.au_uts_host}/{self.au_uts_db_name}?driver=ODBC+Driver+17+for+SQL+Server"  # noqa

    @property
    def sql_server_uts_url_nz(self) -> str:
        return f"mssql+pyodbc://{self.nz_uts_user}:{self.nz_uts_password}@{self.nz_uts_host}/{self.nz_uts_db_name}?driver=ODBC+Driver+17+for+SQL+Server"  # noqa

    @property
    def sql_server_uts_url_at(self) -> str:
        return f"mssql+pyodbc://{self.uk_at_de_uts_user}:{self.uk_at_de_uts_password}@{self.uk_at_de_uts_host}/{self.at_uts_db_name}?driver=ODBC+Driver+17+for+SQL+Server"  # noqa

    @property
    def sql_server_uts_url_de(self) -> str:
        return f"mssql+pyodbc://{self.uk_at_de_uts_user}:{self.uk_at_de_uts_password}@{self.uk_at_de_uts_host}/{self.de_uts_db_name}?driver=ODBC+Driver+17+for+SQL+Server"  # noqa

    @property
    def sql_server_uts_url_uk(self) -> str:
        return f"mssql+pyodbc://{self.uk_at_de_uts_user}:{self.uk_at_de_uts_password}@{self.uk_at_de_uts_host}/{self.uk_uts_db_name}?driver=ODBC+Driver+17+for+SQL+Server"  # noqa


settings = Settings()
