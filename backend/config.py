import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import model_validator

class Settings(BaseSettings):
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_PORT: int
    BACKEND_PORT: int

    POSTGRES_CONSUMER_USER: str
    POSTGRES_CONSUMER_PASSWORD: str

    CLICKHOUSE_HTTP_PORT:int
    CLICKHOUSE_DB: str

    LOGGER_USER: str
    LOGGER_PASSWORD: str

    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(__file__), "..", ".env"),
        extra='ignore'
    )

    DATABASE_POSTGRES_URL: str = ""
    DATABASE_CONSUMER_URL: str = ""

    @model_validator(mode='after')
    def set_db_urls(self):
        self.DATABASE_POSTGRES_URL = f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@postgres:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        self.DATABASE_CONSUMER_URL = f"postgresql+asyncpg://{self.POSTGRES_CONSUMER_USER}:{self.POSTGRES_CONSUMER_PASSWORD}@postgres:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        return self

settings = Settings()

def get_pg_db():
    return settings.DATABASE_POSTGRES_URL

def get_consumer_db():
    return settings.DATABASE_CONSUMER_URL

def get_backend_port():
    return settings.BACKEND_PORT

def get_logger_user():
    return settings.LOGGER_USER

def get_logger_password():
    return settings.LOGGER_PASSWORD

def get_clickhouse_http_port():
    return settings.CLICKHOUSE_HTTP_PORT

def get_clickhouse_db():
    return settings.CLICKHOUSE_DB