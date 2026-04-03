from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str
    # Must be 127.0.0.1, not localhost. AT Protocol OAuth (RFC 8252) requires
    # loopback IP for redirect URIs, and cookies must be on the same site.
    app_url: str = "http://127.0.0.1:8000"
    frontend_url: str = "http://127.0.0.1:8001"
    session_secret_key: str = "change-me-in-production"
    client_secret_jwk: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
