"""chive_shared minimal settings — DATABASE_URL only.

Both chive (pipeline + daemon) and chive-backend consume this. Each provides
DATABASE_URL via their own .env / env-vars; chive_shared just reads it.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    database_url: str


def get_storage_settings() -> StorageSettings:
    return StorageSettings()
