from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    minio_access_key: str
    minio_secret_key: SecretStr

    model_config = SettingsConfigDict(env_file=".env")
