from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    environment: str = "development"
    host: str = "0.0.0.0"
    port: int = 8080

    database_url: str = ""
    database_ssl: bool = False

    public_base_url: str = ""
    webhook_path_prefix: str = "/webhooks/telegram"

    delivery_poll_interval: float = 5.0
    delivery_batch_size: int = 20
    bot_request_timeout: float = 20.0
    max_job_attempts: int = Field(default=5, ge=1)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @field_validator("public_base_url")
    @classmethod
    def normalize_public_base_url(cls, value: str) -> str:
        return str(value or "").rstrip("/")

    @field_validator("webhook_path_prefix")
    @classmethod
    def normalize_webhook_path_prefix(cls, value: str) -> str:
        normalized = "/" + str(value or "").strip("/")
        return normalized.rstrip("/") or "/webhooks/telegram"

    def build_webhook_url(self, bot_id: str) -> str:
        if not self.public_base_url:
            raise ValueError("PUBLIC_BASE_URL is required to build webhook URLs")
        return f"{self.public_base_url}{self.webhook_path_prefix}/{bot_id}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
