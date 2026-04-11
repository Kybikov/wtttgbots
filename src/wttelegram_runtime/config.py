from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    environment: str = "development"
    host: str = "0.0.0.0"
    port: int = 8080

    database_url: str = ""
    database_ssl: bool = False
    startup_database_max_attempts: int = Field(default=10, ge=1)
    startup_database_retry_delay: float = Field(default=2.0, gt=0)

    public_base_url: str = ""
    webhook_path_prefix: str = "/webhooks/telegram"

    delivery_poll_interval: float = 5.0
    delivery_batch_size: int = 20
    bot_request_timeout: float = 20.0
    max_job_attempts: int = Field(default=5, ge=1)
    reminder_poll_interval: float = 60.0
    reminder_batch_size: int = 40
    reminder_timezone: str = "Europe/Kyiv"
    reminder_schedule_hour: int = Field(default=10, ge=0, le=23)
    reminder_schedule_minute: int = Field(default=0, ge=0, le=59)
    smtp_host: str = ""
    smtp_port: int = Field(default=587, ge=1, le=65535)
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_from_email: str = ""
    smtp_from_name: str = "WTmelon"
    smtp_use_tls: bool = True
    smtp_use_ssl: bool = False

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
