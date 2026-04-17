"""Centralized configuration loaded from environment / .env file."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[2] / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Databricks
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_http_path: str = "/sql/1.0/warehouses/auto"

    # Nubank API auth
    nu_cert_path: str = ""
    nu_cert_key_path: str = ""
    nu_auth_url: str = "https://prod-global-auth.nubank.com.br/api/token"

    # Google (Apps Script Web App)
    apps_script_url: str = ""
    google_template_doc_id: str = ""
    google_drive_folder_id: str = ""
    google_service_account_path: str = ""

    # LLM (via LiteLLM proxy)
    litellm_api_key: str = ""
    litellm_base_url: str = ""
    litellm_model: str = "gemini/gemini-2.0-flash"

    # Slack Bot
    slack_bot_token: str = ""
    slack_channel_id: str = ""
    slack_notify_enabled: bool = True

    # Processing
    days_back: int = Field(default=3, description="Days back to query cases")
    log_level: str = "INFO"

    # Pipeline
    spreadsheet_id: str = Field(default="", alias="LEXIA_SPREADSHEET_ID")
    target_processes: str = Field(
        default="",
        alias="LEXIA_TARGET_PROCESSES",
        description="Comma-separated process numbers",
    )


settings = Settings()
