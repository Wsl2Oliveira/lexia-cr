"""Centralized configuration loaded from environment / .env file."""
from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[3] / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Databricks
    databricks_host: str = "https://nubank-e2-general.cloud.databricks.com"
    databricks_token: str = ""
    databricks_http_path: str = "/sql/1.0/warehouses/auto"

    # Nubank API auth
    nu_cert_path: str = ""
    nu_cert_key_path: str = ""
    nu_auth_url: str = "https://prod-global-auth.nubank.com.br/api/token"

    # Google
    google_credentials_path: str = "~/.config/lexia/google-credentials.json"
    google_template_doc_id: str = ""
    google_drive_folder_id: str = ""

    # Gemini
    gemini_api_key: str = ""
    gemini_model: str = "gemini-2.0-flash"

    # Processing
    days_back: int = Field(default=12, description="Days back to query cases")
    log_level: str = "INFO"


settings = Settings()
