from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class CommonSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: str = "dev-api-key"
    redis_url: str = "redis://localhost:6389/0"

    # Observability. The OTEL_* names match the OpenTelemetry SDK's standard variables, so the
    # same env block that works for any other service works here. An empty endpoint disables
    # export; structured logs still go to stdout.
    otel_exporter_otlp_endpoint: str = ""
    otel_service_name: str = ""
    otel_service_version: str = "0.1.0"
    deployment_environment: str = "development"
    log_level: str = "INFO"
    log_format: Literal["json", "text"] = "json"
