import os
from dataclasses import dataclass, field
from pathlib import Path

def _streamlit_secret(key: str) -> str | None:
    try:
        import streamlit as st
        value = st.secrets.get(key)
        return str(value) if value is not None else None
    except Exception:
        return None


def _setting(key: str, default: str | None = None) -> str | None:
    env_val = os.getenv(key)
    if env_val is not None:
        return env_val
    secret_val = _streamlit_secret(key)
    if secret_val is not None:
        return secret_val
    return default


def _load_env_file(path: str) -> None:
    """
    Minimal `.env` loader to avoid hard dependency on `pydantic-settings`.
    Only supports simple KEY=VALUE lines.
    """
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        # Keep runtime resilient; missing/invalid env file should not block import.
        return


@dataclass(frozen=True)
class Settings:
    app_env: str = field(default_factory=lambda: str(_setting("APP_ENV", "dev")))
    database_url: str | None = field(default_factory=lambda: _setting("DATABASE_URL"))
    streamlit_server_port: int = field(
        default_factory=lambda: int(str(_setting("STREAMLIT_SERVER_PORT", "8501")))
    )
    http_timeout_seconds: int = field(default_factory=lambda: int(str(_setting("HTTP_TIMEOUT_SECONDS", "20"))))
    http_max_retries: int = field(default_factory=lambda: int(str(_setting("HTTP_MAX_RETRIES", "3"))))

    filmyard_base_url: str = field(
        default_factory=lambda: str(_setting("FILMYARD_BASE_URL", "https://www.filmyard.com"))
    )
    elcinema_base_url: str = field(default_factory=lambda: str(_setting("ELCINEMA_BASE_URL", "https://elcinema.com")))
    bom_base_url: str = field(default_factory=lambda: str(_setting("BOM_BASE_URL", "https://www.boxofficemojo.com")))
    imdb_base_url: str = field(default_factory=lambda: str(_setting("IMDB_BASE_URL", "https://www.imdb.com")))
    prediction_mode: str = field(default_factory=lambda: str(_setting("PREDICTION_MODE", "direct")))
    prediction_api_url: str = field(default_factory=lambda: str(_setting("PREDICTION_API_URL", "http://127.0.0.1:8000")))
    model_path: str = field(default_factory=lambda: str(_setting("MODEL_PATH", "Model/film_prediction_model.pkl")))


def get_settings() -> Settings:
    # Load `.env` from the project root so CWD doesn't break imports.
    root = Path(__file__).resolve().parents[1]
    _load_env_file(str(root / ".env"))
    return Settings()


