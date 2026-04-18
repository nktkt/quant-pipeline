from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="", extra="ignore")

    alpaca_api_key: str = Field(..., alias="ALPACA_API_KEY")
    alpaca_api_secret: str = Field(..., alias="ALPACA_API_SECRET")
    alpaca_paper: bool = Field(True, alias="ALPACA_PAPER")
    data_dir: Path = Field(Path("./data"), alias="QPIPE_DATA_DIR")

    @property
    def bars_dir(self) -> Path:
        return self.data_dir / "bars"

    @property
    def duckdb_path(self) -> Path:
        return self.data_dir / "quant.duckdb"


def load_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
